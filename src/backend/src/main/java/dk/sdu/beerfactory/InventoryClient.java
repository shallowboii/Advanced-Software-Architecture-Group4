package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Calls the Python RAM inventory mock with bounded timeouts and a small circuit breaker.
 * Reservation idempotency lasts only for that inventory process lifetime.
 */
@Component
public class InventoryClient {
  private final Json json;
  private final HttpClient client =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
  private final String base = Bootstrap.env("INVENTORY_URL", "http://localhost:8081");
  private final AtomicInteger failures = new AtomicInteger();
  private volatile long openUntil;

  /**
   * Wires the collaborators for this inventory client; no production transition occurs during
   * construction.
   */
  public InventoryClient(Json json) {
    this.json = json;
  }

  /**
   * Performs one bounded HTTP request. Callers retry durable reservation IDs after transient
   * failures; stock conflicts remain explicit responses.
   */
  public JsonNode call(String method, String path, JsonNode body) {
    if (System.nanoTime() < openUntil) throw new Unavailable("inventory circuit is open");
    try {
      HttpRequest.Builder request =
          HttpRequest.newBuilder(URI.create(base + path))
              .timeout(Duration.ofSeconds(2))
              .header("Content-Type", "application/json");
      request.method(
          method,
          body == null
              ? HttpRequest.BodyPublishers.noBody()
              : HttpRequest.BodyPublishers.ofString(json.write(body)));
      HttpResponse<String> response =
          client.send(request.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() >= 500)
        throw new Unavailable("inventory returned " + response.statusCode());
      failures.set(0);
      JsonNode result = json.read(response.body());
      if (response.statusCode() >= 400) throw new Rejected(response.statusCode(), result);
      return result;
    } catch (Rejected e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
      if (failures.incrementAndGet() >= 3)
        openUntil = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      throw new Unavailable("inventory unavailable");
    }
  }

  public static class Unavailable extends RuntimeException {
    /** Carries the explicit failure outcome to the caller without changing production state. */
    public Unavailable(String message) {
      super(message);
    }
  }

  public static class Rejected extends RuntimeException {
    public final int status;
    public final JsonNode body;

    /** Carries the explicit failure outcome to the caller without changing production state. */
    public Rejected(int status, JsonNode body) {
      super(body.path("error").asText("inventory request rejected"));
      this.status = status;
      this.body = body;
    }
  }
}
