package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.DateTimeException;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;

/**
 * Maps the dashboard REST surface onto central services. Mutations use idempotency keys; the
 * operator header is an audit label, not authentication.
 */
@RestController
@RequestMapping("/api")
public class ApiController {
  private final FactoryService factory;
  private final InventoryClient inventory;
  private final Bootstrap bootstrap;
  private final Json json;

  /**
   * Wires the collaborators for this api controller; no production transition occurs during
   * construction.
   */
  public ApiController(
      FactoryService factory, InventoryClient inventory, Bootstrap bootstrap, Json json) {
    this.factory = factory;
    this.inventory = inventory;
    this.bootstrap = bootstrap;
    this.json = json;
  }

  @GetMapping("/health")
  /** Reports readiness only after initialisation and current PostgreSQL/Redis access succeed. */
  public ResponseEntity<?> health() {
    boolean ready = bootstrap.ready && factory.healthy();
    return ResponseEntity.status(ready ? 200 : 503)
        .body(Map.of("status", ready ? "ok" : "unavailable"));
  }

  @GetMapping("/machines")
  /**
   * Returns live equipment observations enriched with central business assignments; offline
   * equipment remains visible for audit.
   */
  public Object machines() {
    return factory.machines();
  }

  @GetMapping("/recipes")
  /**
   * Returns the latest immutable definition of each recipe, including its central version
   * identifier.
   */
  public Object recipes() {
    return factory.recipes();
  }

  @GetMapping("/orders")
  /**
   * Returns orders with started, completed and discarded counts derived from central batch records.
   */
  public Object orders() {
    return factory.orders();
  }

  @GetMapping("/batches")
  /** Returns durable batch projections, optionally restricted to one experiment run. */
  public Object batches(@RequestParam(required = false, name = "run_id") String run) {
    return factory.batches(run);
  }

  @GetMapping("/line")
  /** Returns line policy, inhibition and the tracked per-machine STOP acknowledgement barrier. */
  public Object line() {
    return factory.line();
  }

  @GetMapping("/events")
  /**
   * Returns filtered history with explicit ascending export order or descending dashboard order and
   * bounded pagination.
   */
  public Object events(@RequestParam Map<String, String> filters) {
    return factory.events(filters, false);
  }

  @GetMapping("/telemetry")
  /**
   * Enriches a raw reading through its original actuator command, persists it idempotently and
   * updates the dashboard.
   */
  public Object telemetry(@RequestParam Map<String, String> filters) {
    return factory.events(filters, true);
  }

  @GetMapping("/inventory")
  /** Reads the current counters from the Python inventory mock. */
  public Object stock() {
    return inventory.call("GET", "/stock", null);
  }

  @GetMapping("/experiment/status")
  /** Returns the current central experiment status and failure boundary. */
  public Object experiment() {
    return factory.experimentStatus();
  }

  /**
   * Serialises an API mutation and reuses a stored response for a repeated idempotency key;
   * conflicting payloads are rejected.
   */
  private JsonNode mutate(String scope, String key, JsonNode body, Supplier<Object> action) {
    return factory.mutate(scope, key, body == null ? json.object() : body, action);
  }

  /** Normalises the bounded operator audit label without treating it as authentication. */
  private String operator(String value) {
    if (value == null || value.isBlank()) return "dashboard";
    if (value.length() > 160) throw new IllegalArgumentException("X-Operator too long");
    return value;
  }

  @PostMapping("/orders")
  /** Creates a bounded production order, optionally pinned to an existing recipe version. */
  public Object order(
      @RequestBody JsonNode body,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestHeader(value = "X-Operator", required = false) String by) {
    return mutate("orders", key, body, () -> factory.order(body, operator(by)));
  }

  @PostMapping("/recipes")
  /** Creates a new validated recipe through the idempotent mutation boundary. */
  public Object recipe(
      @RequestBody JsonNode body,
      @RequestHeader(value = "Idempotency-Key", required = false) String key) {
    return mutate("recipes", key, body, () -> factory.publishRecipe(body, null));
  }

  @PostMapping("/recipes/{id}/versions")
  /** Appends an immutable version to the recipe named in the URL. */
  public Object version(
      @PathVariable String id,
      @RequestBody JsonNode body,
      @RequestHeader(value = "Idempotency-Key", required = false) String key) {
    return mutate("recipes/" + id, key, body, () -> factory.publishRecipe(body, id));
  }

  @PostMapping("/orders/{id}/cancel")
  /** Requests cancellation of the selected order through the idempotent mutation boundary. */
  public Object cancel(
      @PathVariable String id,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestHeader(value = "X-Operator", required = false) String by) {
    return mutate(
        "orders/" + id + "/cancel", key, null, () -> factory.cancelOrder(id, operator(by)));
  }

  @PostMapping("/batches/{id}/flush")
  /** Resolves the selected central batch before issuing any equipment removal command. */
  public Object flush(
      @PathVariable String id,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestHeader(value = "X-Operator", required = false) String by) {
    return mutate(
        "batches/" + id + "/flush",
        key,
        null,
        () -> factory.flushBatch(id, "operator", operator(by)));
  }

  @PostMapping("/machines/{id}/{operation:stop|resume|flush}")
  /**
   * Resolves a machine action centrally; a controller never receives the dashboard batch
   * identifier.
   */
  public Object control(
      @PathVariable String id,
      @PathVariable String operation,
      @RequestBody(required = false) JsonNode body,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestHeader(value = "X-Operator", required = false) String by) {
    return mutate(
        "machines/" + id + "/" + operation,
        key,
        body,
        () ->
            factory.machineControl(
                id, operation, body == null ? json.object() : body, operator(by)));
  }

  @PostMapping("/line/{operation:stop|resume}")
  /**
   * Persists the global STOP or explicit resume request before corresponding actuator commands are
   * sent.
   */
  public Object lineControl(
      @PathVariable String operation,
      @RequestHeader(value = "Idempotency-Key", required = false) String key,
      @RequestHeader(value = "X-Operator", required = false) String by) {
    return mutate(
        "line/" + operation,
        key,
        null,
        () -> factory.stopLine(operation.equals("stop"), operator(by)));
  }

  @PostMapping("/experiment/start")
  /** Starts a new isolated experiment using the supplied run, policy, WIP cap and horizon. */
  public Object start(
      @RequestBody JsonNode body,
      @RequestHeader(value = "Idempotency-Key", required = false) String key) {
    return mutate("experiment/start", key, body, () -> factory.startExperiment(body));
  }

  @PostMapping("/experiment/stop")
  /** Ends new experiment admissions while retaining active production and measurement evidence. */
  public Object stop(@RequestHeader(value = "Idempotency-Key", required = false) String key) {
    return mutate("experiment/stop", key, null, factory::stopExperiment);
  }

  @PostMapping("/inventory/restock")
  /**
   * Passes a stable request identity to the inventory mock; its deduplication lasts only until that
   * mock restarts.
   */
  public Object restock(
      @RequestBody JsonNode body,
      @RequestHeader(value = "Idempotency-Key", required = false) String key) {
    if (!body.isObject()) throw new IllegalArgumentException("object required");
    ObjectNode request = (ObjectNode) body.deepCopy();
    if (!request.hasNonNull("request_id"))
      request.put("request_id", key == null ? Store.id() : key);
    // The Python service persists the idempotency key with the stock update.
    return inventory.call("POST", "/restock", request);
  }

  @RestControllerAdvice
  public static class Errors {
    @ExceptionHandler(FactoryService.Conflict.class)
    /** Maps a central state conflict to an explicit HTTP 409 response. */
    public ResponseEntity<?> conflict(Exception e) {
      return error(409, "conflict", e.getMessage());
    }

    @ExceptionHandler(FactoryService.Missing.class)
    /** Maps an unknown record to HTTP 404 without inventing a default resource. */
    public ResponseEntity<?> missing(Exception e) {
      return error(404, "not_found", e.getMessage());
    }

    @ExceptionHandler({
      IllegalArgumentException.class,
      DateTimeException.class,
      HttpMessageNotReadableException.class
    })
    /** Maps malformed or invalid input to HTTP 400. */
    public ResponseEntity<?> invalid(Exception e) {
      return error(
          400,
          "invalid_request",
          e instanceof HttpMessageNotReadableException ? "JSON body required" : e.getMessage());
    }

    @ExceptionHandler(InventoryClient.Unavailable.class)
    /** Maps inventory connectivity failure to a retryable HTTP 503 response. */
    public ResponseEntity<?> unavailable(Exception e) {
      return error(503, "inventory_unavailable", e.getMessage());
    }

    @ExceptionHandler(InventoryClient.Rejected.class)
    /** Preserves the inventory mock response status and structured rejection details. */
    public ResponseEntity<?> rejected(InventoryClient.Rejected e) {
      return ResponseEntity.status(e.status).body(e.body);
    }

    /** Constructs the consistent REST error object used by validation and conflict handlers. */
    private ResponseEntity<?> error(int status, String code, String message) {
      return ResponseEntity.status(status)
          .body(Map.of("error", code, "message", message == null ? code : message));
    }
  }
}
