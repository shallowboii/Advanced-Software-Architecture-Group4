package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Initialises durable factory settings and validated immutable recipe definitions before
 * scheduling.
 */
@Component
public class Bootstrap {
  private final Store store;
  private final Json json;
  public volatile boolean ready;

  /**
   * Wires the collaborators for this bootstrap; no production transition occurs during
   * construction.
   */
  public Bootstrap(Store store, Json json) {
    this.store = store;
    this.json = json;
  }

  /** Reads an environment setting, using the documented default when it is absent. */
  public static String env(String key, String fallback) {
    return System.getenv().getOrDefault(key, fallback);
  }

  @EventListener(ApplicationReadyEvent.class)
  /**
   * Validates startup settings and inserts initial recipes atomically; scheduling starts only after
   * success.
   */
  public void initialise() throws Exception {
    double scale = Double.parseDouble(env("TIME_SCALE", "1"));
    Domain.positiveFinite(scale, "TIME_SCALE");
    String policy = env("POLICY", "CONWIP");
    int cap = Integer.parseInt(env("WIP_LIMIT", "20"));
    Domain.canAdmit(policy, 0, cap, false, true, true);
    JsonNode root =
        json.read(Files.readString(Path.of(env("RECIPE_FILE", "/app/config/recipes.json"))));
    if (!root.path("recipes").isArray() || root.path("recipes").isEmpty())
      throw new IllegalArgumentException("recipes file must contain recipes");
    store.transaction(
        () -> {
          store.jdbc.update(
              "INSERT INTO factory_state(id,time_scale,policy,wip_limit,run_id) VALUES(1,?,?,?,?)"
                  + " ON CONFLICT(id) DO NOTHING",
              scale,
              policy,
              cap,
              env("RUN_ID", "demo"));
          for (JsonNode recipe : root.path("recipes")) {
            Recipes.validate(recipe);
            store.jdbc.update(
                "INSERT INTO recipe_version(recipe_id,version,definition) VALUES(?,?,?::jsonb) ON"
                    + " CONFLICT DO NOTHING",
                recipe.path("id").asText(),
                recipe.path("version").asInt(),
                json.write(recipe));
          }
          return null;
        });
    ready = true;
  }
}
