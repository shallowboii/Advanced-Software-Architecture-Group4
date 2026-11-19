package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.FactoryService.*;
import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Manages orders and immutable recipe versions. Each admitted batch retains its selected recipe
 * snapshot independently of later edits.
 */
@Service
public class OrderService {
  private final Store store;
  private final Json json;
  private final ProductionJournal journal;

  /**
   * Wires the collaborators for this order service; no production transition occurs during
   * construction.
   */
  public OrderService(Store store, Json json, ProductionJournal journal) {
    this.store = store;
    this.json = json;
    this.journal = journal;
  }

  /**
   * Returns the latest immutable definition of each recipe, including its central version
   * identifier.
   */
  public List<JsonNode> recipes() {
    return store
        .rows(
            "SELECT DISTINCT ON(recipe_id) definition,recipe_id,version FROM recipe_version ORDER"
                + " BY recipe_id,version DESC")
        .stream()
        .map(
            row -> {
              ObjectNode value = (ObjectNode) jsonValue(row, "definition").deepCopy();
              value.put(
                  "recipe_version_id",
                  stringValue(row, "recipe_id") + ":" + intValue(row, "version"));
              return (JsonNode) value;
            })
        .toList();
  }

  /**
   * Validates and appends a recipe version without changing snapshots already assigned to batches.
   */
  public Object publishRecipe(JsonNode input, String pathId) {
    if (!input.isObject()) throw new IllegalArgumentException("recipe must be an object");
    ObjectNode recipe = (ObjectNode) input.deepCopy();
    String recipeId = pathId == null ? Json.required(recipe, "id") : pathId;
    if (!recipeId.matches("[a-zA-Z0-9_-]{1,80}"))
      throw new IllegalArgumentException("invalid recipe id");
    long latest =
        store.count(
            "SELECT COALESCE(max(version),0) FROM recipe_version WHERE recipe_id=?", recipeId);
    if (pathId == null && latest > 0) throw new Conflict("recipe already exists");
    if (pathId != null && latest == 0) throw new Missing("recipe not found");
    recipe.put("id", recipeId);
    recipe.put("version", latest + 1);
    recipe.remove("recipe_version_id");
    Recipes.validate(recipe);
    store.jdbc.update(
        "INSERT INTO recipe_version(recipe_id,version,definition) VALUES(?,?,?::jsonb)",
        recipeId,
        latest + 1,
        json.write(recipe));
    journal.record(
        "RECIPE_PUBLISHED", null, null, null, Map.of("recipe_id", recipeId, "version", latest + 1));
    return recipe;
  }

  /** Creates a bounded production order, optionally pinned to an existing recipe version. */
  public Object order(JsonNode input, String operator) {
    String recipeId = Json.required(input, "recipe_id");
    if (store.count("SELECT count(*) FROM recipe_version WHERE recipe_id=?", recipeId) == 0)
      throw new Missing("recipe not found");
    if (!input.path("quantity").isIntegralNumber() || !input.path("quantity").canConvertToInt())
      throw new IllegalArgumentException("quantity must be an integer");
    int quantity = input.path("quantity").asInt();
    if (quantity < 1 || quantity > 10000)
      throw new IllegalArgumentException("quantity must be between 1 and 10000");
    if (input.has("priority") && !input.path("priority").isIntegralNumber())
      throw new IllegalArgumentException("priority must be integer");
    int priority = input.path("priority").asInt(0);
    Integer version = null;
    if (input.hasNonNull("version")) {
      if (!input.path("version").isIntegralNumber())
        throw new IllegalArgumentException("version must be integer");
      version = input.path("version").asInt();
      if (store.count(
              "SELECT count(*) FROM recipe_version WHERE recipe_id=? AND version=?",
              recipeId,
              version)
          == 0) throw new Missing("recipe version not found");
    }
    String orderId = id();
    store.jdbc.update(
        "INSERT INTO production_order(id,recipe_id,pinned_version,quantity,priority,operator_label)"
            + " VALUES(?,?,?,?,?,?)",
        orderId,
        recipeId,
        version,
        quantity,
        priority,
        operator);
    journal.record(
        "ORDER_CREATED",
        null,
        null,
        null,
        Map.of("order_id", orderId, "quantity", quantity, "operator", operator));
    return orders().stream()
        .filter(o -> orderId.equals(stringValue(o, "id")))
        .findFirst()
        .orElseThrow();
  }

  /**
   * Returns orders with started, completed and discarded counts derived from central batch records.
   */
  public List<Map<String, Object>> orders() {
    return store.rows(
        "SELECT o.*, (SELECT count(*) FROM batch b WHERE b.order_id=o.id AND b.status<>'RESERVING')"
            + " AS started, (SELECT count(*) FROM batch b WHERE b.order_id=o.id AND"
            + " b.status='COMPLETED') AS completed, (SELECT count(*) FROM batch b WHERE"
            + " b.order_id=o.id AND b.status='DISCARDED') AS discarded FROM production_order o"
            + " ORDER BY o.created_at,o.id");
  }

  /**
   * Cancels unissued work and schedules release of pending inventory reservations; admitted
   * production remains explicit.
   */
  public Object cancelOrder(String orderId, String operator) {
    Map<String, Object> order =
        require(store.one("SELECT * FROM production_order WHERE id=?", orderId), "order");
    store.jdbc.update("UPDATE production_order SET status='CANCELLED' WHERE id=?", orderId);
    journal.record(
        "ORDER_CANCELLED", null, null, null, Map.of("order_id", orderId, "operator", operator));
    return Map.of(
        "id",
        orderId,
        "status",
        "CANCELLED",
        "active_batches_continue",
        true,
        "unstarted_cancelled",
        intValue(order, "quantity")
            - intValue(order, "issued")
            + store.count(
                "SELECT count(*) FROM batch WHERE order_id=? AND status='RESERVING'", orderId));
  }

  /** Returns durable batch projections, optionally restricted to one experiment run. */
  public List<Map<String, Object>> batches(String runId) {
    return runId == null
        ? store.rows("SELECT * FROM batch ORDER BY created_at,id")
        : store.rows("SELECT * FROM batch WHERE run_id=? ORDER BY created_at,id", runId);
  }
}
