package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.stereotype.Service;

/**
 * Application facade used by HTTP, transport adapters and integration tests. Cohesive collaborators
 * own the algorithms; this class owns mutation idempotency and shared entry points.
 */
@Service
public class FactoryService {
  private final Store store;
  private final Json json;
  private final OrderService orders;
  private final MachineRegistry registry;
  private final LineService line;
  private final BatchLifecycle batches;
  private final MachineEvents machineEvents;
  private final AdmissionScheduler admission;
  private final TransferCoordinator transfers;
  private final ExperimentService experiments;
  private final HistoryService history;

  /**
   * Wires the collaborators for this factory service; no production transition occurs during
   * construction.
   */
  public FactoryService(
      Store store,
      Json json,
      OrderService orders,
      MachineRegistry registry,
      LineService line,
      BatchLifecycle batches,
      MachineEvents machineEvents,
      AdmissionScheduler admission,
      TransferCoordinator transfers,
      ExperimentService experiments,
      HistoryService history) {
    this.store = store;
    this.json = json;
    this.orders = orders;
    this.registry = registry;
    this.line = line;
    this.batches = batches;
    this.machineEvents = machineEvents;
    this.admission = admission;
    this.transfers = transfers;
    this.experiments = experiments;
    this.history = history;
  }

  public static class Conflict extends RuntimeException {
    /** Carries the explicit failure outcome to the caller without changing production state. */
    public Conflict(String message) {
      super(message);
    }
  }

  public static class Missing extends RuntimeException {
    /** Carries the explicit failure outcome to the caller without changing production state. */
    public Missing(String message) {
      super(message);
    }
  }

  /**
   * Checks live PostgreSQL and Redis availability for readiness without requiring this replica to
   * be leader.
   */
  public boolean healthy() {
    try {
      return registry.available()
          && store.count("SELECT count(*) FROM factory_state WHERE id=1") == 1;
    } catch (Exception unavailable) {
      return false;
    }
  }

  /** Runs a central mutation inside the shared factory row lock and one database transaction. */
  public <T> T locked(Supplier<T> action) {
    return store.transaction(
        () -> {
          store.lock();
          return action.get();
        });
  }

  /**
   * Serialises an API mutation and reuses a stored response for a repeated idempotency key;
   * conflicting payloads are rejected.
   */
  public JsonNode mutate(String scope, String key, JsonNode request, Supplier<Object> action) {
    String fingerprint;
    try {
      fingerprint =
          HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(json.write(request).getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    String hash = fingerprint;
    return locked(
        () -> {
          if (key != null) {
            if (key.isBlank() || key.length() > 160)
              throw new IllegalArgumentException("invalid Idempotency-Key");
            Map<String, Object> previous =
                store.one(
                    "SELECT fingerprint,response FROM api_request WHERE scope=? AND request_key=?",
                    scope,
                    key);
            if (previous != null) {
              if (!hash.equals(stringValue(previous, "fingerprint")))
                throw new Conflict("Idempotency-Key already used for another payload");
              return jsonValue(previous, "response");
            }
          }
          JsonNode result = json.tree(action.get());
          if (key != null)
            store.jdbc.update(
                "INSERT INTO api_request(scope,request_key,fingerprint,response)"
                    + " VALUES(?,?,?,?::jsonb)",
                scope,
                key,
                hash,
                json.write(result));
          return result;
        });
  }

  /**
   * Returns the latest immutable definition of each recipe, including its central version
   * identifier.
   */
  public List<JsonNode> recipes() {
    return orders.recipes();
  }

  /**
   * Validates and appends a recipe version without changing snapshots already assigned to batches.
   */
  public Object publishRecipe(JsonNode input, String pathId) {
    return orders.publishRecipe(input, pathId);
  }

  /** Creates a bounded production order, optionally pinned to an existing recipe version. */
  public Object order(JsonNode input, String operator) {
    return orders.order(input, operator);
  }

  /**
   * Returns orders with started, completed and discarded counts derived from central batch records.
   */
  public List<Map<String, Object>> orders() {
    return orders.orders();
  }

  /**
   * Cancels unissued work and schedules release of pending inventory reservations; admitted
   * production remains explicit.
   */
  public Object cancelOrder(String orderId, String operator) {
    return orders.cancelOrder(orderId, operator);
  }

  /** Returns durable batch projections, optionally restricted to one experiment run. */
  public List<Map<String, Object>> batches(String runId) {
    return orders.batches(runId);
  }

  /**
   * Returns live equipment observations enriched with central business assignments; offline
   * equipment remains visible for audit.
   */
  public List<JsonNode> machines() {
    return registry.machines();
  }

  /**
   * Accepts a technical observation through the live registry; boot changes are handled before any
   * scheduling decision.
   */
  public void heartbeat(JsonNode snapshot) {
    registry.heartbeat(snapshot);
  }

  /** Returns line policy, inhibition and the tracked per-machine STOP acknowledgement barrier. */
  public Object line() {
    return line.line();
  }

  /**
   * Persists a global latch and tracks acknowledgements; an unresolved controller restart cannot be
   * resumed.
   */
  public Object stopLine(boolean stop, String operator) {
    return line.stopLine(stop, operator);
  }

  /**
   * Applies an explicit local latch or centrally resolved flush without silently clearing the
   * global latch.
   */
  public Object machineControl(
      String machineId, String operation, JsonNode input, String operator) {
    return line.machineControl(machineId, operation, input, operator);
  }

  /**
   * Marks a centrally identified batch for discard and reconciles any in-flight transfer before
   * timed removal.
   */
  public Object flushBatch(String batchId, String reason, String operator) {
    return batches.flushBatch(batchId, reason, operator);
  }

  /**
   * Deduplicates a technical event, resolves its command context and applies only a valid current
   * production transition.
   */
  public void machineEvent(JsonNode incoming) {
    machineEvents.machineEvent(incoming);
  }

  /**
   * Reserves one free live mash machine under PUSH or CONWIP and creates a stable inventory
   * reservation identity.
   */
  public Map<String, Object> beginAdmission(long epoch) {
    return admission.beginAdmission(epoch);
  }

  /**
   * Lists pre-admission work whose cancelled orders require inventory release before assignment
   * removal.
   */
  public List<Map<String, Object>> cancelledReservations() {
    return admission.cancelledReservations();
  }

  /** Clears a cancelled pre-admission assignment only after inventory acknowledged its release. */
  public void reservationReleased(String batchId, long epoch) {
    admission.reservationReleased(batchId, epoch);
  }

  /**
   * Admits exactly one reserved batch and records its loading command, provided leadership and line
   * state still allow it.
   */
  public void inventoryReserved(String batchId, long epoch) {
    admission.inventoryReserved(batchId, epoch);
  }

  /**
   * Records the first stock or inventory-availability blockage for a pending admission without
   * duplicating reservations.
   */
  public void inventoryBlocked(String batchId, String reason) {
    admission.inventoryBlocked(batchId, reason);
  }

  /**
   * Checks pending central work and reconciles same-process actuator rejections. It never
   * reconstructs lost controller state.
   */
  public void reconcile(long epoch) {
    admission.reconcile(epoch);
  }

  /**
   * Reserves an available downstream machine for the earliest central holding deadline without
   * creating a buffer.
   */
  public void scheduleTransfers(long epoch) {
    transfers.scheduleTransfers(epoch);
  }

  /**
   * Starts a new measurement window only in an empty, consistent factory with no pending earlier
   * orders.
   */
  public Object startExperiment(JsonNode input) {
    return experiments.startExperiment(input);
  }

  /**
   * Stops new admissions for the current run without discarding active batches or deleting
   * evidence.
   */
  public Object stopExperiment() {
    return experiments.stopExperiment();
  }

  /**
   * Ends admissions when the authoritative simulated clock reaches the configured experiment
   * horizon.
   */
  public void checkHorizon(long epoch) {
    experiments.checkHorizon(epoch);
  }

  /**
   * Returns durable output/loss counters plus absolute and elapsed time; failed runs are identified
   * explicitly.
   */
  public Map<String, Object> experimentStatus() {
    return experiments.experimentStatus();
  }

  /**
   * Returns filtered history with explicit ascending export order or descending dashboard order and
   * bounded pagination.
   */
  public List<JsonNode> events(Map<String, String> filters, boolean telemetry) {
    return history.events(filters, telemetry);
  }

  /**
   * Enriches a raw reading through its original actuator command, persists it idempotently and
   * updates the dashboard.
   */
  public void telemetry(JsonNode sample) {
    history.telemetry(sample);
  }

  /**
   * Counts all centrally unresolved batches, including pending admissions and material awaiting
   * removal.
   */
  public int activeWip() {
    return store.activeWip();
  }
}
