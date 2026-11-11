package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.FactoryService.*;
import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Controls measurement windows and exposes central production counters. A controller state loss
 * makes the run failed and requires a clean factory.
 */
@Service
public class ExperimentService {
  private final Store store;
  private final ProductionJournal journal;

  /**
   * Wires the collaborators for this experiment service; no production transition occurs during
   * construction.
   */
  public ExperimentService(Store store, ProductionJournal journal) {
    this.store = store;
    this.journal = journal;
  }

  /**
   * Starts a new measurement window only in an empty, consistent factory with no pending earlier
   * orders.
   */
  public Object startExperiment(JsonNode input) {
    if (flag(store.state(), "restart_blocked"))
      throw new Conflict("Controller state was lost; a clean factory is required");
    if (store.activeWip() != 0)
      throw new Conflict("factory must be empty before starting an experiment");
    if (store.count(
            "SELECT count(*) FROM production_order WHERE issued<quantity AND status NOT IN"
                + " ('CANCELLED','FINISHED')")
        > 0) throw new Conflict("cancel pending orders before a new experiment");
    String run = Json.required(input, "run_id"), policy = Json.required(input, "policy");
    if (input.has("wip_limit")
        && (!input.path("wip_limit").isIntegralNumber()
            || !input.path("wip_limit").canConvertToInt()))
      throw new IllegalArgumentException("wip_limit must be an integer");
    int cap = input.path("wip_limit").asInt(20);
    Domain.canAdmit(policy, 0, cap, false, true, true);
    double horizon = Json.number(input, "horizon_h");
    Domain.positiveFinite(horizon, "horizon_h");
    if (store.count("SELECT count(*) FROM production_event WHERE run_id=?", run) > 0)
      throw new Conflict("run_id already has events; use an isolated factory or a new run_id");
    store.jdbc.update(
        "UPDATE factory_state SET"
            + " run_id=?,policy=?,wip_limit=?,horizon_h=?,experiment_start_h=EXTRACT(EPOCH"
            + " FROM(clock_timestamp()-sim_origin))/time_scale,experiment_done=false WHERE id=1",
        run,
        policy,
        cap,
        horizon);
    journal.record("EXPERIMENT_STARTED", null, null, run, input);
    return experimentStatus();
  }

  /**
   * Stops new admissions for the current run without discarding active batches or deleting
   * evidence.
   */
  public Object stopExperiment() {
    store.jdbc.update("UPDATE factory_state SET experiment_done=true WHERE id=1");
    journal.record("EXPERIMENT_STOPPED", null, null, null, Map.of());
    return experimentStatus();
  }

  /**
   * Ends admissions when the authoritative simulated clock reaches the configured experiment
   * horizon.
   */
  public void checkHorizon(long epoch) {
    store.locked(
        () -> {
          Map<String, Object> state = store.state();
          if (((Number) state.get("epoch")).longValue() != epoch) return null;
          if (!flag(state, "experiment_done")
              && state.get("horizon_h") != null
              && ((Number) state.get("sim_time_h")).doubleValue()
                      - ((Number) state.get("experiment_start_h")).doubleValue()
                  >= ((Number) state.get("horizon_h")).doubleValue()) stopExperiment();
          return null;
        });
  }

  /**
   * Returns durable output/loss counters plus absolute and elapsed time; failed runs are identified
   * explicitly.
   */
  public Map<String, Object> experimentStatus() {
    Map<String, Object> state = store.state();
    String run = stringValue(state, "run_id");
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("run_id", run);
    result.put("policy", state.get("policy"));
    result.put("wip_limit", state.get("wip_limit"));
    result.put(
        "started",
        store.count("SELECT count(*) FROM batch WHERE run_id=? AND status<>'RESERVING'", run));
    long completed =
        store.count("SELECT count(*) FROM batch WHERE run_id=? AND status='COMPLETED'", run);
    long spoiled =
        store.count("SELECT count(*) FROM batch WHERE run_id=? AND loss_reason='spoilage'", run);
    long manual =
        store.count(
            "SELECT count(*) FROM batch WHERE run_id=? AND loss_reason IS NOT NULL AND"
                + " loss_reason<>'spoilage'",
            run);
    result.put("failed", flag(state, "restart_blocked"));
    result.put("failure_reason", state.get("failure_reason"));
    result.put("requires_clean_factory", flag(state, "restart_blocked"));
    result.put("completed", completed);
    result.put("spoiled", spoiled);
    result.put("operator_discarded", manual);
    result.put("hl_out", completed * 50);
    result.put("hl_wasted", (spoiled + manual) * 50);
    result.put("active_wip", store.activeWip());
    result.put("sim_clock_h", state.get("sim_time_h"));
    result.put(
        "elapsed_h",
        ((Number) state.get("sim_time_h")).doubleValue()
            - ((Number) state.get("experiment_start_h")).doubleValue());
    result.put("start_sim_h", state.get("experiment_start_h"));
    result.put("done", state.get("experiment_done"));
    return result;
  }
}
