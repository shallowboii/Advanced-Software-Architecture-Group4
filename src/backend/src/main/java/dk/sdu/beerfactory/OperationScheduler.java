package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.springframework.stereotype.Service;

/** Sequences recipe operations and checks quality deadlines centrally; controllers know neither. */
@Service
public class OperationScheduler {
  private final Store store;
  private final ProductionJournal journal;
  private final StageCommands stages;
  private final BatchLifecycle batches;
  private final MachineRegistry registry;

  /** Shares the same transaction boundary as event ingestion and transfer coordination. */
  public OperationScheduler(
      Store store,
      ProductionJournal journal,
      StageCommands stages,
      BatchLifecycle batches,
      MachineRegistry registry) {
    this.store = store;
    this.journal = journal;
    this.stages = stages;
    this.batches = batches;
    this.registry = registry;
  }

  /**
   * Advances exactly one central phase after its actuator operation completes. Unitank maturation
   * stays in the same occupied machine. Final filter completion requests a separate outlet release.
   */
  public void completed(Map<String, Object> batch, JsonNode event) {
    if (batch.get("loss_reason") != null) return;
    int stage = intValue(batch, "stage"), phase = intValue(batch, "phase_index");
    JsonNode definition = jsonValue(batch, "recipe").path("stages").get(stage);
    journal.record(
        "PHASE_COMPLETED",
        stringValue(batch, "id"),
        stringValue(batch, "machine_id"),
        stringValue(batch, "run_id"),
        Map.of(
            "phase",
            definition.path("phases").get(phase).path("name").asText(),
            "phase_index",
            phase));
    if (phase + 1 < definition.path("phases").size()) {
      batch.put("phase_index", phase + 1);
      store.jdbc.update(
          "UPDATE batch SET"
              + " phase_index=?,status='PROCESSING',pause_started_h=NULL,pause_accumulated_h=0"
              + " WHERE id=?",
          phase + 1,
          stringValue(batch, "id"));
      journal.command(
          stringValue(batch, "machine_id"), "PROCESS", stages.stagePayload(batch, stage));
    } else if (stage == 5) {
      store.jdbc.update(
          "UPDATE batch SET status='OUTPUT_PENDING' WHERE id=?", stringValue(batch, "id"));
      journal.command(
          stringValue(batch, "machine_id"),
          "RELEASE",
          Map.of(
              "batch_id",
              stringValue(batch, "id"),
              "execution_id",
              stringValue(batch, "execution_id"),
              "phase_index",
              phase));
    } else {
      double deadline =
          event.path("sim_time_h").asDouble(store.simTime()) + definition.path("hold_h").asDouble();
      store.jdbc.update(
          "UPDATE batch SET status='WAITING_TRANSFER',hold_deadline_h=? WHERE" + " id=?",
          deadline,
          stringValue(batch, "id"));
      journal.record(
          "READY_TO_TRANSFER",
          stringValue(batch, "id"),
          stringValue(batch, "machine_id"),
          stringValue(batch, "run_id"),
          Map.of("hold_deadline_h", deadline));
    }
  }

  /**
   * Checks simulated ageing even while stopped. Loss is counted once in PostgreSQL and removal is
   * requested through ordinary actuator commands; no controller receives a quality policy.
   */
  public void checkTimeouts(long epoch) {
    store.locked(
        () -> {
          Map<String, Object> state = store.state();
          if (((Number) state.get("epoch")).longValue() != epoch || flag(state, "restart_blocked"))
            return null;
          double now = ((Number) state.get("sim_time_h")).doubleValue();
          for (Map<String, Object> batch :
              store.rows(
                  "SELECT * FROM batch WHERE status NOT IN"
                      + " ('COMPLETED','DISCARDED','RESERVING','UNRESOLVED') AND loss_reason IS"
                      + " NULL")) {
            boolean expired =
                batch.get("hold_deadline_h") instanceof Number deadline
                    && now >= deadline.doubleValue();
            Map<String, Object> transfer =
                store.one(
                    "SELECT source_hold_deadline_h FROM transfer WHERE batch_id=? AND"
                        + " status='COMMITTED' AND source_done=false",
                    stringValue(batch, "id"));
            if (transfer != null
                && transfer.get("source_hold_deadline_h") instanceof Number deadline
                && now >= deadline.doubleValue()) expired = true;
            Map<String, Object> machine =
                store.one(
                    "SELECT snapshot FROM machine WHERE id=?", stringValue(batch, "machine_id"));
            boolean paused =
                !registry.online(stringValue(batch, "machine_id"))
                    || jsonValue(machine, "snapshot").path("stopped").asBoolean();
            expired |= updatePauseBudget(batch, paused, now);
            if (expired) {
              batches.markLoss(batch, "spoilage");
              batches.abortOrFlush(batch);
            }
          }
          return null;
        });
  }

  /**
   * Records STOP/RESUME observations immediately, so several short pauses between scheduler ticks
   * still consume the same phase budget. Historical lower-version snapshots cannot rewrite it.
   */
  public void observeMachine(String machineId, JsonNode snapshot) {
    if (flag(store.state(), "restart_blocked")) return;
    Map<String, Object> known =
        store.one("SELECT state_version FROM machine WHERE id=?", machineId);
    if (known == null
        || snapshot.path("state_version").asLong()
            < ((Number) known.get("state_version")).longValue()) return;
    Map<String, Object> batch =
        store.one(
            "SELECT * FROM batch WHERE machine_id=? AND status NOT IN"
                + " ('COMPLETED','DISCARDED','RESERVING','UNRESOLVED') AND loss_reason IS NULL",
            machineId);
    if (batch == null) return;
    if (updatePauseBudget(
        batch,
        snapshot.path("stopped").asBoolean(),
        snapshot.path("sim_time_h").asDouble(store.simTime()))) {
      batches.markLoss(batch, "spoilage");
      batches.abortOrFlush(batch);
    }
  }

  /**
   * Accumulates interrupted time across resumes; only advancing a recipe phase resets this budget.
   */
  private boolean updatePauseBudget(Map<String, Object> batch, boolean paused, double now) {
    double accumulated = ((Number) batch.get("pause_accumulated_h")).doubleValue();
    Number started = (Number) batch.get("pause_started_h");
    if (paused && started == null) {
      store.jdbc.update(
          "UPDATE batch SET pause_started_h=? WHERE id=?", now, stringValue(batch, "id"));
    } else if (!paused && started != null) {
      accumulated += Math.max(0, now - started.doubleValue());
      store.jdbc.update(
          "UPDATE batch SET pause_accumulated_h=?,pause_started_h=NULL WHERE id=?",
          accumulated,
          stringValue(batch, "id"));
    }
    double interrupted =
        accumulated + (paused && started != null ? Math.max(0, now - started.doubleValue()) : 0);
    double limit =
        jsonValue(batch, "recipe")
            .path("stages")
            .get(intValue(batch, "stage"))
            .path("max_pause_h")
            .asDouble();
    return interrupted >= limit;
  }
}
