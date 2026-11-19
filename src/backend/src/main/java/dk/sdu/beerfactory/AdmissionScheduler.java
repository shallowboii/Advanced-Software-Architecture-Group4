package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.FactoryService.*;
import static dk.sdu.beerfactory.Store.*;

import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Selects live equipment and reserves central work before calling inventory. Retries preserve the
 * same admission; actuator rejections are retried only when still applicable.
 */
@Service
public class AdmissionScheduler {
  private final Store store;
  private final Json json;
  private final ProductionJournal journal;
  private final MachineRegistry registry;
  private final StageCommands stages;

  /**
   * Wires the collaborators for this admission scheduler; no production transition occurs during
   * construction.
   */
  public AdmissionScheduler(
      Store store,
      Json json,
      ProductionJournal journal,
      MachineRegistry registry,
      StageCommands stages) {
    this.store = store;
    this.json = json;
    this.journal = journal;
    this.registry = registry;
    this.stages = stages;
  }

  /**
   * Reserves one free live mash machine under PUSH or CONWIP and creates a stable inventory
   * reservation identity.
   */
  public Map<String, Object> beginAdmission(long epoch) {
    return store.locked(
        () -> {
          Map<String, Object> state = store.state();
          if (((Number) state.get("epoch")).longValue() != epoch
              || flag(state, "stopped")
              || flag(state, "experiment_done")) return null;
          Map<String, Object> pending =
              store.one(
                  "SELECT b.* FROM batch b JOIN production_order o ON o.id=b.order_id WHERE"
                      + " b.status='RESERVING' AND o.status<>'CANCELLED' ORDER BY b.created_at"
                      + " LIMIT 1");
          if (pending != null) return pending;
          if (!Domain.canAdmit(
              stringValue(state, "policy"),
              store.activeWip(),
              intValue(state, "wip_limit"),
              false,
              true,
              true)) return null;
          Map<String, Object> order =
              store.one(
                  "SELECT * FROM production_order WHERE issued<quantity AND status<>'CANCELLED'"
                      + " ORDER BY priority DESC,created_at,id LIMIT 1");
          if (order == null) return null;
          Map<String, Object> machine = registry.freeMachine("mash");
          if (machine == null) return null;
          Map<String, Object> version =
              order.get("pinned_version") == null
                  ? store.one(
                      "SELECT * FROM recipe_version WHERE recipe_id=? ORDER BY version DESC LIMIT"
                          + " 1",
                      stringValue(order, "recipe_id"))
                  : store.one(
                      "SELECT * FROM recipe_version WHERE recipe_id=? AND version=?",
                      stringValue(order, "recipe_id"),
                      order.get("pinned_version"));
          if (version == null) throw new Conflict("order recipe version unavailable");
          String batchId = id(),
              execution = id(),
              versionId = stringValue(version, "recipe_id") + ":" + intValue(version, "version");
          store.jdbc.update(
              "INSERT INTO"
                  + " batch(id,order_id,recipe_id,recipe_version_id,recipe,status,machine_id,execution_id,run_id,admitted_sim_h)"
                  + " VALUES(?,?,?,?,?::jsonb,'RESERVING',?,?,?,?)",
              batchId,
              stringValue(order, "id"),
              stringValue(order, "recipe_id"),
              versionId,
              json.write(jsonValue(version, "definition")),
              stringValue(machine, "id"),
              execution,
              stringValue(state, "run_id"),
              state.get("sim_time_h"));
          store.jdbc.update(
              "UPDATE machine SET assigned_batch=?,assigned_role='owner' WHERE id=?",
              batchId,
              stringValue(machine, "id"));
          store.jdbc.update(
              "UPDATE production_order SET issued=issued+1,status='RUNNING' WHERE id=?",
              stringValue(order, "id"));
          return store.one("SELECT * FROM batch WHERE id=?", batchId);
        });
  }

  /**
   * Lists pre-admission work whose cancelled orders require inventory release before assignment
   * removal.
   */
  public List<Map<String, Object>> cancelledReservations() {
    return store.rows(
        "SELECT b.* FROM batch b JOIN production_order o ON o.id=b.order_id WHERE"
            + " b.status='RESERVING' AND o.status='CANCELLED'");
  }

  /** Clears a cancelled pre-admission assignment only after inventory acknowledged its release. */
  public void reservationReleased(String batchId, long epoch) {
    store.locked(
        () -> {
          if (((Number) store.state().get("epoch")).longValue() != epoch) return null;
          Map<String, Object> batch =
              store.one(
                  "SELECT b.* FROM batch b JOIN production_order o ON o.id=b.order_id WHERE b.id=?"
                      + " AND b.status='RESERVING' AND o.status='CANCELLED'",
                  batchId);
          if (batch == null) return null;
          store.jdbc.update(
              "UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE assigned_batch=?",
              batchId);
          store.jdbc.update("DELETE FROM batch WHERE id=?", batchId);
          store.jdbc.update(
              "UPDATE production_order SET issued=issued-1 WHERE id=?",
              stringValue(batch, "order_id"));
          journal.record(
              "ADMISSION_CANCELLED",
              batchId,
              stringValue(batch, "machine_id"),
              stringValue(batch, "run_id"),
              Map.of("reason", "order_cancelled"));
          return null;
        });
  }

  /**
   * Admits exactly one reserved batch and records its loading command, provided leadership and line
   * state still allow it.
   */
  public void inventoryReserved(String batchId, long epoch) {
    store.locked(
        () -> {
          Map<String, Object> batch = store.one("SELECT * FROM batch WHERE id=?", batchId);
          if (batch == null || !stringValue(batch, "status").equals("RESERVING")) return null;
          if (((Number) store.state().get("epoch")).longValue() != epoch
              || flag(store.state(), "stopped")) return null;
          if (store.count(
                  "SELECT count(*) FROM production_order WHERE id=? AND status='CANCELLED'",
                  stringValue(batch, "order_id"))
              > 0) return null;
          store.jdbc.update(
              "UPDATE batch SET status='ADMITTED',admitted_sim_h=? WHERE id=?",
              store.simTime(),
              batchId);
          journal.command(stringValue(batch, "machine_id"), "START", stages.stagePayload(batch, 0));
          journal.record(
              "ADMITTED",
              batchId,
              stringValue(batch, "machine_id"),
              stringValue(batch, "run_id"),
              Map.of(
                  "volume_hl",
                  50,
                  "order_id",
                  stringValue(batch, "order_id"),
                  "recipe_version_id",
                  stringValue(batch, "recipe_version_id")));
          return null;
        });
  }

  /**
   * Records the first stock or inventory-availability blockage for a pending admission without
   * duplicating reservations.
   */
  public void inventoryBlocked(String batchId, String reason) {
    store.locked(
        () -> {
          if (store.count(
                  "SELECT count(*) FROM production_event WHERE batch_id=? AND"
                      + " event='BLOCKED_INVENTORY'",
                  batchId)
              == 0)
            journal.record("BLOCKED_INVENTORY", batchId, null, null, Map.of("reason", reason));
          return null;
        });
  }

  /**
   * Checks pending central work and reconciles same-process actuator rejections. It never
   * reconstructs lost controller state.
   */
  public void reconcile(long epoch) {
    store.locked(
        () -> {
          if (((Number) store.state().get("epoch")).longValue() != epoch
              || flag(store.state(), "restart_blocked")) return null;
          for (Map<String, Object> row :
              store.rows(
                  "SELECT * FROM command WHERE acknowledged=false AND last_sent<now()-interval '30"
                      + " seconds'")) {
            if (intValue(row, "attempts") % 30 == 0)
              store.jdbc.update(
                  "INSERT INTO alarm(machine_id,reason,details)"
                      + " VALUES(?,'COMMAND_UNCONFIRMED',?::jsonb)",
                  stringValue(row, "machine_id"),
                  json.write(row));
          }
          for (Map<String, Object> batch :
              store.rows(
                  "SELECT b.* FROM batch b JOIN machine m ON b.machine_id=m.id WHERE b.status IN"
                      + " ('ADMITTED','PROCESSING','OUTPUT_PENDING') AND b.loss_reason IS NULL AND"
                      + " m.snapshot->>'state' IN ('EMPTY','READY') AND"
                      + " COALESCE((m.snapshot->>'stopped')::boolean,true)=false")) {
            if (!registry.online(stringValue(batch, "machine_id"))) continue;
            Map<String, Object> rejected =
                store.one(
                    "SELECT * FROM command WHERE payload->>'batch_id'=? AND"
                        + " payload->>'execution_id'=? AND (payload->>'phase_index')::integer=? AND"
                        + " cmd IN ('START','PROCESS','RELEASE') ORDER BY created_at DESC,id DESC"
                        + " LIMIT 1",
                    stringValue(batch, "id"),
                    stringValue(batch, "execution_id"),
                    intValue(batch, "phase_index"));
            if (rejected == null
                || !"machine_stopped".equals(stringValue(rejected, "rejection_reason"))) continue;
            Map<String, Object> retry = new LinkedHashMap<>();
            jsonValue(rejected, "payload")
                .fields()
                .forEachRemaining(field -> retry.put(field.getKey(), field.getValue()));
            journal.command(stringValue(batch, "machine_id"), stringValue(rejected, "cmd"), retry);
          }
          // An occupied actuator without a central assignment is inhibited, never adopted.
          for (Map<String, Object> machine :
              store.rows(
                  "SELECT * FROM machine WHERE assigned_batch IS NULL AND"
                      + " snapshot->>'occupied'='true'")) {
            if (store.count(
                    "SELECT count(*) FROM command WHERE machine_id=? AND cmd='STOP' AND"
                        + " acknowledged=false",
                    stringValue(machine, "id"))
                == 0) journal.command(stringValue(machine, "id"), "STOP", Map.of("scope", "local"));
          }
          return null;
        });
  }
}
