package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Persists actuator commands and canonical events in the same transaction as production changes.
 * Business context stays private; Kafka publication uses an outbox.
 */
@Service
public class ProductionJournal {
  private final Store store;
  private final Json json;

  /**
   * Wires the collaborators for this production journal; no production transition occurs during
   * construction.
   */
  public ProductionJournal(Store store, Json json) {
    this.store = store;
    this.json = json;
  }

  /**
   * Freezes an actuator command and its business correlation before publication. Boot and
   * state-version guards belong to this exact attempt.
   */
  public String command(String machine, String cmd, Map<String, ?> payload) {
    ObjectNode body = json.object();
    body.setAll((ObjectNode) json.tree(payload));
    captureAssignmentContext(body, machine);
    String commandId = id();
    body.put("schema_version", 2);
    body.put("message_id", commandId);
    body.put("command_id", commandId);
    body.put("machine_id", machine);
    body.put("cmd", cmd);
    Map<String, Object> target = store.one("SELECT snapshot FROM machine WHERE id=?", machine);
    if (target != null)
      body.put(
          "controller_boot_id", Json.required(jsonValue(target, "snapshot"), "controller_boot_id"));
    if (target != null && ControllerProtocol.changesMaterial(cmd))
      body.put(
          "expected_state_version", jsonValue(target, "snapshot").path("state_version").asLong());
    body.put("ts_utc", Instant.now().toString());
    body.put("sim_time_h", store.simTime());
    body.put("epoch", ((Number) store.state().get("epoch")).longValue());
    store.jdbc.update(
        "INSERT INTO command(id,machine_id,cmd,payload) VALUES(?,?,?,?::jsonb)",
        commandId,
        machine,
        cmd,
        json.write(body));
    return commandId;
  }

  /** Appends a new canonical event using a generated durable event ID. */
  public void record(String event, String batch, String machine, String run, Object details) {
    recordWithId(id(), event, batch, machine, run, json.tree(details));
  }

  /**
   * Deduplicates a canonical event, assigns insertion order and writes its Kafka outbox record in
   * the same transaction.
   */
  public void recordWithId(
      String eventId, String event, String batch, String machine, String run, JsonNode details) {
    Map<String, Object> state = store.state();
    String actualRun = run == null ? stringValue(state, "run_id") : run;
    ObjectNode body = details.isObject() ? (ObjectNode) details.deepCopy() : json.object();
    body.put("schema_version", 1);
    body.put("event_id", eventId);
    body.put("event", event);
    body.put("batch_id", batch);
    body.put("machine_id", machine);
    body.put("run_id", actualRun);
    Map<String, Object> target = store.one("SELECT snapshot FROM machine WHERE id=?", machine);
    if (target != null)
      body.put(
          "controller_boot_id", Json.required(jsonValue(target, "snapshot"), "controller_boot_id"));
    body.put("ts_utc", Instant.now().toString());
    if (!body.path("sim_time_h").isNumber())
      body.put("sim_time_h", ((Number) state.get("sim_time_h")).doubleValue());
    Map<String, Object> inserted =
        store.one(
            "INSERT INTO"
                + " production_event(event_id,event,batch_id,machine_id,run_id,sim_time_h,payload)"
                + " VALUES(?,?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING RETURNING recorded_seq",
            eventId,
            event,
            batch,
            machine,
            actualRun,
            body.path("sim_time_h").asDouble(),
            json.write(body));
    if (inserted != null) {
      body.put("recorded_seq", ((Number) inserted.get("recorded_seq")).longValue());
      store.jdbc.update(
          "UPDATE production_event SET payload=?::jsonb WHERE event_id=?",
          json.write(body),
          eventId);
      store.jdbc.update(
          "INSERT INTO outbox(topic,message_key,payload) VALUES('batch-events',?,?::jsonb)",
          batch == null ? actualRun : batch,
          json.write(body));
    }
  }

  /**
   * Freezes business correlation when issuing an actuator command, never when its late reply
   * arrives.
   */
  private void captureAssignmentContext(ObjectNode context, String machine) {
    String transferId = Json.str(context, "transfer_id");
    if (transferId != null) {
      Map<String, Object> transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
      if (transfer != null) {
        context.put("batch_id", stringValue(transfer, "batch_id"));
        context.put(
            "execution_id",
            machine.equals(stringValue(transfer, "source_id"))
                ? stringValue(transfer, "source_execution_id")
                : stringValue(transfer, "next_execution_id"));
      }
    }
    String batchId = Json.str(context, "batch_id");
    if (batchId == null) return;
    Map<String, Object> batch = store.one("SELECT * FROM batch WHERE id=?", batchId);
    if (batch == null) return;
    if (!context.hasNonNull("execution_id"))
      context.put("execution_id", stringValue(batch, "execution_id"));
    if (!context.has("phase_index")) context.put("phase_index", intValue(batch, "phase_index"));
    context.put("recipe_version_id", stringValue(batch, "recipe_version_id"));
    context.put("run_id", stringValue(batch, "run_id"));
    context.put("flush_h", jsonValue(batch, "recipe").path("flush_h").asDouble(.1));
  }
}
