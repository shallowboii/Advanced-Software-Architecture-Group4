package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Service;

/**
 * Boundary between equipment and production. Controllers receive only actuator settings and actual
 * command identities. Batch, recipe, phase and transfer associations exist solely in PostgreSQL.
 */
@Service
public class ControllerProtocol {
  private static final Set<String> MATERIAL_COMMANDS =
      Set.of(
          "START",
          "PROCESS",
          "PREPARE_TRANSFER",
          "EXECUTE_TRANSFER",
          "COMMIT_TRANSFER",
          "ABORT_TRANSFER",
          "FLUSH",
          "RELEASE");
  private final Store store;
  private final Json json;

  /** Uses the durable command ledger to correlate delayed observations with their original work. */
  public ControllerProtocol(Store store, Json json) {
    this.store = store;
    this.json = json;
  }

  /** Identifies commands that must be fenced against a changed equipment state. */
  public static boolean changesMaterial(String command) {
    return MATERIAL_COMMANDS.contains(command);
  }

  /**
   * Builds a whitelist-only schema-2 message. The private command context may contain production
   * IDs, but none can cross this method into a controller command.
   */
  public ObjectNode actuatorCommand(JsonNode context, long epoch) {
    ObjectNode wire = json.object();
    wire.put("schema_version", 2);
    wire.put("epoch", epoch);
    for (String field : ListFields.ENVELOPE)
      if (context.has(field)) wire.set(field, context.get(field));
    String command = Json.required(context, "cmd");
    if (changesMaterial(command) && context.has("expected_state_version"))
      wire.set("expected_state_version", context.get("expected_state_version"));
    switch (command) {
      case "START", "PROCESS" -> {
        JsonNode phase = context.path("phases").get(context.path("phase_index").asInt(0));
        if (phase == null)
          throw new IllegalArgumentException("command lacks a centrally selected operation");
        wire.put("duration_h", Json.number(phase, "duration_h"));
        wire.put("target_temp_c", Json.number(phase, "target_temp_c"));
      }
      case "PREPARE_TRANSFER" -> wire.put("role", Json.required(context, "role"));
      case "EXECUTE_TRANSFER" -> wire.put("duration_h", Json.number(context, "duration_h"));
      case "FLUSH" -> wire.put("duration_h", context.path("flush_h").asDouble(.1));
      case "STOP", "RESUME" -> wire.put("scope", Json.required(context, "scope"));
      case "PERMIT" -> {
        wire.put("permit_ms", 3000);
        wire.set("time_scale", context.path("time_scale"));
      }
      default -> {}
    }
    return wire;
  }

  /** Adds production associations to an event only after resolving its actual actuator command. */
  public ObjectNode enrichEvent(JsonNode raw) {
    ObjectNode event = (ObjectNode) raw.deepCopy();
    attachContext(event, observationContext(raw));
    if (raw.path("event").asText().equals("PROCESS_STARTED")) event.put("event", "STARTED");
    if (raw.path("contents_discarded").asBoolean()) event.put("discard_reason", "operator");
    if (raw.path("event").asText().equals("RELEASED")) event.put("event", "COMPLETED");
    return event;
  }

  /**
   * Correlates late sensor readings through the immutable command record, never current occupancy.
   */
  public ObjectNode enrichTelemetry(JsonNode raw) {
    ObjectNode sample = (ObjectNode) raw.deepCopy();
    attachContext(sample, observationContext(raw));
    return sample;
  }

  /**
   * Resolves network replies directly and physical interventions through the interrupted actuator
   * command. Unknown command references stay unassociated rather than borrowing a successor batch.
   */
  private ObjectNode observationContext(JsonNode raw) {
    String machineId = Json.required(raw, "machine_id");
    for (String field : ListFields.CORRELATION) {
      String commandId = Json.str(raw, field);
      if (commandId == null) continue;
      Map<String, Object> command =
          store.one(
              "SELECT payload FROM command WHERE id=? AND machine_id=?", commandId, machineId);
      if (command == null) continue;
      ObjectNode context = (ObjectNode) jsonValue(command, "payload").deepCopy();
      String boot = Json.str(raw, "controller_boot_id");
      if (boot == null) boot = Json.str(raw.path("snapshot"), "controller_boot_id");
      if (!java.util.Objects.equals(boot, Json.str(context, "controller_boot_id"))) continue;
      String transferId = Json.str(context, "transfer_id");
      if (transferId != null) {
        Map<String, Object> transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
        if (transfer != null) {
          context.put("batch_id", stringValue(transfer, "batch_id"));
          context.put(
              "execution_id",
              machineId.equals(stringValue(transfer, "source_id"))
                  ? stringValue(transfer, "source_execution_id")
                  : stringValue(transfer, "next_execution_id"));
        }
      }
      String batchId = Json.str(context, "batch_id");
      if (batchId != null) {
        Map<String, Object> batch = store.one("SELECT * FROM batch WHERE id=?", batchId);
        if (batch != null) {
          context.put("recipe_version_id", stringValue(batch, "recipe_version_id"));
          context.put("run_id", stringValue(batch, "run_id"));
        }
        JsonNode phase = context.path("phases").get(context.path("phase_index").asInt(0));
        if (phase != null) context.put("phase", phase.path("name").asText());
        return context;
      }
    }
    return json.object();
  }

  /** Copies only centrally owned associations, retaining the raw technical snapshot unchanged. */
  private void attachContext(ObjectNode observation, ObjectNode context) {
    for (String field : ListFields.BUSINESS) {
      observation.remove(field);
      if (context.has(field)) observation.set(field, context.get(field));
    }
  }

  /** Fixed allowlists make the physical/business boundary reviewable in one place. */
  private static class ListFields {
    private static final String[] ENVELOPE = {
      "message_id", "command_id", "machine_id", "controller_boot_id", "cmd", "ts_utc", "sim_time_h"
    };
    private static final String[] CORRELATION = {
      "command_id", "interrupted_command_id", "current_command_id"
    };
    private static final String[] BUSINESS = {
      "batch_id",
      "execution_id",
      "transfer_id",
      "recipe_version_id",
      "run_id",
      "phase",
      "phase_index"
    };
  }
}
