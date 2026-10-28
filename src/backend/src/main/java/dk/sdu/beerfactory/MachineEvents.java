package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Processes equipment observations inside the factory transaction. The command ledger supplies
 * production identity; ownership and phase checks reject late results.
 */
@Service
public class MachineEvents {
  private final Store store;
  private final Json json;
  private final ProductionJournal journal;
  private final MachineRegistry registry;
  private final LineService line;
  private final BatchLifecycle batches;
  private final TransferCoordinator transfers;
  private final ControllerProtocol protocol;
  private final OperationScheduler operations;
  private final StageCommands stages;

  /**
   * Wires the collaborators for this machine events; no production transition occurs during
   * construction.
   */
  public MachineEvents(
      Store store,
      Json json,
      ProductionJournal journal,
      MachineRegistry registry,
      LineService line,
      BatchLifecycle batches,
      TransferCoordinator transfers,
      ControllerProtocol protocol,
      OperationScheduler operations,
      StageCommands stages) {
    this.store = store;
    this.json = json;
    this.journal = journal;
    this.registry = registry;
    this.line = line;
    this.batches = batches;
    this.transfers = transfers;
    this.protocol = protocol;
    this.operations = operations;
    this.stages = stages;
  }

  /**
   * Deduplicates a technical event, resolves its command context and applies only a valid current
   * production transition.
   */
  public void machineEvent(JsonNode raw) {
    JsonNode incoming = protocol.enrichEvent(raw);
    String eventId = Json.required(incoming, "event_id");
    store.locked(
        () -> {
          if (store.jdbc.update(
                  "INSERT INTO event_inbox(event_id) VALUES(?) ON CONFLICT DO NOTHING", eventId)
              == 0) return null;
          JsonNode snapshot = incoming.path("snapshot");
          if (!snapshot.isObject())
            throw new IllegalArgumentException("machine event snapshot required");
          if (!registry.acceptSnapshot(snapshot)) return null;
          String machineId = Json.required(incoming, "machine_id"),
              event = Json.required(incoming, "event");
          operations.observeMachine(machineId, snapshot);
          String commandId = Json.str(incoming, "command_id");
          Map<String, Object> acknowledged =
              commandId == null
                  ? null
                  : store.one(
                      "SELECT * FROM command WHERE id=? AND machine_id=?", commandId, machineId);
          if (commandId != null)
            store.jdbc.update(
                "UPDATE command SET acknowledged=true WHERE id=? AND machine_id=?",
                commandId,
                machineId);
          if (event.equals("STOPPED")
              && incoming.path("scope").asText().equals("global")
              && acknowledged == null
              && (commandId == null || !commandId.startsWith("backend-inhibit-"))
              && !flag(store.state(), "stopped")) line.stopLine(true, "physical:" + machineId);
          if (flag(store.state(), "restart_blocked")
              && !Set.of("STOPPED", "SNAPSHOT", "REJECTED").contains(event)) {
            ObjectNode uncertain = (ObjectNode) incoming.deepCopy();
            uncertain.put("original_event", event);
            uncertain.put("accepted", false);
            journal.recordWithId(
                eventId,
                "UNRECONCILED_MACHINE_EVENT",
                Json.str(incoming, "batch_id"),
                machineId,
                null,
                uncertain);
            return null;
          }
          String batchId = Json.str(incoming, "batch_id");
          Map<String, Object> batch =
              batchId == null ? null : store.one("SELECT * FROM batch WHERE id=?", batchId);
          String run = batch == null ? null : stringValue(batch, "run_id");
          boolean owner =
              batch != null
                  && Domain.validOwner(
                      stringValue(batch, "machine_id"),
                      stringValue(batch, "execution_id"),
                      machineId,
                      Json.str(incoming, "execution_id"));
          boolean transferParticipant = false;
          String incomingTransfer = Json.str(incoming, "transfer_id");
          if (batch != null && incomingTransfer != null) {
            Map<String, Object> pending =
                store.one(
                    "SELECT * FROM transfer WHERE id=? AND batch_id=? AND status NOT IN"
                        + " ('DONE','ABORTED')",
                    incomingTransfer,
                    batchId);
            if (pending != null)
              transferParticipant =
                  Domain.validOwner(
                          stringValue(pending, "source_id"),
                          stringValue(pending, "source_execution_id"),
                          machineId,
                          Json.str(incoming, "execution_id"))
                      || Domain.validOwner(
                          stringValue(pending, "target_id"),
                          stringValue(pending, "next_execution_id"),
                          machineId,
                          Json.str(incoming, "execution_id"));
          }
          boolean lossEvent = Set.of("SPOILED", "FLUSH_REQUESTED").contains(event);
          boolean materialEvent =
              Set.of(
                      "STARTED",
                      "PROCESS_COMPLETED",
                      "READY_TO_TRANSFER",
                      "COMPLETED",
                      "FLUSHED",
                      "SPOILED",
                      "FLUSH_REQUESTED")
                  .contains(event);
          boolean accepted =
              !materialEvent
                  || (batch != null
                      && !Domain.terminal(stringValue(batch, "status"))
                      && (owner || (lossEvent && transferParticipant)));
          if (event.equals("PROCESS_COMPLETED") && accepted)
            accepted = incoming.path("phase_index").asInt(-1) == intValue(batch, "phase_index");
          if (event.equals("COMPLETED") && accepted)
            accepted =
                Domain.canComplete(
                    intValue(batch, "stage"),
                    stringValue(batch, "status"),
                    batch.get("loss_reason") != null);
          ObjectNode canonical = (ObjectNode) incoming.deepCopy();
          canonical.put("accepted", accepted);
          if (!accepted) canonical.put("original_event", event);
          journal.recordWithId(
              eventId,
              accepted ? event : "IGNORED_MACHINE_EVENT",
              batchId,
              machineId,
              run,
              canonical);
          if (event.equals("REJECTED")) {
            if (acknowledged != null)
              store.jdbc.update(
                  "UPDATE command SET rejection_reason=? WHERE id=?",
                  incoming.path("reason").asText(),
                  stringValue(acknowledged, "id"));
            if (flag(store.state(), "restart_blocked")) return null;
            if (acknowledged != null
                && stringValue(acknowledged, "cmd").equals("FLUSH")
                && incoming.path("reason").asText().equals("machine_empty")
                && batch != null
                && owner
                && batch.get("loss_reason") != null) {
              journal.command(
                  machineId,
                  "ABORT_TRANSFER",
                  Map.of(
                      "batch_id",
                      batchId,
                      "execution_id",
                      stringValue(batch, "execution_id"),
                      "cancel_loading",
                      true));
              return null;
            }
            store.jdbc.update(
                "INSERT INTO alarm(machine_id,reason,details) VALUES(?,?,?::jsonb)",
                machineId,
                "COMMAND_REJECTED",
                json.write(incoming));
            if (acknowledged != null
                && Set.of("stale_coordinator_epoch", "state_version_conflict")
                    .contains(incoming.path("reason").asText())
                && !Set.of("PREPARE_TRANSFER", "EXECUTE_TRANSFER")
                    .contains(stringValue(acknowledged, "cmd"))) {
              retryCurrentCommand(acknowledged, machineId);
              return null;
            }
            if (acknowledged != null
                && Set.of("PREPARE_TRANSFER", "EXECUTE_TRANSFER")
                    .contains(stringValue(acknowledged, "cmd"))) {
              String transferId = Json.str(jsonValue(acknowledged, "payload"), "transfer_id");
              Map<String, Object> pending =
                  store.one(
                      "SELECT b.* FROM batch b JOIN transfer t ON t.batch_id=b.id WHERE t.id=? AND"
                          + " t.status IN ('PREPARING','EXECUTING')",
                      transferId);
              if (pending != null) batches.abortOrFlush(pending);
            }
            return null;
          }
          if (event.equals("TRANSFER_ABORTED")
              && acknowledged != null
              && jsonValue(acknowledged, "payload").path("cancel_loading").asBoolean()
              && batch != null
              && owner
              && batch.get("loss_reason") != null
              && snapshot.path("state").asText().equals("EMPTY")) {
            batches.finishBatch(batch, "DISCARDED");
            return null;
          }
          if (event.startsWith("TRANSFER_")) {
            transfers.transferEvent(incoming, batch);
            return null;
          }
          if (batch == null || Domain.terminal(stringValue(batch, "status"))) return null;
          if (!accepted) return null;
          if (lossEvent && transferParticipant) {
            batches.markLoss(batch, event.equals("SPOILED") ? "spoilage" : "operator");
            batches.abortOrFlush(batch);
            return null;
          }
          if (!owner) return null;
          switch (event) {
            case "PROCESS_COMPLETED" -> operations.completed(batch, incoming);
            case "STARTED" -> {
              if (stringValue(batch, "status").equals("ADMITTED"))
                store.jdbc.update("UPDATE batch SET status='PROCESSING' WHERE id=?", batchId);
            }
            case "READY_TO_TRANSFER" -> {
              if (batch.get("loss_reason") == null
                  && !stringValue(batch, "status").equals("RESERVING"))
                store.jdbc.update("UPDATE batch SET status='WAITING_TRANSFER' WHERE id=?", batchId);
            }
            case "SPOILED", "FLUSH_REQUESTED" -> {
              batches.markLoss(batch, event.equals("SPOILED") ? "spoilage" : "operator");
              batches.abortOrFlush(batch);
            }
            case "FLUSHED" -> {
              if (batch.get("loss_reason") == null) batches.markLoss(batch, "operator");
              batches.finishBatch(batch, "DISCARDED");
            }
            case "COMPLETED" -> {
              if (Domain.canComplete(
                  intValue(batch, "stage"),
                  stringValue(batch, "status"),
                  batch.get("loss_reason") != null)) batches.finishBatch(batch, "COMPLETED");
            }
            default -> {}
          }
          return null;
        });
  }

  /**
   * Reissues a rejected actuator command only while its original central assignment still exists.
   */
  private void retryCurrentCommand(Map<String, Object> rejected, String machineId) {
    JsonNode context = jsonValue(rejected, "payload");
    String transferId = Json.str(context, "transfer_id"), batchId = Json.str(context, "batch_id");
    if (transferId != null) {
      Map<String, Object> transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
      if (transfer == null) return;
      String role =
          machineId.equals(stringValue(transfer, "source_id"))
              ? "source"
              : machineId.equals(stringValue(transfer, "target_id")) ? "target" : null;
      if (role == null || flag(transfer, role + "_done")) return;
      String expected =
          stringValue(rejected, "cmd").equals("COMMIT_TRANSFER") ? "COMMITTED" : "ABORTING";
      if (!stringValue(transfer, "status").equals(expected)) return;
    } else if (batchId != null) {
      Map<String, Object> batch = store.one("SELECT * FROM batch WHERE id=?", batchId);
      if (batch == null
          || Domain.terminal(stringValue(batch, "status"))
          || !machineId.equals(stringValue(batch, "machine_id"))) return;
      if (Set.of("START", "PROCESS").contains(stringValue(rejected, "cmd"))
          && (batch.get("loss_reason") != null
              || context.path("phase_index").asInt(-1) != intValue(batch, "phase_index"))) return;
    }
    Map<String, Object> retry = new LinkedHashMap<>();
    context.fields().forEachRemaining(field -> retry.put(field.getKey(), field.getValue()));
    journal.command(machineId, stringValue(rejected, "cmd"), retry);
  }
}
