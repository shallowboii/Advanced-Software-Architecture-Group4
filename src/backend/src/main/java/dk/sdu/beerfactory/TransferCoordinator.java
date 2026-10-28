package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Owns two-end transfer decisions in PostgreSQL. Equipment only receives technical reservation and
 * valve operations, never transfer or batch identities.
 */
@Service
public class TransferCoordinator {
  private final Store store;
  private final ProductionJournal journal;
  private final BatchLifecycle batches;
  private final MachineRegistry registry;
  private final StageCommands stages;

  /**
   * Wires the collaborators for this transfer coordinator; no production transition occurs during
   * construction.
   */
  public TransferCoordinator(
      Store store,
      ProductionJournal journal,
      BatchLifecycle batches,
      MachineRegistry registry,
      StageCommands stages) {
    this.store = store;
    this.journal = journal;
    this.batches = batches;
    this.registry = registry;
    this.stages = stages;
  }

  /**
   * Advances one durable transfer from participant evidence; source release must precede receiver
   * acceptance.
   */
  public void transferEvent(JsonNode event, Map<String, Object> batch) {
    String transferId = Json.str(event, "transfer_id");
    if (transferId == null) return;
    Map<String, Object> transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
    if (transfer == null) return;
    if (batch == null)
      batch = store.one("SELECT * FROM batch WHERE id=?", stringValue(transfer, "batch_id"));
    if (batch == null) return;
    String machine = Json.required(event, "machine_id");
    String role =
        machine.equals(stringValue(transfer, "source_id"))
            ? "source"
            : machine.equals(stringValue(transfer, "target_id")) ? "target" : null;
    if (role == null) return;
    String kind = Json.required(event, "event"), state = stringValue(transfer, "status");
    if (kind.equals("TRANSFER_PREPARED") && state.equals("PREPARING")) {
      store.jdbc.update("UPDATE transfer SET " + role + "_prepared=true WHERE id=?", transferId);
      transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
      if (flag(transfer, "source_prepared") && flag(transfer, "target_prepared")) {
        store.jdbc.update("UPDATE transfer SET status='EXECUTING' WHERE id=?", transferId);
        Map<String, Object> payload =
            Map.of(
                "transfer_id",
                transferId,
                "duration_h",
                jsonValue(batch, "recipe").path("transfer_h").asDouble());
        journal.command(stringValue(transfer, "source_id"), "EXECUTE_TRANSFER", payload);
        journal.command(stringValue(transfer, "target_id"), "EXECUTE_TRANSFER", payload);
      }
    } else if (kind.equals("TRANSFER_READY") && state.equals("EXECUTING")) {
      store.jdbc.update("UPDATE transfer SET " + role + "_ready=true WHERE id=?", transferId);
      transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
      if (Domain.canCommitTransfer(
          flag(transfer, "source_ready"),
          flag(transfer, "target_ready"),
          state,
          batch.get("loss_reason") != null)) {
        store.jdbc.update("UPDATE transfer SET status='COMMITTED' WHERE id=?", transferId);
        store.jdbc.update(
            "UPDATE batch SET"
                + " machine_id=?,stage=stage+1,execution_id=?,status='TRANSFERRING',phase_index=0,hold_deadline_h=NULL,pause_started_h=NULL,pause_accumulated_h=0"
                + " WHERE id=?",
            stringValue(transfer, "target_id"),
            stringValue(transfer, "next_execution_id"),
            stringValue(batch, "id"));
        store.jdbc.update(
            "UPDATE machine SET assigned_role='owner' WHERE id=?",
            stringValue(transfer, "target_id"));
        journal.command(
            stringValue(transfer, "source_id"),
            "COMMIT_TRANSFER",
            Map.of("transfer_id", transferId));
      }
    } else if (kind.equals("TRANSFER_COMMITTED") && state.equals("COMMITTED")) {
      if (flag(transfer, role + "_done")) return;
      store.jdbc.update("UPDATE transfer SET " + role + "_done=true WHERE id=?", transferId);
      transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
      String discardReason = Json.str(event, "discard_reason");
      if (discardReason != null && !discardReason.isBlank())
        batches.markLoss(
            batch,
            Set.of("hold_timeout", "interruption_limit", "spoilage").contains(discardReason)
                ? "spoilage"
                : "operator");
      if (role.equals("source")) {
        store.jdbc.update(
            "UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE id=? AND"
                + " assigned_batch=?",
            machine,
            stringValue(batch, "id"));
        Map<String, Object> commit = new LinkedHashMap<>();
        commit.put("transfer_id", transferId);
        if (batch.get("loss_reason") != null)
          commit.put("discard_reason", stringValue(batch, "loss_reason"));
        journal.command(stringValue(transfer, "target_id"), "COMMIT_TRANSFER", commit);
      }
      if (flag(transfer, "source_done") && flag(transfer, "target_done")) {
        store.jdbc.update("UPDATE transfer SET status='DONE' WHERE id=?", transferId);
        store.jdbc.update(
            "UPDATE batch SET status='PROCESSING' WHERE id=? AND status='TRANSFERRING'",
            stringValue(batch, "id"));
        if (batch.get("loss_reason") == null) {
          Map<String, Object> current =
              store.one("SELECT * FROM batch WHERE id=?", stringValue(batch, "id"));
          journal.command(
              stringValue(current, "machine_id"),
              "PROCESS",
              stages.stagePayload(current, intValue(current, "stage")));
        }
        if (batch.get("loss_reason") != null)
          journal.command(
              stringValue(batch, "machine_id"),
              "FLUSH",
              Map.of(
                  "batch_id",
                  stringValue(batch, "id"),
                  "reason",
                  stringValue(batch, "loss_reason")));
      }
    } else if (kind.equals("TRANSFER_ABORTED") && state.equals("ABORTING")) {
      store.jdbc.update("UPDATE transfer SET " + role + "_done=true WHERE id=?", transferId);
      transfer = store.one("SELECT * FROM transfer WHERE id=?", transferId);
      if (flag(transfer, "source_done") && flag(transfer, "target_done")) {
        store.jdbc.update("UPDATE transfer SET status='ABORTED' WHERE id=?", transferId);
        store.jdbc.update(
            "UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE id=? AND"
                + " assigned_batch=?",
            stringValue(transfer, "target_id"),
            stringValue(batch, "id"));
        if (batch.get("loss_reason") != null)
          journal.command(
              stringValue(transfer, "source_id"),
              "FLUSH",
              Map.of(
                  "batch_id",
                  stringValue(batch, "id"),
                  "reason",
                  stringValue(batch, "loss_reason")));
        else
          store.jdbc.update(
              "UPDATE batch SET status='WAITING_TRANSFER' WHERE id=?", stringValue(batch, "id"));
      }
    }
  }

  /**
   * Reserves an available downstream machine for the earliest central holding deadline without
   * creating a buffer.
   */
  public void scheduleTransfers(long epoch) {
    store.locked(
        () -> {
          Map<String, Object> state = store.state();
          if (((Number) state.get("epoch")).longValue() != epoch || flag(state, "stopped"))
            return null;
          List<Map<String, Object>> waiting =
              store.rows(
                  "SELECT b.* FROM batch b JOIN machine m ON m.id=b.machine_id WHERE"
                      + " b.status='WAITING_TRANSFER' AND b.loss_reason IS NULL AND b.stage<5 AND"
                      + " m.last_seen>now()-interval '7 seconds' AND"
                      + " COALESCE((m.snapshot->>'local_stop')::boolean,false)=false ORDER BY"
                      + " COALESCE(b.hold_deadline_h,1e99),b.created_at,b.id");
          for (Map<String, Object> batch : waiting) {
            if (!registry.online(stringValue(batch, "machine_id"))) continue;
            if (store.count(
                    "SELECT count(*) FROM transfer WHERE batch_id=? AND status NOT IN"
                        + " ('DONE','ABORTED')",
                    stringValue(batch, "id"))
                > 0) continue;
            Map<String, Object> target =
                registry.freeMachine(Domain.STAGES.get(intValue(batch, "stage") + 1));
            if (target == null) continue;
            String transferId = id(), execution = id();
            store.jdbc.update(
                "INSERT INTO"
                    + " transfer(id,batch_id,source_id,target_id,source_execution_id,next_execution_id)"
                    + " VALUES(?,?,?,?,?,?)",
                transferId,
                stringValue(batch, "id"),
                stringValue(batch, "machine_id"),
                stringValue(target, "id"),
                stringValue(batch, "execution_id"),
                execution);
            store.jdbc.update(
                "UPDATE transfer SET source_hold_deadline_h=? WHERE id=?",
                batch.get("hold_deadline_h"),
                transferId);
            store.jdbc.update(
                "UPDATE machine SET assigned_batch=?,assigned_role='receiver' WHERE id=?",
                stringValue(batch, "id"),
                stringValue(target, "id"));
            store.jdbc.update(
                "UPDATE batch SET status='TRANSFERRING' WHERE id=?", stringValue(batch, "id"));
            Map<String, Object> source = stages.stagePayload(batch, intValue(batch, "stage"));
            source.put("transfer_id", transferId);
            source.put("role", "source");
            source.put("peer_id", stringValue(target, "id"));
            Map<String, Object> dest = stages.stagePayload(batch, intValue(batch, "stage") + 1);
            dest.put("execution_id", execution);
            dest.put("transfer_id", transferId);
            dest.put("role", "target");
            dest.put("peer_id", stringValue(batch, "machine_id"));
            journal.command(stringValue(batch, "machine_id"), "PREPARE_TRANSFER", source);
            journal.command(stringValue(target, "id"), "PREPARE_TRANSFER", dest);
            journal.record(
                "TRANSFER_REQUESTED",
                stringValue(batch, "id"),
                stringValue(batch, "machine_id"),
                stringValue(batch, "run_id"),
                Map.of("transfer_id", transferId, "target_id", stringValue(target, "id")));
          }
          return null;
        });
  }
}
