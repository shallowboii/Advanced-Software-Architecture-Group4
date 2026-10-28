package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.FactoryService.*;
import static dk.sdu.beerfactory.Store.*;

import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Owns terminal outcomes and loss decisions. A loss is recorded once; timed physical removal is a
 * separate operation.
 */
@Service
public class BatchLifecycle {
  private final Store store;
  private final ProductionJournal journal;

  /**
   * Wires the collaborators for this batch lifecycle; no production transition occurs during
   * construction.
   */
  public BatchLifecycle(Store store, ProductionJournal journal) {
    this.store = store;
    this.journal = journal;
  }

  /**
   * Marks a centrally identified batch for discard and reconciles any in-flight transfer before
   * timed removal.
   */
  public Object flushBatch(String batchId, String reason, String operator) {
    if (flag(store.state(), "restart_blocked"))
      throw new Conflict("Controller state was lost; preserve this run and start a clean factory");
    Map<String, Object> batch =
        require(store.one("SELECT * FROM batch WHERE id=?", batchId), "batch");
    if (Domain.terminal(stringValue(batch, "status"))) return batch;
    if (stringValue(batch, "status").equals("RESERVING"))
      throw new Conflict("reservation is pending; cancel the order or wait for admission");
    markLoss(batch, reason);
    journal.record(
        "FLUSH_REQUESTED",
        batchId,
        stringValue(batch, "machine_id"),
        stringValue(batch, "run_id"),
        Map.of("reason", reason, "operator", operator));
    abortOrFlush(batch);
    return store.one("SELECT * FROM batch WHERE id=?", batchId);
  }

  /**
   * Records loss once and retires queued processing/output commands; later flush completion cannot
   * count the same loss again.
   */
  public void markLoss(Map<String, Object> batch, String reason) {
    if (Domain.terminal(stringValue(batch, "status"))) return;
    store.jdbc.update(
        "UPDATE command SET acknowledged=true WHERE payload->>'batch_id'=? AND cmd IN"
            + " ('START','PROCESS','RELEASE')",
        stringValue(batch, "id"));
    if (batch.get("loss_reason") == null)
      journal.record(
          "LOSS_RECORDED",
          stringValue(batch, "id"),
          stringValue(batch, "machine_id"),
          stringValue(batch, "run_id"),
          Map.of("reason", reason, "volume_hl", 50));
    store.jdbc.update(
        "UPDATE batch SET loss_reason=COALESCE(loss_reason,?),status='FLUSH_PENDING' WHERE id=? AND"
            + " status NOT IN ('COMPLETED','DISCARDED')",
        reason,
        stringValue(batch, "id"));
    batch.put("loss_reason", batch.get("loss_reason") == null ? reason : batch.get("loss_reason"));
    batch.put("status", "FLUSH_PENDING");
  }

  /**
   * Aborts an uncommitted transfer before removal, or respects a committed ownership decision until
   * its acknowledgements arrive.
   */
  public void abortOrFlush(Map<String, Object> batch) {
    Map<String, Object> transfer =
        store.one(
            "SELECT * FROM transfer WHERE batch_id=? AND status NOT IN ('DONE','ABORTED')",
            stringValue(batch, "id"));
    if (transfer == null) {
      journal.command(
          stringValue(batch, "machine_id"),
          "FLUSH",
          Map.of(
              "batch_id", stringValue(batch, "id"), "reason", stringValue(batch, "loss_reason")));
      return;
    }
    String state = stringValue(transfer, "status");
    if (state.equals("COMMITTED")) return;
    if (!state.equals("ABORTING")) {
      store.jdbc.update(
          "UPDATE transfer SET status='ABORTING',source_done=false,target_done=false WHERE id=?",
          stringValue(transfer, "id"));
      store.jdbc.update(
          "UPDATE command SET acknowledged=true WHERE payload->>'transfer_id'=? AND cmd IN"
              + " ('PREPARE_TRANSFER','EXECUTE_TRANSFER')",
          stringValue(transfer, "id"));
      journal.command(
          stringValue(transfer, "source_id"),
          "ABORT_TRANSFER",
          Map.of("transfer_id", stringValue(transfer, "id")));
      journal.command(
          stringValue(transfer, "target_id"),
          "ABORT_TRANSFER",
          Map.of("transfer_id", stringValue(transfer, "id")));
    }
  }

  /**
   * Records one terminal outcome, releases the central machine assignment and completes a fully
   * issued order when appropriate.
   */
  public void finishBatch(Map<String, Object> batch, String outcome) {
    store.jdbc.update(
        "UPDATE batch SET status=?,completed_at=now(),completed_sim_h=? WHERE id=? AND status NOT"
            + " IN ('COMPLETED','DISCARDED')",
        outcome,
        store.simTime(),
        stringValue(batch, "id"));
    store.jdbc.update(
        "UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE id=? AND assigned_batch=?",
        stringValue(batch, "machine_id"),
        stringValue(batch, "id"));
    store.jdbc.update(
        "UPDATE command SET acknowledged=true WHERE payload->>'batch_id'=?",
        stringValue(batch, "id"));
    String orderId = stringValue(batch, "order_id");
    if (store.count(
            "SELECT count(*) FROM batch WHERE order_id=? AND status NOT IN"
                + " ('COMPLETED','DISCARDED')",
            orderId)
        == 0)
      store.jdbc.update(
          "UPDATE production_order SET status='FINISHED' WHERE id=? AND issued=quantity AND"
              + " status<>'CANCELLED'",
          orderId);
  }
}
