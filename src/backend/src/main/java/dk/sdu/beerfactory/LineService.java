package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.FactoryService.*;
import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Controls global and local inhibition independently of recipe execution. STOP acknowledgement
 * barriers include offline and unacknowledged equipment.
 */
@Service
public class LineService {
  private final Store store;
  private final ProductionJournal journal;
  private final MachineRegistry registry;
  private final BatchLifecycle batches;

  /**
   * Wires the collaborators for this line service; no production transition occurs during
   * construction.
   */
  public LineService(
      Store store, ProductionJournal journal, MachineRegistry registry, BatchLifecycle batches) {
    this.store = store;
    this.journal = journal;
    this.registry = registry;
    this.batches = batches;
  }

  /** Returns line policy, inhibition and the tracked per-machine STOP acknowledgement barrier. */
  public Object line() {
    Map<String, Object> state = store.state();
    state.put("active_wip", store.activeWip());
    state.put("recovery_required", flag(state, "restart_blocked"));
    Set<String> awaitingAck = new HashSet<>();
    for (Map<String, Object> command :
        store.rows(
            "SELECT DISTINCT machine_id FROM command WHERE cmd='STOP' AND"
                + " payload->>'scope'='global' AND acknowledged=false"))
      awaitingAck.add(stringValue(command, "machine_id"));
    state.put(
        "machines_pending_stop",
        registry.machines().stream()
            .filter(
                m ->
                    !m.path("online").asBoolean()
                        || !m.path("global_stop").asBoolean()
                        || awaitingAck.contains(m.path("machine_id").asText()))
            .map(m -> m.path("machine_id").asText())
            .toList());
    if (!flag(state, "stopped")) state.put("machines_pending_stop", List.of());
    return state;
  }

  /**
   * Persists a global latch and tracks acknowledgements; an unresolved controller restart cannot be
   * resumed.
   */
  public Object stopLine(boolean stop, String operator) {
    if (!stop && flag(store.state(), "restart_blocked"))
      throw new Conflict("Controller state was lost; start a clean factory before resuming");
    store.jdbc.update(
        "UPDATE factory_state SET stopped=?,control_version=control_version+1 WHERE id=1", stop);
    if (stop) supersedeUnsafeCommands();
    else
      store.jdbc.update(
          "UPDATE command SET acknowledged=true WHERE cmd='STOP' AND payload->>'scope'='global' AND"
              + " acknowledged=false");
    for (Map<String, Object> machine : store.rows("SELECT id,type FROM machine"))
      journal.command(
          stringValue(machine, "id"), stop ? "STOP" : "RESUME", Map.of("scope", "global"));
    journal.record(
        stop ? "LINE_STOPPED" : "LINE_RESUMED", null, null, null, Map.of("operator", operator));
    return line();
  }

  /** Retires pending resumes when a newer global STOP takes precedence. */
  public void supersedeUnsafeCommands() {
    // Inhibition is separate from operations: retries resume after the latch is explicitly cleared.
    store.jdbc.update(
        "UPDATE command SET acknowledged=true WHERE cmd='RESUME' AND acknowledged=false");
  }

  /**
   * Applies an explicit local latch or centrally resolved flush without silently clearing the
   * global latch.
   */
  public Object machineControl(
      String machineId, String operation, JsonNode input, String operator) {
    if (!operation.equals("stop") && flag(store.state(), "restart_blocked"))
      throw new Conflict("Controller state was lost; material cannot be reconciled automatically");
    Map<String, Object> machine =
        require(store.one("SELECT * FROM machine WHERE id=?", machineId), "machine");
    if (operation.equals("flush")) {
      String batchId = Json.required(input, "batch_id");
      JsonNode snap = jsonValue(machine, "snapshot");
      if (!batchId.equals(stringValue(machine, "assigned_batch"))
          && !batchId.equals(snap.path("batch_id").asText()))
        throw new Conflict("machine does not hold the requested batch");
      return batches.flushBatch(batchId, "operator", operator);
    }
    if (operation.equals("stop"))
      store.jdbc.update(
          "UPDATE command SET acknowledged=true WHERE machine_id=? AND cmd='RESUME' AND"
              + " acknowledged=false",
          machineId);
    if (operation.equals("resume"))
      store.jdbc.update(
          "UPDATE command SET acknowledged=true WHERE machine_id=? AND cmd='STOP' AND"
              + " payload->>'scope'='local' AND acknowledged=false",
          machineId);
    journal.command(
        machineId, operation.equals("stop") ? "STOP" : "RESUME", Map.of("scope", "local"));
    journal.record(
        operation.equals("stop") ? "LOCAL_STOP_REQUESTED" : "LOCAL_RESUME_REQUESTED",
        stringValue(machine, "assigned_batch"),
        machineId,
        null,
        Map.of("operator", operator));
    return Map.of("machine_id", machineId, "requested", operation);
  }
}
