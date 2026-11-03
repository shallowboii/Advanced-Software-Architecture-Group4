package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Redis owns the live machine registry. PostgreSQL keeps identities, assignments and the last
 * observed snapshot for audit, not as a fallback source of liveness. A controller restart loses
 * physical execution state; the registry fails the run instead of reconstructing that state.
 */
@Service
public class MachineRegistry {
  private static final Logger log = LoggerFactory.getLogger(MachineRegistry.class);
  private static final Duration HEARTBEAT_TTL = Duration.ofSeconds(6);
  private final Store store;
  private final Json json;
  private final ProductionJournal journal;
  private final StringRedisTemplate redis;

  /** Wires durable coordination storage and the explicitly ephemeral Redis registry. */
  public MachineRegistry(
      Store store, Json json, ProductionJournal journal, StringRedisTemplate redis) {
    this.store = store;
    this.json = json;
    this.journal = journal;
    this.redis = redis;
  }

  /** Tests actual Redis reachability; an absent probe key is an ordinary successful response. */
  public boolean available() {
    try {
      redis.hasKey("machine:__readiness_probe__");
      return true;
    } catch (Exception unavailable) {
      return false;
    }
  }

  /** Serialises a heartbeat with scheduling so boot changes cannot race admission. */
  public void heartbeat(JsonNode snapshot) {
    store.locked(() -> acceptSnapshot(snapshot));
  }

  /**
   * Accepts only the current boot and nondecreasing snapshot versions. A fresh boot may reset the
   * version to zero. Previously retired boots remain rejected even when MQTT replays old messages.
   * The caller must hold the factory transaction lock.
   */
  public boolean acceptSnapshot(JsonNode snapshot) {
    String machineId = Json.required(snapshot, "machine_id");
    String type = Json.required(snapshot, "type");
    String bootId = Json.required(snapshot, "controller_boot_id");
    if (!Domain.STAGES.contains(type)
        || !machineId.matches("[a-z]+-[0-9]+")
        || !machineId.startsWith(type + "-")) {
      throw new IllegalArgumentException("invalid machine identity");
    }
    long version = snapshot.path("state_version").asLong(0);
    if (version < 0) throw new IllegalArgumentException("state_version must be nonnegative");
    Map<String, Object> previous = store.one("SELECT * FROM machine WHERE id=?", machineId);
    String previousBoot =
        previous == null ? null : Json.str(jsonValue(previous, "snapshot"), "controller_boot_id");
    boolean changedBoot = previous != null && !bootId.equals(previousBoot);
    if (changedBoot
        && store.count(
                "SELECT count(*) FROM controller_boot WHERE machine_id=? AND boot_id=?",
                machineId,
                bootId)
            > 0) {
      return false;
    }
    if (!changedBoot
        && previous != null
        && version < ((Number) previous.get("state_version")).longValue()) {
      return true; // Historical events may still be applied, but cannot refresh live state.
    }
    boolean lostExecution = changedBoot && hasUnresolvedExecution(previous);
    store.jdbc.update(
        "INSERT INTO machine(id,type,snapshot,state_version) VALUES(?,?,?::jsonb,?) ON CONFLICT(id)"
            + " DO UPDATE SET"
            + " snapshot=EXCLUDED.snapshot,state_version=EXCLUDED.state_version,last_seen=now()",
        machineId,
        type,
        json.write(snapshot),
        version);
    if (changedBoot && previousBoot != null) {
      store.jdbc.update(
          "INSERT INTO controller_boot(machine_id,boot_id,retired_at) VALUES(?,?,now()) ON"
              + " CONFLICT(machine_id,boot_id) DO UPDATE SET retired_at=now()",
          machineId,
          previousBoot);
      // Old broker-queued operations are fenced at the controller by their target boot ID.
      store.jdbc.update(
          "UPDATE command SET acknowledged=true WHERE machine_id=? AND acknowledged=false",
          machineId);
    }
    store.jdbc.update(
        "INSERT INTO controller_boot(machine_id,boot_id) VALUES(?,?) ON CONFLICT DO NOTHING",
        machineId,
        bootId);
    if (lostExecution || (previous == null && snapshot.path("occupied").asBoolean())) {
      failRunAfterStateLoss(machineId, previous, bootId);
    } else if (changedBoot) {
      journal.record(
          "CONTROLLER_RESTARTED",
          null,
          machineId,
          null,
          Map.of("controller_boot_id", bootId, "empty_restart", true));
      if (jsonValue(previous, "snapshot").path("local_stop").asBoolean())
        journal.command(machineId, "STOP", Map.of("scope", "local"));
      if (flag(store.state(), "stopped"))
        journal.command(machineId, "STOP", Map.of("scope", "global"));
    }
    cache(snapshot);
    return true;
  }

  /** Finds any durable evidence that a lost process may have held or been receiving material. */
  private boolean hasUnresolvedExecution(Map<String, Object> machine) {
    if (machine.get("assigned_batch") != null
        || jsonValue(machine, "snapshot").path("occupied").asBoolean()) return true;
    return store.count(
            "SELECT count(*) FROM command WHERE machine_id=? AND acknowledged=false AND cmd IN"
                + " ('START','PREPARE_TRANSFER','EXECUTE_TRANSFER','COMMIT_TRANSFER','ABORT_TRANSFER','FLUSH')",
            stringValue(machine, "id"))
        > 0;
  }

  /**
   * Preserves uncertain material as evidence, inhibits the entire line and invalidates the run. No
   * automatic loss/output claim is made: the RAM controller cannot prove what happened. Starting a
   * clean isolated factory is the supported way to begin another experiment.
   */
  private void failRunAfterStateLoss(
      String machineId, Map<String, Object> previous, String bootId) {
    String reason =
        "Controller "
            + machineId
            + " restarted with unresolved production; a clean factory is required";
    store.jdbc.update(
        "UPDATE factory_state SET"
            + " stopped=true,restart_blocked=true,failure_reason=?,experiment_done=true,control_version=control_version+1"
            + " WHERE id=1",
        reason);
    store.jdbc.update("UPDATE command SET acknowledged=true WHERE acknowledged=false");
    String affectedBatch = previous == null ? null : stringValue(previous, "assigned_batch");
    if (affectedBatch != null)
      store.jdbc.update(
          "UPDATE batch SET status='UNRESOLVED' WHERE id=? AND status NOT IN"
              + " ('COMPLETED','DISCARDED')",
          affectedBatch);
    Map<String, Object> details =
        Map.of("reason", reason, "controller_boot_id", bootId, "requires_clean_factory", true);
    store.jdbc.update(
        "INSERT INTO alarm(machine_id,reason,details) VALUES(?,'CONTROLLER_STATE_LOST',?::jsonb)",
        machineId,
        json.write(details));
    journal.record("CONTROLLER_STATE_LOST", affectedBatch, machineId, null, details);
    for (Map<String, Object> known : store.rows("SELECT id FROM machine"))
      journal.command(stringValue(known, "id"), "STOP", Map.of("scope", "global"));
  }

  /**
   * Refreshes only Redis's expiring live record; Redis failures never turn audit data into live
   * data.
   */
  private void cache(JsonNode snapshot) {
    try {
      redis
          .opsForValue()
          .set(
              "machine:" + Json.required(snapshot, "machine_id"),
              json.write(snapshot),
              HEARTBEAT_TTL);
    } catch (Exception unavailable) {
      log.debug("Live registry unavailable; controllers will lose permits", unavailable);
    }
  }

  /**
   * Returns a live record only when Redis has not expired it and its boot matches durable identity.
   */
  private JsonNode liveSnapshot(Map<String, Object> known) {
    try {
      String value = redis.opsForValue().get("machine:" + stringValue(known, "id"));
      if (value == null) return null;
      JsonNode snapshot = json.read(value);
      return Objects.equals(
              Json.str(snapshot, "controller_boot_id"),
              Json.str(jsonValue(known, "snapshot"), "controller_boot_id"))
          ? snapshot
          : null;
    } catch (Exception unavailable) {
      return null;
    }
  }

  /**
   * Combines live Redis data with durable identities; expired machines remain visible as offline.
   */
  public List<JsonNode> machines() {
    List<JsonNode> result = new ArrayList<>();
    for (Map<String, Object> known : store.rows("SELECT * FROM machine ORDER BY type,id")) {
      JsonNode live = liveSnapshot(known);
      ObjectNode snapshot =
          (ObjectNode) (live == null ? jsonValue(known, "snapshot") : live).deepCopy();
      snapshot.put("machine_id", stringValue(known, "id"));
      snapshot.put("type", stringValue(known, "type"));
      snapshot.put("online", live != null);
      snapshot.put("last_seen", stringValue(known, "last_seen"));
      enrichForDashboard(snapshot, known);
      result.add(snapshot);
    }
    return result;
  }

  /**
   * Supplies only Redis-live controllers to permit renewal; PostgreSQL timestamps are not leases.
   */
  public List<Map<String, Object>> onlineMachines() {
    return store.rows("SELECT * FROM machine ORDER BY id").stream()
        .filter(machine -> liveSnapshot(machine) != null)
        .toList();
  }

  /** Selects an unassigned, live, empty, uninhibited machine; callers hold the factory row lock. */
  public Map<String, Object> freeMachine(String type) {
    for (Map<String, Object> machine :
        store.rows(
            "SELECT * FROM machine WHERE type=? AND assigned_batch IS NULL ORDER BY id", type)) {
      JsonNode snapshot = liveSnapshot(machine);
      if (snapshot != null
          && snapshot.path("state").asText().equals("EMPTY")
          && !snapshot.path("local_stop").asBoolean()
          && !snapshot.path("global_stop").asBoolean()) return machine;
    }
    return null;
  }

  /** Confirms the source is still visible in Redis before attempting a downstream reservation. */
  public boolean online(String machineId) {
    Map<String, Object> machine = store.one("SELECT * FROM machine WHERE id=?", machineId);
    return machine != null && liveSnapshot(machine) != null;
  }

  /**
   * Enriches the dashboard from central assignments; these fields never enter controller traffic.
   */
  private void enrichForDashboard(ObjectNode snapshot, Map<String, Object> machine) {
    snapshot.put("phase_elapsed_h", snapshot.path("elapsed_h").asDouble());
    snapshot.put("phase_duration_h", snapshot.path("duration_h").asDouble());
    for (String field :
        new String[] {
          "batch_id",
          "execution_id",
          "recipe_version_id",
          "run_id",
          "transfer_id",
          "phase",
          "hold_deadline_h"
        }) snapshot.putNull(field);
    String batchId = stringValue(machine, "assigned_batch");
    if (batchId == null) return;
    Map<String, Object> batch = store.one("SELECT * FROM batch WHERE id=?", batchId);
    if (batch == null) return;
    snapshot.put("assigned_batch", batchId);
    snapshot.put("batch_id", batchId);
    snapshot.put("execution_id", stringValue(batch, "execution_id"));
    snapshot.put("recipe_version_id", stringValue(batch, "recipe_version_id"));
    snapshot.put("run_id", stringValue(batch, "run_id"));
    snapshot.put("phase_index", intValue(batch, "phase_index"));
    JsonNode phase =
        jsonValue(batch, "recipe")
            .path("stages")
            .get(intValue(batch, "stage"))
            .path("phases")
            .get(intValue(batch, "phase_index"));
    if (phase != null) snapshot.put("phase", phase.path("name").asText());
    if (batch.get("hold_deadline_h") instanceof Number deadline)
      snapshot.put("hold_deadline_h", deadline.doubleValue());
    Map<String, Object> transfer =
        store.one(
            "SELECT id FROM transfer WHERE batch_id=? AND status NOT IN ('DONE','ABORTED')",
            batchId);
    if (transfer != null) {
      snapshot.put("transfer_id", stringValue(transfer, "id"));
      snapshot.put("phase", "transfer");
    }
  }
}
