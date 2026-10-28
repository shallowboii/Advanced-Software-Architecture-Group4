package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs short scheduled coordination cycles. Only the PostgreSQL leader renews permits and publishes
 * commands; network calls occur outside material transactions.
 */
@Component
public class Coordinator {
  private static final Logger log = LoggerFactory.getLogger(Coordinator.class);
  private final Store store;
  private final FactoryService factory;
  private final Leadership leader;
  private final MqttGateway mqtt;
  private final KafkaGateway kafka;
  private final InventoryClient inventory;
  private final Json json;
  private final Bootstrap bootstrap;
  private final LiveSocket live;
  private final MachineRegistry registry;
  private final ControllerProtocol protocol;
  private final OperationScheduler operations;
  private final AtomicBoolean dispatching = new AtomicBoolean();

  /**
   * Wires the collaborators for this coordinator; no production transition occurs during
   * construction.
   */
  public Coordinator(
      Store store,
      FactoryService factory,
      Leadership leader,
      MqttGateway mqtt,
      KafkaGateway kafka,
      InventoryClient inventory,
      Json json,
      Bootstrap bootstrap,
      LiveSocket live,
      MachineRegistry registry,
      ControllerProtocol protocol,
      OperationScheduler operations) {
    this.store = store;
    this.factory = factory;
    this.leader = leader;
    this.mqtt = mqtt;
    this.kafka = kafka;
    this.inventory = inventory;
    this.json = json;
    this.bootstrap = bootstrap;
    this.live = live;
    this.registry = registry;
    this.protocol = protocol;
    this.operations = operations;
  }

  /** Checks both local leadership and the durable epoch before publishing coordinator work. */
  private boolean current(long epoch) {
    return bootstrap.ready
        && epoch > 0
        && leader.epoch() == epoch
        && ((Number) store.state().get("epoch")).longValue() == epoch;
  }

  @Scheduled(fixedDelay = 300)
  /**
   * Renews boot-targeted permits for Redis-live machines, or repeatedly inhibits the line while
   * global STOP is latched.
   */
  public void permits() {
    long epoch = leader.epoch();
    if (!bootstrap.ready || epoch == 0 || !mqtt.connected()) return;
    try {
      if (!current(epoch)) return;
      Map<String, Object> state = store.state();
      if (flag(state, "stopped")) {
        String inhibit = "backend-inhibit-" + epoch + "-" + state.get("control_version");
        mqtt.send(
            "factory/line/control",
            json.write(
                Map.of(
                    "schema_version",
                    2,
                    "command_id",
                    inhibit,
                    "message_id",
                    inhibit,
                    "cmd",
                    "STOP",
                    "scope",
                    "global",
                    "epoch",
                    epoch)),
            1);
      }
      for (Map<String, Object> machine : registry.onlineMachines()) {
        ObjectNode permit = json.object();
        String id = id();
        permit.put("schema_version", 2);
        permit.put("message_id", id);
        permit.put("command_id", id);
        permit.put("cmd", "PERMIT");
        permit.put("machine_id", stringValue(machine, "id"));
        permit.put(
            "controller_boot_id",
            Json.required(jsonValue(machine, "snapshot"), "controller_boot_id"));
        permit.put("epoch", epoch);
        permit.put("permit_ms", 3000);
        permit.put("sim_time_h", ((Number) state.get("sim_time_h")).doubleValue());
        permit.put("time_scale", ((Number) state.get("time_scale")).doubleValue());
        permit.put("ts_utc", Instant.now().toString());
        if (flag(state, "stopped")) {
          String inhibit = "backend-inhibit-" + epoch + "-" + state.get("control_version");
          permit.put("message_id", inhibit);
          permit.put("command_id", inhibit);
          permit.put("cmd", "STOP");
          permit.put("scope", "global");
        }
        mqtt.send(
            "factory/" + stringValue(machine, "type") + "/" + stringValue(machine, "id") + "/cmd",
            json.write(permit),
            flag(state, "stopped") ? 1 : 0);
      }
    } catch (Exception e) {
      log.debug("Permit renewal unavailable: {}", e.getClass().getSimpleName());
    }
  }

  @Scheduled(fixedDelay = 150)
  /**
   * Checks central quality deadlines, advances transfers and performs one idempotent inventory
   * admission outside the database lock.
   */
  public void schedule() {
    long epoch = leader.epoch();
    if (!bootstrap.ready
        || epoch == 0
        || !mqtt.connected()
        || !dispatching.compareAndSet(false, true)) return;
    try {
      if (!current(epoch)) return;
      for (Map<String, Object> cancelled : factory.cancelledReservations()) {
        inventory.call(
            "POST", "/reservations/" + stringValue(cancelled, "id") + "/release", json.object());
        factory.reservationReleased(stringValue(cancelled, "id"), epoch);
      }
      operations.checkTimeouts(epoch);
      factory.checkHorizon(epoch);
      factory.scheduleTransfers(epoch);
      Map<String, Object> batch = factory.beginAdmission(epoch);
      if (batch == null) return;
      try {
        inventory.call(
            "POST",
            "/reserve",
            json.tree(
                Map.of(
                    "batch_id",
                    stringValue(batch, "id"),
                    "ingredients",
                    jsonValue(batch, "recipe").path("ingredients"))));
        if (current(epoch)) factory.inventoryReserved(stringValue(batch, "id"), epoch);
      } catch (InventoryClient.Rejected e) {
        factory.inventoryBlocked(stringValue(batch, "id"), e.getMessage());
      } catch (InventoryClient.Unavailable e) {
        factory.inventoryBlocked(stringValue(batch, "id"), e.getMessage());
      }
    } catch (Exception e) {
      log.error("Scheduler cycle failed", e);
    } finally {
      dispatching.set(false);
    }
  }

  @Scheduled(fixedDelay = 200)
  /**
   * Retries pending actuator commands with their original ID and guards; the wire whitelist removes
   * all production metadata.
   */
  public void commands() {
    long epoch = leader.epoch();
    if (!bootstrap.ready || epoch == 0 || !mqtt.connected()) return;
    try {
      if (!current(epoch)) return;
      boolean stopped = flag(store.state(), "stopped");
      for (Map<String, Object> command :
          store.rows(
              "SELECT c.*,m.type FROM command c JOIN machine m ON m.id=c.machine_id WHERE"
                  + " c.acknowledged=false AND (c.last_sent IS NULL OR c.last_sent<now()-interval"
                  + " '1 second') ORDER BY CASE WHEN c.cmd='STOP' THEN 0 ELSE 1"
                  + " END,c.created_at,c.id LIMIT 100")) {
        String cmd = stringValue(command, "cmd");
        if (stopped
            && !Set.of("STOP", "SNAPSHOT", "ABORT_TRANSFER", "COMMIT_TRANSFER").contains(cmd))
          continue;
        if (!current(epoch)) return;
        ObjectNode payload = protocol.actuatorCommand(jsonValue(command, "payload"), epoch);
        payload.put("epoch", epoch);
        mqtt.send(
            "factory/"
                + stringValue(command, "type")
                + "/"
                + stringValue(command, "machine_id")
                + "/cmd",
            json.write(payload),
            1);
        store.jdbc.update(
            "UPDATE command SET last_sent=now(),attempts=attempts+1 WHERE id=?",
            stringValue(command, "id"));
      }
    } catch (Exception e) {
      log.debug("Command publication will retry: {}", e.getClass().getSimpleName());
    }
  }

  @Scheduled(fixedDelay = 250)
  /**
   * Publishes canonical Kafka outbox records only as leader; a failed send leaves the same event
   * pending.
   */
  public void events() {
    long epoch = leader.epoch();
    if (!bootstrap.ready || epoch == 0) return;
    try {
      if (!current(epoch)) return;
      for (Map<String, Object> row :
          store.rows("SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 100")) {
        if (!current(epoch)) return;
        kafka.publish(
            stringValue(row, "topic"),
            stringValue(row, "message_key"),
            json.write(jsonValue(row, "payload")));
        store.jdbc.update("UPDATE outbox SET sent_at=now() WHERE id=?", row.get("id"));
      }
    } catch (Exception e) {
      log.debug("Event publication will retry: {}", e.getClass().getSimpleName());
    }
  }

  @Scheduled(fixedDelay = 2000)
  /**
   * Checks pending central work and reconciles same-process actuator rejections. It never
   * reconstructs lost controller state.
   */
  public void reconcile() {
    long epoch = leader.epoch();
    if (!bootstrap.ready || epoch == 0) return;
    try {
      factory.reconcile(epoch);
    } catch (Exception e) {
      log.warn("Recovery pending: {}", e.getClass().getSimpleName());
    }
  }

  @Scheduled(fixedDelay = 1000)
  /**
   * Publishes a fresh central dashboard projection so clients can resynchronise after reconnecting.
   */
  public void snapshot() {
    if (!bootstrap.ready) return;
    try {
      live.publish(
          "snapshot",
          Map.of(
              "machines",
              factory.machines(),
              "line",
              factory.line(),
              "orders",
              factory.orders(),
              "experiment",
              factory.experimentStatus()));
    } catch (Exception e) {
      log.debug("Snapshot unavailable: {}", e.getClass().getSimpleName());
    }
  }
}
