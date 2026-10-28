package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Real PostgreSQL transactions with a deterministic Redis test double and schema-2 equipment
 * events.
 */
@EnabledIfEnvironmentVariable(named = "TEST_DATABASE_URL", matches = ".+")
class FactoryServiceTest {
  private final Json json = RecipesTest.JSON;
  private final Map<String, String> liveCache = new HashMap<>();
  private Store store;
  private FactoryService factory;
  private OperationScheduler operations;
  private ControllerProtocol protocol;
  private String schema;

  /** Creates a disposable schema; production tables outside this schema are never modified. */
  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    schema = "backend_test_" + UUID.randomUUID().toString().replace("-", "");
    var datasource =
        new DriverManagerDataSource(
            System.getenv("TEST_DATABASE_URL"),
            Bootstrap.env("TEST_DATABASE_USER", "beerfactory"),
            Bootstrap.env("TEST_DATABASE_PASSWORD", "dev-only-password"));
    var properties = new Properties();
    properties.setProperty("currentSchema", schema);
    datasource.setConnectionProperties(properties);
    Flyway.configure().dataSource(datasource).schemas(schema).load().migrate();
    store =
        new Store(
            new JdbcTemplate(datasource),
            new TransactionTemplate(new DataSourceTransactionManager(datasource)),
            json);
    StringRedisTemplate redis = mock(StringRedisTemplate.class);
    ValueOperations<String, String> values = mock(ValueOperations.class);
    when(redis.opsForValue()).thenReturn(values);
    when(values.get(anyString())).thenAnswer(call -> liveCache.get(call.getArgument(0)));
    doAnswer(
            call -> {
              liveCache.put(call.getArgument(0), call.getArgument(1));
              return null;
            })
        .when(values)
        .set(anyString(), anyString(), any(Duration.class));
    var journal = new ProductionJournal(store, json);
    var registry = new MachineRegistry(store, json, journal, redis);
    var stages = new StageCommands();
    var lifecycle = new BatchLifecycle(store, journal);
    var line = new LineService(store, journal, registry, lifecycle);
    var transfers = new TransferCoordinator(store, journal, lifecycle, registry, stages);
    var admission = new AdmissionScheduler(store, json, journal, registry, stages);
    protocol = new ControllerProtocol(store, json);
    operations = new OperationScheduler(store, journal, stages, lifecycle, registry);
    var events =
        new MachineEvents(
            store,
            json,
            journal,
            registry,
            line,
            lifecycle,
            transfers,
            protocol,
            operations,
            stages);
    factory =
        new FactoryService(
            store,
            json,
            new OrderService(store, json, journal),
            registry,
            line,
            lifecycle,
            events,
            admission,
            transfers,
            new ExperimentService(store, journal),
            new HistoryService(store, json, new LiveSocket(json), protocol));
    store.jdbc.update(
        "INSERT INTO factory_state(id,epoch,time_scale,policy,wip_limit,run_id)"
            + " VALUES(1,1,1,'CONWIP',20,'test')");
    store.jdbc.update(
        "INSERT INTO recipe_version(recipe_id,version,definition) VALUES('lager',1,?::jsonb)",
        json.write(RecipesTest.recipe()));
    for (String type : Domain.STAGES) factory.heartbeat(emptySnapshot(type + "-1", id()));
  }

  /** Drops only the uniquely named schema allocated by this test. */
  @AfterEach
  void teardown() {
    if (store != null) store.jdbc.execute("DROP SCHEMA " + schema + " CASCADE");
  }

  /** Creates a controller's initial technical observation without any production identifiers. */
  private ObjectNode emptySnapshot(String machine, String boot) {
    ObjectNode snapshot = json.object();
    snapshot.put("schema_version", 2);
    snapshot.put("machine_id", machine);
    snapshot.put("type", machine.substring(0, machine.lastIndexOf('-')));
    snapshot.put("controller_boot_id", boot);
    snapshot.put("state", "EMPTY");
    snapshot.put("state_version", 0);
    snapshot.put("occupied", false);
    snapshot.put("global_stop", false);
    snapshot.put("local_stop", false);
    snapshot.put("stopped", false);
    return snapshot;
  }

  /** Admits one order and acknowledges its raw loading operation. */
  private Map<String, Object> admit() {
    JsonNode request = json.read("{\"recipe_id\":\"lager\",\"quantity\":1}");
    factory.mutate("orders", id(), request, () -> factory.order(request, "test"));
    Map<String, Object> batch = factory.beginAdmission(1);
    assertNotNull(batch);
    factory.inventoryReserved(stringValue(batch, "id"), 1);
    emit("mash-1", latest("mash-1", "START"), "PROCESS_STARTED", "PROCESSING", false);
    return batch(stringValue(batch, "id"));
  }

  /** Reads the central projection, which is deliberately absent from equipment messages. */
  private Map<String, Object> batch(String batchId) {
    return store.one("SELECT * FROM batch WHERE id=?", batchId);
  }

  /** Finds the actual actuator command whose result the simulated controller will report. */
  private Map<String, Object> latest(String machine, String command) {
    Map<String, Object> found =
        store.one(
            "SELECT * FROM command WHERE machine_id=? AND cmd=? ORDER BY created_at DESC,id DESC"
                + " LIMIT 1",
            machine,
            command);
    assertNotNull(found, "missing " + command + " for " + machine);
    return found;
  }

  /**
   * Emits a technical-only event and snapshot; central business correlation must use command_id.
   */
  private ObjectNode emit(
      String machine, Map<String, Object> command, String kind, String state, boolean discarded) {
    Map<String, Object> known = store.one("SELECT * FROM machine WHERE id=?", machine);
    ObjectNode snapshot = (ObjectNode) jsonValue(known, "snapshot").deepCopy();
    snapshot.put("state", state);
    snapshot.put("occupied", !Set.of("EMPTY", "RESERVED_IN", "TRANSFERRING_IN").contains(state));
    snapshot.put("state_version", ((Number) known.get("state_version")).longValue() + 1);
    String commandId = command == null ? "physical-" + id() : stringValue(command, "id");
    String interrupted = Json.str(snapshot, "current_command_id");
    snapshot.put("current_command_id", commandId);
    ObjectNode event = json.object();
    event.put("schema_version", 2);
    event.put("event_id", id());
    event.put("event", kind);
    event.put("command_id", commandId);
    event.put("machine_id", machine);
    event.put("controller_boot_id", Json.required(snapshot, "controller_boot_id"));
    event.put("sim_time_h", store.simTime());
    event.put("ts_utc", Instant.now().toString());
    event.set("snapshot", snapshot);
    if (command == null) {
      event.put("interrupted_command_id", interrupted);
      snapshot.put("interrupted_command_id", interrupted);
    }
    if (kind.equals("STOPPED")) {
      event.put("scope", "global");
      snapshot.put("global_stop", true);
      snapshot.put("stopped", true);
    }
    if (discarded) event.put("contents_discarded", true);
    factory.machineEvent(event);
    return event;
  }

  /** Completes each scalar operation of the current stage, including both unitank phases. */
  private void completeStage(String batchId) {
    for (int step = 0; step < 3; step++) {
      Map<String, Object> batch = batch(batchId);
      if (!stringValue(batch, "status").equals("PROCESSING")) return;
      Map<String, Object> command =
          store.one(
              "SELECT * FROM command WHERE payload->>'batch_id'=? AND cmd IN ('START','PROCESS')"
                  + " AND (payload->>'phase_index')::integer=? ORDER BY created_at DESC,id DESC"
                  + " LIMIT 1",
              batchId,
              intValue(batch, "phase_index"));
      assertNotNull(command);
      emit(stringValue(batch, "machine_id"), command, "PROCESS_COMPLETED", "READY", false);
    }
    fail("stage did not finish after its declared phases");
  }

  /** Drives the two-end technical transfer up to the central commit decision. */
  private Map<String, Object> prepare(Map<String, Object> batch) {
    completeStage(stringValue(batch, "id"));
    factory.scheduleTransfers(1);
    Map<String, Object> transfer =
        store.one(
            "SELECT * FROM transfer WHERE batch_id=? AND status='PREPARING'",
            stringValue(batch, "id"));
    assertNotNull(transfer);
    String source = stringValue(transfer, "source_id"), target = stringValue(transfer, "target_id");
    emit(source, latest(source, "PREPARE_TRANSFER"), "TRANSFER_PREPARED", "READY", false);
    emit(target, latest(target, "PREPARE_TRANSFER"), "TRANSFER_PREPARED", "RESERVED_IN", false);
    emit(source, latest(source, "EXECUTE_TRANSFER"), "TRANSFER_READY", "TRANSFERRING_OUT", false);
    emit(target, latest(target, "EXECUTE_TRANSFER"), "TRANSFER_READY", "TRANSFERRING_IN", false);
    assertEquals(
        "COMMITTED",
        stringValue(
            store.one("SELECT * FROM transfer WHERE id=?", stringValue(transfer, "id")), "status"));
    assertEquals(
        0,
        store.count(
            "SELECT count(*) FROM command WHERE machine_id=? AND cmd='COMMIT_TRANSFER' AND"
                + " payload->>'transfer_id'=?",
            target,
            stringValue(transfer, "id")));
    return transfer;
  }

  /**
   * Tests all six stages, two unitank commands, immutable recipes and duplicate release delivery.
   */
  @Test
  void sixStagesRemainCentralAndOutputIsCountedOnce() {
    Map<String, Object> initial = admit();
    String batchId = stringValue(initial, "id");
    var changed = RecipesTest.recipe();
    changed.put("name", "Changed recipe");
    factory.locked(() -> factory.publishRecipe(changed, "lager"));
    for (int stage = 0; stage < 5; stage++) {
      Map<String, Object> transfer = prepare(batch(batchId));
      String source = stringValue(transfer, "source_id"),
          target = stringValue(transfer, "target_id");
      ObjectNode ack =
          emit(source, latest(source, "COMMIT_TRANSFER"), "TRANSFER_COMMITTED", "EMPTY", false);
      factory.machineEvent(ack);
      emit(target, latest(target, "COMMIT_TRANSFER"), "TRANSFER_COMMITTED", "READY", false);
      emit(target, latest(target, "PROCESS"), "PROCESS_STARTED", "PROCESSING", false);
      assertEquals(stage + 1, intValue(batch(batchId), "stage"));
      assertEquals("lager:1", stringValue(batch(batchId), "recipe_version_id"));
    }
    completeStage(batchId);
    assertEquals("OUTPUT_PENDING", stringValue(batch(batchId), "status"));
    Map<String, Object> stoppedRelease = latest("filter-1", "RELEASE");
    reject("filter-1", stoppedRelease, "machine_stopped", true);
    factory.reconcile(1);
    assertEquals(
        stringValue(stoppedRelease, "id"), stringValue(latest("filter-1", "RELEASE"), "id"));
    ObjectNode resumed =
        (ObjectNode)
            jsonValue(store.one("SELECT snapshot FROM machine WHERE id='filter-1'"), "snapshot")
                .deepCopy();
    resumed.put("stopped", false);
    resumed.put("state_version", resumed.path("state_version").asLong() + 1);
    factory.heartbeat(resumed);
    factory.reconcile(1);
    assertNotEquals(
        stringValue(stoppedRelease, "id"), stringValue(latest("filter-1", "RELEASE"), "id"));
    ObjectNode release =
        emit("filter-1", latest("filter-1", "RELEASE"), "RELEASED", "EMPTY", false);
    factory.machineEvent(release);
    assertEquals("COMPLETED", stringValue(batch(batchId), "status"));
    assertEquals(0, factory.activeWip());
    assertEquals(50L, factory.experimentStatus().get("hl_out"));
    assertEquals(
        2,
        store.count("SELECT count(*) FROM command WHERE machine_id='unitank-1' AND cmd='PROCESS'"));
    assertEquals(1, store.count("SELECT count(*) FROM production_event WHERE event='COMPLETED'"));
  }

  /** A source discard after the decision never starts a receiver operation and counts loss once. */
  @Test
  void physicalFlushAfterDecisionNeverProcessesReceiver() {
    Map<String, Object> original = admit(), transfer = prepare(original);
    String source = stringValue(transfer, "source_id"), target = stringValue(transfer, "target_id");
    ObjectNode ack =
        emit(source, latest(source, "COMMIT_TRANSFER"), "TRANSFER_COMMITTED", "EMPTY", true);
    factory.machineEvent(ack);
    emit(target, latest(target, "COMMIT_TRANSFER"), "TRANSFER_COMMITTED", "READY", false);
    assertEquals(
        0,
        store.count("SELECT count(*) FROM command WHERE machine_id=? AND cmd='PROCESS'", target));
    emit(target, latest(target, "FLUSH"), "FLUSHED", "EMPTY", false);
    assertEquals("DISCARDED", stringValue(batch(stringValue(original, "id")), "status"));
    assertEquals(1L, factory.experimentStatus().get("operator_discarded"));
    assertEquals(50L, factory.experimentStatus().get("hl_wasted"));
  }

  /** Physical intervention at the receiver is linked through its actual interrupted command. */
  @Test
  void receiverFlushDuringPreparationAbortsBothEnds() {
    Map<String, Object> batch = admit();
    completeStage(stringValue(batch, "id"));
    factory.scheduleTransfers(1);
    Map<String, Object> transfer =
        store.one("SELECT * FROM transfer WHERE batch_id=?", stringValue(batch, "id"));
    String target = stringValue(transfer, "target_id"), source = stringValue(transfer, "source_id");
    emit(target, latest(target, "PREPARE_TRANSFER"), "TRANSFER_PREPARED", "RESERVED_IN", false);
    emit(target, null, "FLUSH_REQUESTED", "FLUSH_PENDING", false);
    assertEquals(
        "ABORTING",
        stringValue(
            store.one("SELECT * FROM transfer WHERE id=?", stringValue(transfer, "id")), "status"));
    emit(target, latest(target, "ABORT_TRANSFER"), "TRANSFER_ABORTED", "EMPTY", false);
    emit(source, latest(source, "ABORT_TRANSFER"), "TRANSFER_ABORTED", "READY", false);
    assertEquals(
        1, store.count("SELECT count(*) FROM production_event WHERE event='LOSS_RECORDED'"));
    assertNotNull(latest(source, "FLUSH"));
  }

  /**
   * Quality policy is central; a technical READY controller never receives recipe holding limits.
   */
  @Test
  void schedulerOwnsSpoilageDeadline() {
    Map<String, Object> batch = admit();
    completeStage(stringValue(batch, "id"));
    store.jdbc.update("UPDATE batch SET hold_deadline_h=-1 WHERE id=?", stringValue(batch, "id"));
    operations.checkTimeouts(1);
    assertEquals("spoilage", stringValue(batch(stringValue(batch, "id")), "loss_reason"));
    assertEquals(1L, factory.experimentStatus().get("spoiled"));
    JsonNode wire = protocol.actuatorCommand(jsonValue(latest("mash-1", "FLUSH"), "payload"), 1);
    assertFalse(wire.has("batch_id"));
    assertFalse(wire.has("hold_h"));
  }

  /** A process restart with possible material invalidates the run and never silently reloads it. */
  @Test
  void busyRestartFailsClosedAndRetiredBootCannotOverwrite() {
    Map<String, Object> batch = admit();
    JsonNode previous =
        jsonValue(store.one("SELECT snapshot FROM machine WHERE id='mash-1'"), "snapshot");
    ObjectNode fresh = emptySnapshot("mash-1", id());
    factory.heartbeat(fresh);
    assertTrue(flag(store.state(), "restart_blocked"));
    assertTrue(flag(store.state(), "stopped"));
    assertEquals("UNRESOLVED", stringValue(batch(stringValue(batch, "id")), "status"));
    assertEquals(true, factory.experimentStatus().get("failed"));
    assertThrows(
        FactoryService.Conflict.class, () -> factory.locked(() -> factory.stopLine(false, "test")));
    factory.heartbeat(previous);
    assertEquals(
        fresh.path("controller_boot_id"),
        jsonValue(store.one("SELECT snapshot FROM machine WHERE id='mash-1'"), "snapshot")
            .path("controller_boot_id"));
    assertEquals(
        0, store.count("SELECT count(*) FROM command WHERE acknowledged=false AND cmd<>'STOP'"));
  }

  /**
   * Empty equipment can restart; expired Redis keys cannot be replaced by PostgreSQL audit data.
   */
  @Test
  void emptyRestartResetsVersionAndRedisExpiryMakesMachineOffline() {
    ObjectNode previous =
        (ObjectNode)
            jsonValue(store.one("SELECT snapshot FROM machine WHERE id='mash-1'"), "snapshot")
                .deepCopy();
    previous.put("state_version", 99);
    factory.heartbeat(previous);
    ObjectNode fresh = emptySnapshot("mash-1", id());
    factory.heartbeat(fresh);
    factory.heartbeat(previous);
    assertFalse(flag(store.state(), "restart_blocked"));
    assertEquals(
        0L, store.one("SELECT state_version FROM machine WHERE id='mash-1'").get("state_version"));
    liveCache.remove("machine:mash-1");
    assertFalse(
        factory.machines().stream()
            .filter(machine -> machine.path("machine_id").asText().equals("mash-1"))
            .findFirst()
            .orElseThrow()
            .path("online")
            .asBoolean());
    JsonNode request = json.read("{\"recipe_id\":\"lager\",\"quantity\":1}");
    factory.mutate("orders", id(), request, () -> factory.order(request, "test"));
    assertNull(factory.beginAdmission(1));
  }

  /**
   * Delayed telemetry belongs to the old actuator command even after a new batch enters the
   * machine.
   */
  @Test
  void lateTelemetryUsesCommandHistoryNotCurrentOccupancy() {
    Map<String, Object> original = admit();
    String oldCommand = stringValue(latest("mash-1", "START"), "id");
    String boot =
        Json.required(jsonValue(latest("mash-1", "START"), "payload"), "controller_boot_id");
    factory.locked(() -> factory.flushBatch(stringValue(original, "id"), "operator", "test"));
    emit("mash-1", latest("mash-1", "FLUSH"), "FLUSHED", "EMPTY", false);
    Map<String, Object> next = admit();
    assertNotEquals(stringValue(original, "id"), stringValue(next, "id"));
    ObjectNode sensor = json.object();
    sensor.put("schema_version", 2);
    sensor.put("message_id", id());
    sensor.put("machine_id", "mash-1");
    sensor.put("controller_boot_id", boot);
    sensor.put("current_command_id", oldCommand);
    sensor.put("sensor", "temp_c");
    sensor.put("value", 65);
    sensor.put("unit", "C");
    sensor.put("ts_utc", Instant.now().toString());
    sensor.put("sim_time_h", store.simTime());
    factory.telemetry(sensor);
    assertEquals(1, factory.events(Map.of("batch_id", stringValue(original, "id")), true).size());
    assertTrue(factory.events(Map.of("batch_id", stringValue(next, "id")), true).isEmpty());
  }

  /** Cancelled in-flight inventory work cannot become a physical loading command. */
  @Test
  void cancelledReservationCannotAdmit() {
    JsonNode request = json.read("{\"recipe_id\":\"lager\",\"quantity\":1}");
    JsonNode order = factory.mutate("orders", id(), request, () -> factory.order(request, "test"));
    Map<String, Object> pending = factory.beginAdmission(1);
    factory.locked(() -> factory.cancelOrder(order.path("id").asText(), "test"));
    factory.inventoryReserved(stringValue(pending, "id"), 1);
    factory.reservationReleased(stringValue(pending, "id"), 1);
    assertNull(batch(stringValue(pending, "id")));
    assertEquals(0, store.count("SELECT count(*) FROM command WHERE cmd='START'"));
  }

  /** STOP export barriers require application acknowledgements, not merely stopped heartbeats. */
  @Test
  void stoppedHeartbeatDoesNotSatisfyTrackedStopBarrier() {
    factory.locked(() -> factory.stopLine(true, "test"));
    for (Map<String, Object> row : store.rows("SELECT snapshot FROM machine")) {
      ObjectNode snapshot = (ObjectNode) jsonValue(row, "snapshot").deepCopy();
      snapshot.put("global_stop", true);
      snapshot.put("state_version", 1);
      factory.heartbeat(snapshot);
    }
    assertEquals(6, ((List<?>) ((Map<?, ?>) factory.line()).get("machines_pending_stop")).size());
    emit("mash-1", latest("mash-1", "STOP"), "STOPPED", "EMPTY", false);
    assertEquals(5, ((List<?>) ((Map<?, ?>) factory.line()).get("machines_pending_stop")).size());
  }

  /** Mutation retries preserve one response and the canonical export retains insertion order. */
  @Test
  void idempotencyAndEventOrderingRemainStable() {
    JsonNode request = json.read("{\"recipe_id\":\"lager\",\"quantity\":2}");
    JsonNode response =
        factory.mutate("orders", "repeat", request, () -> factory.order(request, "test"));
    assertEquals(
        json.read(json.write(response)),
        factory.mutate(
            "orders",
            "repeat",
            request,
            () -> {
              throw new AssertionError("reexecuted mutation");
            }));
    assertThrows(
        FactoryService.Conflict.class,
        () -> factory.mutate("orders", "repeat", json.object(), () -> Map.of()));
    factory.locked(() -> factory.stopLine(true, "test"));
    assertNull(factory.beginAdmission(1));
    factory.locked(() -> factory.stopLine(false, "test"));
    assertNotNull(factory.beginAdmission(1));
    long first =
        factory.events(Map.of("limit", "1"), false).getFirst().path("recorded_seq").asLong();
    assertTrue(
        factory
                .events(Map.of("order", "desc", "limit", "1"), false)
                .getFirst()
                .path("recorded_seq")
                .asLong()
            > first);
    assertTrue(
        factory.events(Map.of("after", Long.toString(first)), false).stream()
            .allMatch(event -> event.path("recorded_seq").asLong() > first));
  }

  /** Resuming cannot forgive earlier interruptions of the same physical recipe operation. */
  @Test
  void repeatedPausesConsumeOneCumulativePhaseBudget() {
    Map<String, Object> batch = admit();
    ObjectNode snapshot =
        (ObjectNode)
            jsonValue(store.one("SELECT snapshot FROM machine WHERE id='mash-1'"), "snapshot")
                .deepCopy();
    double[] times = {0, 4, 5, 9, 10, 12};
    for (int index = 0; index < times.length; index++) {
      snapshot.put("sim_time_h", times[index]);
      snapshot.put("stopped", index % 2 == 0);
      factory.locked(
          () -> {
            operations.observeMachine("mash-1", snapshot);
            return null;
          });
    }
    assertEquals("spoilage", stringValue(batch(stringValue(batch, "id")), "loss_reason"));
    assertEquals(
        10.0, ((Number) batch(stringValue(batch, "id")).get("pause_accumulated_h")).doubleValue());
    assertEquals(
        1, store.count("SELECT count(*) FROM production_event WHERE event='LOSS_RECORDED'"));
  }

  /** Supplies a real rejection envelope without changing its immutable actuator correlation. */
  private void reject(String machine, Map<String, Object> command, String reason, boolean stopped) {
    Map<String, Object> known = store.one("SELECT * FROM machine WHERE id=?", machine);
    ObjectNode snapshot = (ObjectNode) jsonValue(known, "snapshot").deepCopy();
    snapshot.put("state_version", ((Number) known.get("state_version")).longValue() + 1);
    snapshot.put("stopped", stopped);
    ObjectNode event = json.object();
    event.put("schema_version", 2);
    event.put("event_id", id());
    event.put("event", "REJECTED");
    event.put("reason", reason);
    event.put("command_id", stringValue(command, "id"));
    event.put("machine_id", machine);
    event.put("controller_boot_id", Json.required(snapshot, "controller_boot_id"));
    event.set("snapshot", snapshot);
    factory.machineEvent(event);
  }

  /** A committed transfer's source still needs release retries after durable ownership moves. */
  @Test
  void sourceCommitRetryDoesNotRequireCurrentBatchOwnership() {
    Map<String, Object> batch = admit(), transfer = prepare(batch);
    String source = stringValue(transfer, "source_id");
    Map<String, Object> original = latest(source, "COMMIT_TRANSFER");
    reject(source, original, "state_version_conflict", false);
    assertNotEquals(
        stringValue(original, "id"), stringValue(latest(source, "COMMIT_TRANSFER"), "id"));
    assertEquals(
        stringValue(transfer, "target_id"),
        stringValue(batch(stringValue(batch, "id")), "machine_id"));
  }

  /** The receiving endpoint can abort even though it has never owned the batch in PostgreSQL. */
  @Test
  void receiverAbortRetryDoesNotRequireBatchOwnership() {
    Map<String, Object> batch = admit();
    completeStage(stringValue(batch, "id"));
    factory.scheduleTransfers(1);
    Map<String, Object> transfer =
        store.one("SELECT * FROM transfer WHERE batch_id=?", stringValue(batch, "id"));
    String target = stringValue(transfer, "target_id");
    emit(target, latest(target, "PREPARE_TRANSFER"), "TRANSFER_PREPARED", "RESERVED_IN", false);
    emit(target, null, "FLUSH_REQUESTED", "FLUSH_PENDING", false);
    Map<String, Object> original = latest(target, "ABORT_TRANSFER");
    reject(target, original, "state_version_conflict", false);
    assertNotEquals(
        stringValue(original, "id"), stringValue(latest(target, "ABORT_TRANSFER"), "id"));
  }

  /**
   * A rejected next operation waits for resume and cannot repeat after its later successful start.
   */
  @Test
  void stoppedProcessRetriesOnceAfterResume() {
    Map<String, Object> batch = admit(), transfer = prepare(batch);
    String source = stringValue(transfer, "source_id"), target = stringValue(transfer, "target_id");
    emit(source, latest(source, "COMMIT_TRANSFER"), "TRANSFER_COMMITTED", "EMPTY", false);
    emit(target, latest(target, "COMMIT_TRANSFER"), "TRANSFER_COMMITTED", "READY", false);
    Map<String, Object> original = latest(target, "PROCESS");
    reject(target, original, "machine_stopped", true);
    factory.reconcile(1);
    assertEquals(stringValue(original, "id"), stringValue(latest(target, "PROCESS"), "id"));
    ObjectNode snapshot =
        (ObjectNode)
            jsonValue(store.one("SELECT snapshot FROM machine WHERE id=?", target), "snapshot")
                .deepCopy();
    snapshot.put("stopped", false);
    snapshot.put("state_version", snapshot.path("state_version").asLong() + 1);
    factory.heartbeat(snapshot);
    factory.reconcile(1);
    Map<String, Object> retry = latest(target, "PROCESS");
    assertNotEquals(stringValue(original, "id"), stringValue(retry, "id"));
    emit(target, retry, "PROCESS_STARTED", "PROCESSING", false);
    // A READY heartbeat can overtake completion: it must not cause an accepted operation to repeat.
    snapshot =
        (ObjectNode)
            jsonValue(store.one("SELECT snapshot FROM machine WHERE id=?", target), "snapshot")
                .deepCopy();
    snapshot.put("state", "READY");
    snapshot.put("state_version", snapshot.path("state_version").asLong() + 1);
    factory.heartbeat(snapshot);
    factory.reconcile(1);
    assertEquals(stringValue(retry, "id"), stringValue(latest(target, "PROCESS"), "id"));
  }

  /** Cancelling a never-loaded admission fences broker-queued loading before releasing its slot. */
  @Test
  void emptyFlushUsesVersionBarrierBeforeDiscard() {
    JsonNode request = json.read("{\"recipe_id\":\"lager\",\"quantity\":1}");
    factory.mutate("orders", id(), request, () -> factory.order(request, "test"));
    Map<String, Object> batch = factory.beginAdmission(1);
    factory.inventoryReserved(stringValue(batch, "id"), 1);
    factory.locked(() -> factory.flushBatch(stringValue(batch, "id"), "operator", "test"));
    reject("mash-1", latest("mash-1", "FLUSH"), "machine_empty", false);
    assertEquals("FLUSH_PENDING", stringValue(batch(stringValue(batch, "id")), "status"));
    assertEquals(
        0, store.count("SELECT count(*) FROM command WHERE cmd='START' AND acknowledged=false"));
    emit("mash-1", latest("mash-1", "ABORT_TRANSFER"), "TRANSFER_ABORTED", "EMPTY", false);
    assertEquals("DISCARDED", stringValue(batch(stringValue(batch, "id")), "status"));
    assertEquals(0, factory.activeWip());
  }
}
