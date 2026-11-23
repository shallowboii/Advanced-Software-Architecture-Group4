import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

/**
 * Greedy brewery scheduler 
 *
 * Run:  mvn -q compile exec:java -Dexec.args="3"
 */
public class GreedyScheduler {

    // ---- configuration ---------------------------------------------------
    static final int MAX_BATCHES = 64;
    static final int DEFAULT_BATCHES = 6;
    static final int RELEASE_INTERVAL = 0;          // hours between releases
    static final int TANK_STEP = 4;                 // step where the batch goes in a tank

    static final String BROKERS = env("KAFKA_BROKERS", "localhost:9092");
    static final String CONSUMER_GROUP = "brewery-scheduler";
    static final Pattern STATUS_TOPIC_PATTERN = Pattern.compile("^machine\\..+");
    static final int SECONDS_PER_SIM_HOUR = 1;      // 1 sim hour = 1 real second
    static final int WARMUP_S = 5;
    static final int RUN_TIMEOUT_S = 3600;
    static final long POLL_TIMEOUT_MS = 100;

    static final String MQTT_URI = env("MQTT_URI", "tcp://localhost:1883");
    static final String MQTT_CLIENT_ID = "brewery-scheduler-hb";
    static final String HEARTBEAT_TOPIC = "heartbeat";
    static final int HEARTBEAT_TIMEOUT_S = 30;
    static final int MAX_HEARTBEATS = 64;

    static final String REDIS_HOST = env("REDIS_HOST", "localhost");
    static final int REDIS_PORT = Integer.parseInt(env("REDIS_PORT", "6379"));

    // ---- plant definition ----
    record Step(String name, int count, int durationHours, String output) {}

    static final Step[] PLANT = {
        new Step("mashing",      1, 2, "mash_batch"),
        new Step("lautering",    1, 2, "sweet_wort_batch"),
        new Step("boiling",      1, 2, "boiled_wort_batch"),
        new Step("whirlpooling", 1, 1, "clarified_wort_batch"),
        new Step("fermentation", 5, 336, "green_beer_batch"),     // TANK_STEP
        new Step("filtering",    1, 2, "filtered_beer_batch"),
    };
    static final int NUM_STEPS = PLANT.length;
    static final int MAX_MACHINES = Arrays.stream(PLANT).mapToInt(Step::count).max().orElse(1);

    static int duration(int step, int machine) {
        return PLANT[step].durationHours();
    }

    // ---- types -----------------------------------------------------------
    enum StepState { PENDING, SENT, RUNNING, DONE, FAILED }

    static class Batch {
        int id;
        int release;
        int[] start = new int[NUM_STEPS];
        int[] machine = new int[NUM_STEPS];
        int tank;
        int end;
    }

    /** A status message from a machine. */
    record Status(String type, String machineType, int machineId, String batchId,
                  String errorCode, String errorReason) {}

    static class HbEntry {
        String name;
        Instant lastSeen;
        boolean silent;
    }

    // ---- run-time state --------------------------------------------------
    static final ObjectMapper JSON = new ObjectMapper();

    static Batch[] batches;
    static int n;
    static StepState[][] st;
    static boolean[] failed;
    static int[][] current = new int[NUM_STEPS][MAX_MACHINES];   // batch index or -1
    static int[] tankOwner = new int[MAX_MACHINES];              // batch index or -1

    static Jedis redis;
    static KafkaProducer<String, String> producer;
    static Instant t0;

    static final List<HbEntry> hbTab = new ArrayList<>();
    static final ConcurrentLinkedQueue<byte[]> hbInbox = new ConcurrentLinkedQueue<>();
    static MqttClient hbClient;

    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    static int simHour() {
        return (int) (Duration.between(t0 == null ? Instant.now() : t0, Instant.now())
                .toSeconds() / SECONDS_PER_SIM_HOUR);
    }

    static void log(String fmt, Object... a) {
        System.out.printf("h%4d: %s%n", simHour(), String.format(fmt, a));
    }

    static void err(String fmt, Object... a) {
        System.err.printf("h%4d: %s%n", simHour(), String.format(fmt, a));
    }

    // ---- redis -----------------------------------------------------------
    static void redisReset() {
        try {
            redis.del("brewery:last_started", "brewery:last_done_type", "brewery:done_count",
                      "brewery:batch_tank", "brewery:last_error", "brewery:heartbeat");
        } catch (Exception e) { System.err.println("Redis DEL failed: " + e.getMessage()); }
    }

    static void recordTank(int batchId, int tank) {
        try { redis.hset("brewery:batch_tank", "batch" + batchId, String.valueOf(tank + 1)); }
        catch (Exception e) { System.err.println("Redis HSET batch_tank failed: " + e.getMessage()); }
    }

    static void recordStart(int step, int machine, int batchId, int hour) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("step", String.valueOf(step + 1));
        m.put("step_name", PLANT[step].name());
        m.put("machine", String.valueOf(machine + 1));
        m.put("batch", String.valueOf(batchId));
        m.put("hour", String.valueOf(hour));
        m.put("real_time", String.valueOf(Instant.now().getEpochSecond()));
        try { redis.hset("brewery:last_started", m); }
        catch (Exception e) { System.err.println("Redis HSET last_started failed: " + e.getMessage()); }
    }

    static void recordDone(int step) {
        String type = PLANT[step].output();
        try {
            redis.hincrBy("brewery:done_count", type, 1);
            redis.set("brewery:last_done_type", type);
        } catch (Exception e) { System.err.println("Redis done_count failed: " + e.getMessage()); }
    }

    static void recordError(int step, int machine, int batchId, String code, String reason) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("step", String.valueOf(step + 1));
        m.put("step_name", PLANT[step].name());
        m.put("machine", String.valueOf(machine + 1));
        m.put("batch", String.valueOf(batchId));
        m.put("code", code.isEmpty() ? "-" : code);
        m.put("reason", reason.isEmpty() ? "-" : reason);
        m.put("real_time", String.valueOf(Instant.now().getEpochSecond()));
        try { redis.hset("brewery:last_error", m); }
        catch (Exception e) { System.err.println("Redis HSET last_error failed: " + e.getMessage()); }
    }

    // ---- heartbeats (MQTT) -----------------------------------------------
    static final int HB_SIZE = 4 + 32;
    
    static String parseHeartbeatName(byte[] p) {
        if (p.length != HB_SIZE) {
            System.err.printf("MQTT: heartbeat of %d bytes ignored (expected %d)%n", p.length, HB_SIZE);
            return "";
        }
        int len = 0;
        while (len < 31 && p[4 + len] != 0) len++;
        return new String(p, 4, len, StandardCharsets.US_ASCII);
    }

    static void hbStart() {
        try {
            hbClient = new MqttClient(MQTT_URI, MQTT_CLIENT_ID, new MemoryPersistence());
            hbClient.setCallback(new MqttCallback() {
                @Override public void connectionLost(Throwable c) {
                    System.err.println("MQTT: connection lost, heartbeats disabled");
                }
                @Override public void messageArrived(String topic, MqttMessage m) {
                    hbInbox.add(m.getPayload());
                }
                @Override public void deliveryComplete(IMqttDeliveryToken t) {}
            });
            MqttConnectOptions o = new MqttConnectOptions();
            o.setCleanSession(true);
            o.setKeepAliveInterval(60);
            o.setConnectionTimeout(3);
            hbClient.connect(o);
            hbClient.subscribe(HEARTBEAT_TOPIC, 0);
        } catch (Exception e) {
            System.err.println("MQTT unavailable, running without heartbeats: " + e.getMessage());
            hbStop();
        }
    }

    static void hbStop() {
        if (hbClient == null) return;
        try { if (hbClient.isConnected()) hbClient.disconnect(); hbClient.close(); }
        catch (Exception ignored) {}
        hbClient = null;
    }

    static void hbPoll() {
        byte[] p;
        while ((p = hbInbox.poll()) != null) {
            String name = parseHeartbeatName(p);
            if (name.isEmpty()) continue;

            HbEntry e = null;
            for (HbEntry x : hbTab) if (x.name.equals(name)) { e = x; break; }

            if (e == null) {
                if (hbTab.size() >= MAX_HEARTBEATS) continue;
                e = new HbEntry();
                e.name = name;
                hbTab.add(e);
                log("heartbeat: first one from %s", name);
            } else if (e.silent) {
                log("heartbeat: %s is back", name);
            }
            e.lastSeen = Instant.now();
            e.silent = false;
            try { redis.hset("brewery:heartbeat", name, String.valueOf(e.lastSeen.getEpochSecond())); }
            catch (Exception ex) { System.err.println("Redis HSET heartbeat failed: " + ex.getMessage()); }
        }

        Instant now = Instant.now();
        for (HbEntry e : hbTab)
            if (!e.silent && Duration.between(e.lastSeen, now).toSeconds() > HEARTBEAT_TIMEOUT_S) {
                e.silent = true;
                err("heartbeat: %s silent for over %d s", e.name, HEARTBEAT_TIMEOUT_S);
            }
    }

    // ---- scheduling rules ------------------------------------------------
    static int freeTank() {
        for (int m = 0; m < PLANT[TANK_STEP].count(); m++)
            if (tankOwner[m] < 0) return m;
        return -1;
    }

    static void releaseTanksOf(int b) {
        for (int m = 0; m < MAX_MACHINES; m++)
            if (tankOwner[m] == b) tankOwner[m] = -1;
    }

    static int prevActive(int b) {
        for (int p = b - 1; p >= 0; p--) if (!failed[p]) return p;
        return -1;
    }

    static boolean canStart(int b, int s) {
        int m = batches[b].machine[s];

        if (failed[b]) return false;
        if (st[b][s] != StepState.PENDING) return false;
        if (s > 0 && st[b][s - 1] != StepState.DONE) return false;

        if (s == 0) {
            int p = prevActive(b);
            if (p >= 0) {
                StepState ps = st[p][TANK_STEP];
                if (ps != StepState.RUNNING && ps != StepState.DONE) return false;
            }
            if (freeTank() < 0) return false;
        }

        if (current[s][m] >= 0) return false;

        if (s != TANK_STEP)
            for (int p = 0; p < b; p++) {
                if (failed[p]) continue;
                if (batches[p].machine[s] == m && st[p][s] != StepState.DONE) return false;
            }
        return true;
    }

    // ---- kafka -----------------------------------------------------------
    static void sendStart(int b, int s) {
        if (s == 0) {
            int t = freeTank();
            tankOwner[t] = b;
            batches[b].tank = t;
            batches[b].machine[TANK_STEP] = t;
            log("batch %d reserves tank %d", batches[b].id, t + 1);
        }

        int m = batches[b].machine[s];
        ObjectNode msg = JSON.createObjectNode();
        msg.put("type", "START");
        msg.put("machine_type", PLANT[s].name());
        msg.put("machine_id", m + 1);
        msg.put("batch_id", "batch" + batches[b].id);
        msg.put("message_id", "batch" + batches[b].id + "-step" + (s + 1) + "-start");
        msg.put("ts_utc", Instant.now().toString());
        ObjectNode params = msg.putObject("params");
        params.put("duration_s", (double) duration(s, m) * SECONDS_PER_SIM_HOUR);

        producer.send(new ProducerRecord<>("machine." + PLANT[s].name(),
                String.valueOf(m + 1), msg.toString()));

        st[b][s] = StepState.SENT;
        current[s][m] = b;
        log("%-16s machine %d START sent (batch %d)", PLANT[s].name(), m + 1, batches[b].id);
    }

    static Status parseStatus(String json) {
        try {
            JsonNode j = JSON.readTree(json);
            return new Status(
                j.path("type").asText(""),
                j.path("machine_type").asText(""),
                j.path("machine_id").asInt(0),
                j.path("batch_id").asText(""),
                j.path("error_code").asText(""),
                j.path("error_reason").asText(""));
        } catch (Exception e) {
            return null;
        }
    }

    static int stepOf(String type) {
        for (int s = 0; s < NUM_STEPS; s++) if (PLANT[s].name().equals(type)) return s;
        return -1;
    }

    static void onStatus(Status msg) {
        String t = msg.type();
        if (!t.equals("IDLE") && !t.equals("RUNNING") && !t.equals("ERROR")) return;

        int s = stepOf(msg.machineType());
        int m = msg.machineId() - 1;
        if (s < 0 || m < 0 || m >= PLANT[s].count()) return;

        int b = current[s][m];
        if (b < 0) return;

        String expect = "batch" + batches[b].id;
        if (!msg.batchId().isEmpty() && !msg.batchId().equals(expect)) return;

        int hour = simHour();

        switch (t) {
            case "RUNNING" -> {
                if (st[b][s] != StepState.SENT) return;
                st[b][s] = StepState.RUNNING;
                log("%-16s machine %d running (batch %d)", PLANT[s].name(), m + 1, batches[b].id);
                recordStart(s, m, batches[b].id, hour);
                if (s == TANK_STEP) {
                    log("batch %d is in tank %d", batches[b].id, m + 1);
                    recordTank(batches[b].id, m);
                }
            }
            case "IDLE" -> {
                if (st[b][s] != StepState.RUNNING) return;
                st[b][s] = StepState.DONE;
                current[s][m] = -1;
                log("%-16s machine %d done (batch %d)", PLANT[s].name(), m + 1, batches[b].id);
                recordDone(s);
                if (s == TANK_STEP) {
                    tankOwner[m] = -1;
                    log("tank %d is free", m + 1);
                }
            }
            case "ERROR" -> {
                st[b][s] = StepState.FAILED;
                failed[b] = true;
                current[s][m] = -1;
                releaseTanksOf(b);
                err("ERROR %s machine %d (batch %d): %s %s", PLANT[s].name(), m + 1,
                    batches[b].id, msg.errorCode(), msg.errorReason());
                recordError(s, m, batches[b].id, msg.errorCode(), msg.errorReason());
            }
            default -> {}
        }
    }

    static void pollKafka(KafkaConsumer<String, String> cons) {
        for (ConsumerRecord<String, String> r : cons.poll(Duration.ofMillis(POLL_TIMEOUT_MS))) {
            Status s = parseStatus(r.value());
            if (s != null) onStatus(s);
        }
    }

    static boolean allDone() {
        for (int b = 0; b < n; b++)
            if (!failed[b] && st[b][NUM_STEPS - 1] != StepState.DONE) return false;
        return true;
    }

    // ---- main ------------------------------------------------------------
    public static void main(String[] args) {
        n = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_BATCHES;
        if (n < 1 || n > MAX_BATCHES) {
            System.err.println("batches must be 1.." + MAX_BATCHES);
            System.exit(1);
        }
        if (TANK_STEP < 1 || TANK_STEP >= NUM_STEPS) {
            System.err.println("TANK_STEP " + TANK_STEP + " is out of range");
            System.exit(1);
        }

        redis = new Jedis(REDIS_HOST, REDIS_PORT);
        try { redis.ping(); }
        catch (Exception e) {
            System.err.println("Cannot connect to Redis: " + e.getMessage());
            System.exit(1);
        }
        redisReset();

        for (int s = 0; s < NUM_STEPS; s++)
            if (PLANT[s].count() > MAX_MACHINES) {
                System.err.println("too many machines in step " + (s + 1));
                System.exit(1);
            }

        batches = new Batch[n];
        st = new StepState[n][NUM_STEPS];
        failed = new boolean[n];
        for (StepState[] row : st) Arrays.fill(row, StepState.PENDING);
        for (int i = 0; i < n; i++) {
            batches[i] = new Batch();
            batches[i].id = i + 1;
            batches[i].release = i * RELEASE_INTERVAL;
        }
        Arrays.sort(batches, Comparator.<Batch>comparingInt(x -> x.release).thenComparingInt(x -> x.id));

        // ---- greedy planning ----
        int[][] freeAt = new int[NUM_STEPS][MAX_MACHINES];
        int prevInTank = 0;

        for (int b = 0; b < n; b++) {
            int ready = batches[b].release;
            if (b > 0 && prevInTank > ready) ready = prevInTank;

            int tank = 0;
            for (int m = 1; m < PLANT[TANK_STEP].count(); m++)
                if (freeAt[TANK_STEP][m] < freeAt[TANK_STEP][tank]) tank = m;
            if (freeAt[TANK_STEP][tank] > ready) ready = freeAt[TANK_STEP][tank];

            for (int s = 0; s < NUM_STEPS; s++) {
                int bestM = 0, bestStart = -1;
                if (s == TANK_STEP) {
                    bestM = tank;
                    bestStart = Math.max(freeAt[s][tank], ready);
                } else {
                    for (int m = 0; m < PLANT[s].count(); m++) {
                        int start = Math.max(freeAt[s][m], ready);
                        if (bestStart < 0 || start < bestStart) { bestStart = start; bestM = m; }
                    }
                }
                int finish = bestStart + duration(s, bestM);
                freeAt[s][bestM] = finish;
                batches[b].start[s] = bestStart;
                batches[b].machine[s] = bestM;
                ready = finish;
            }
            batches[b].end = ready;
            batches[b].tank = batches[b].machine[TANK_STEP];
            prevInTank = batches[b].start[TANK_STEP];
        }

        System.out.println("Planned tank assignment (real tanks are picked at run time):");
        for (Batch bt : batches)
            System.out.printf("  batch %2d -> tank %d (planned entry h%d)%n",
                    bt.id, bt.tank + 1, bt.start[TANK_STEP]);
        System.out.println();

        int plannedMakespan = Arrays.stream(batches).mapToInt(x -> x.end).max().orElse(0);

        for (int[] row : current) Arrays.fill(row, -1);
        Arrays.fill(tankOwner, -1);

        // ---- Kafka ----
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(pp);

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        cp.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "5000");   
        KafkaConsumer<String, String> cons = new KafkaConsumer<>(cp);
        cons.subscribe(STATUS_TOPIC_PATTERN);

        hbStart();

        Instant warm = Instant.now();
        while (Duration.between(warm, Instant.now()).toSeconds() < WARMUP_S) {
            pollKafka(cons);
            hbPoll();
        }

        // ---- run the schedule, driven by machine statuses ----
        t0 = Instant.now();
        boolean timedOut = false;

        while (!allDone()) {
            if (Duration.between(t0, Instant.now()).toSeconds() >= RUN_TIMEOUT_S) {
                timedOut = true;
                break;
            }
            for (int b = 0; b < n; b++)
                for (int s = 0; s < NUM_STEPS; s++)
                    if (canStart(b, s)) sendStart(b, s);

            pollKafka(cons);
            hbPoll();
        }

        int nfailed = 0;
        for (boolean f : failed) if (f) nfailed++;

        if (timedOut)
            System.out.printf("%nTIMEOUT after %d s, some machines never finished%n", RUN_TIMEOUT_S);
        System.out.printf("%nPlanned makespan: %d hours, actual: %d hours, failed batches: %d%n",
                plannedMakespan, simHour(), nfailed);

        hbStop();
        cons.close();
        producer.close();
        redis.close();
        System.exit(timedOut || nfailed > 0 ? 1 : 0);
    }
}