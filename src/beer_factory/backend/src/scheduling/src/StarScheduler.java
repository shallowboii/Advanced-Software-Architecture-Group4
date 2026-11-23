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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;

/**
 * Star scheduler 
 *
 *
 * Run:  mvn -q compile exec:java -Dexec.mainClass=StarScheduler -Dexec.args="3"
 */
public class StarScheduler {

    // ---- configuration ---------------------------------------------------
    static final int MAX_BATCHES = 64;
    static final int DEFAULT_BATCHES = 6;
    static final int TANK_STEP = 4;          // maturation
    static final int WHIRLPOOL_STEP = 3;     // produces clarified wort
    static final int FREE_TANK_MIN = 4;      // start_mashing guard: free_tank >= 4

    static final String BROKERS = env("KAFKA_BROKERS", "localhost:9092");
    static final String CONSUMER_GROUP = "brewery-scheduler-" + java.util.UUID.randomUUID();
    static final int SECONDS_PER_SIM_HOUR = Integer.parseInt(env("SECONDS_PER_SIM_HOUR", "1"));
    static final int WARMUP_S = 5;
    static final int RUN_TIMEOUT_S = Integer.parseInt(env("RUN_TIMEOUT_S", "3600"));
    static final long POLL_TIMEOUT_MS = 100;

    static final String MQTT_URI = env("MQTT_URI", "tcp://localhost:1883");
    static final String MQTT_CLIENT_ID = "brewery-scheduler-hb";
    static final String HEARTBEAT_TOPIC = "heartbeat";
    static final int HEARTBEAT_TIMEOUT_S = 30;
    static final int MAX_HEARTBEATS = 64;

    static final String REDIS_HOST = env("REDIS_HOST", "localhost");
    static final int REDIS_PORT = Integer.parseInt(env("REDIS_PORT", "6379"));

    static final String TOPIC_PREFIX = env("TOPIC_PREFIX", "asaat67.");
    static final Pattern STATUS_TOPIC_PATTERN =
        Pattern.compile("^" + Pattern.quote(TOPIC_PREFIX) + "machine\\..+");

    static String topic(int s) { return TOPIC_PREFIX + "machine." + PLANT[s].name(); }

    static KafkaProducer<String, byte[]> producer;

    // ---- plant definition  ----
    record Step(String name, int count, int durationHours, String output) {}

    static final Step[] PLANT = {
        new Step("mashing",      1, 2, "mash_batch"),
        new Step("lautering",    1, 2, "sweet_wort_batch"),
        new Step("boiling",      1, 2, "boiled_wort_batch"),
        new Step("whirlpooling", 1, 1, "clarified_wort_batch"),
        new Step("fermentation_maturation", 5, 336, "green_beer_batch"),     // TANK_STEP, needs >= FREE_TANK_MIN
        new Step("filtering",    1, 2, "filtered_beer_batch"),
    };
    static final int NUM_STEPS = PLANT.length;
    static final int MAX_MACHINES = Arrays.stream(PLANT).mapToInt(Step::count).max().orElse(1);

    /** Same role as PLANT[s]->duration(machine), in sim hours. */
    static int durationHours(int step, int machine) {
        return PLANT[step].durationHours();
    }

    // ---- types -----------------------------------------------------------
    enum StepState { PENDING, SENT, RUNNING, DONE, FAILED }

    static class Batch {
        int id;
        int tank = -1;    // tank for this batch, -1 until chosen
    }

    record Status(String type, String machineType, int machineId, String batchId,
                  String errorCode, String errorReason) {}

    static class HbEntry {
        String name;
        Instant lastSeen;
        boolean silent;
    }

    // ---- run-time state --------------------------------------------------
    static final ObjectMapper JSON = new ObjectMapper();

    static final Batch[] batches = new Batch[MAX_BATCHES];
    static int n;                 // batches wanted
    static int started;           // batches released so far
    static final StepState[][] st = new StepState[MAX_BATCHES][NUM_STEPS];
    static final boolean[] failed = new boolean[MAX_BATCHES];
    static final int[][] current = new int[NUM_STEPS][MAX_MACHINES];   // batch index or -1

    // Variables of the UPPAAL model
    static boolean isMashing;
    static final int[] tanksStatus = new int[MAX_MACHINES];            // 0 = free, 1 = in use
    static int clarifiedWortBatches;                                   // waiting for a tank
    static int chosenTank;
    static final long[] tankEnd = new long[MAX_MACHINES];              

    static Jedis redis;
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
            o.setUserName(env("MQTT_USERNAME", "admin"));
            o.setPassword(env("MQTT_PASSWORD", "").toCharArray());
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

    // ---- model helper functions ------------------------------------------
    static double durationS(int s, int m) {
        return (double) durationHours(s, m) * SECONDS_PER_SIM_HOUR;
    }

    static int freeTank() {
        int free = 0;
        for (int t = 0; t < PLANT[TANK_STEP].count(); t++)
            if (tanksStatus[t] == 0) free++;
        return free;
    }

    static double leadTimeS() {
        double sum = 0;
        for (int s = 0; s < TANK_STEP; s++) sum += durationS(s, 0);
        return sum;
    }

    static final int TANK_TIME_LEFT_LIMIT_HOURS = 6;

    static boolean anyAlmostDone() {
        long now = Instant.now().getEpochSecond();
        for (int t = 0; t < PLANT[TANK_STEP].count(); t++) {
            int b = current[TANK_STEP][t];
            if (b < 0 || st[b][TANK_STEP] != StepState.RUNNING) continue;
            if (tankEnd[t] - now <= (long) TANK_TIME_LEFT_LIMIT_HOURS * SECONDS_PER_SIM_HOUR)
                return true;
        }
        return false;
    }

    static int idleMachine(int s) {
        for (int m = 0; m < PLANT[s].count(); m++)
            if (current[s][m] < 0) return m;
        return -1;
    }

    static int selectFreeTank() {
        for (int t = 0; t < PLANT[TANK_STEP].count(); t++)
            if (tanksStatus[t] == 0 && current[TANK_STEP][t] < 0) return t;
        return -1;
    }

    // ---- commands --------------------------------------------------------
    static void sendStart(int b, int s, int m) {
        byte[] payload = Wire.encodeStart(PLANT[s].name(), m + 1, "batch" + batches[b].id,
            "batch" + batches[b].id + "-step" + (s + 1) + "-start",
            "duration_s", durationS(s, m));
        producer.send(new ProducerRecord<>(topic(s),
            String.valueOf(m + 1), payload));

        st[b][s] = StepState.SENT;
        current[s][m] = b;
        log("%-16s machine %d START sent (batch %d)", PLANT[s].name(), m + 1, batches[b].id);
    }

    static void edgeStartMashing() {
        int m = idleMachine(0);
        if (m < 0) return;

        int b = started++;
        batches[b] = new Batch();
        batches[b].id = b + 1;
        isMashing = true;
        sendStart(b, 0, m);
    }

    static boolean canMash() {
        return started < n && !isMashing && (freeTank() >= FREE_TANK_MIN || anyAlmostDone());
    }

    static boolean inLocation(int b, int s) {
        return !failed[b] && st[b][s - 1] == StepState.DONE && st[b][s] == StepState.PENDING;
    }

    static void edgeStart(int b, int s) {
        int m;
        if (s == TANK_STEP) {
            if (clarifiedWortBatches < 1) return;
            m = selectFreeTank();
            if (m < 0) return;
            chosenTank = m;
            tanksStatus[m] = 1;
            clarifiedWortBatches--;
            batches[b].tank = m;
            log("batch %d goes to tank %d", batches[b].id, chosenTank + 1);
        } else {
            m = idleMachine(s);
            if (m < 0) return;
        }
        sendStart(b, s, m);
    }

    static void fireEdges() {
        // committed locations first, oldest batch first
        for (int b = 0; b < started; b++)
            for (int s = 1; s < NUM_STEPS; s++)
                if (inLocation(b, s)) edgeStart(b, s);

        if (canMash()) edgeStartMashing();
    }

    // ---- status handling -------------------------------------------------

    static Status parseStatus(byte[] payload) {
        Wire.Msg w = Wire.decode(payload);
        if (w == null) return null;
        return new Status(w.type(), w.machineType(), w.machineId(), w.batchId(),
                          w.errorCode(), w.errorReason());
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
                    tankEnd[m] = Instant.now().getEpochSecond() + (long) durationS(s, m);
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
                if (s == 0) isMashing = false;
                if (s == WHIRLPOOL_STEP) clarifiedWortBatches++;
                if (s == TANK_STEP) {
                    tanksStatus[m] = 0;
                    log("tank %d is free", m + 1);
                }
                if (s == NUM_STEPS - 1) log("batch %d is finished", batches[b].id);
            }
            case "ERROR" -> {
                st[b][s] = StepState.FAILED;
                failed[b] = true;
                current[s][m] = -1;
                if (s == 0) isMashing = false;
                if (s == TANK_STEP) tanksStatus[m] = 0;   // failed batch leaves its tank empty
                err("ERROR %s machine %d (batch %d): %s %s", PLANT[s].name(), m + 1,
                    batches[b].id, msg.errorCode(), msg.errorReason());
                recordError(s, m, batches[b].id, msg.errorCode(), msg.errorReason());
            }
            default -> {}
        }
    }

    static void pollKafka(KafkaConsumer<String, byte[]> cons) {
        for (ConsumerRecord<String, byte[]> r : cons.poll(Duration.ofMillis(POLL_TIMEOUT_MS))) {
            Status s = parseStatus(r.value());
            if (s != null) onStatus(s);
        }
    }

    static boolean allDone() {
        if (started < n) return false;
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
        if (NUM_STEPS != 6 || TANK_STEP != 4 || WHIRLPOOL_STEP != 3) {
            System.err.println("the UPPAAL model has 6 steps (tank step 4)");
            System.exit(1);
        }

        String redisPw = env("REDIS_PASSWORD", "");
        var redisCfg = DefaultJedisClientConfig.builder()
                .ssl(Boolean.parseBoolean(env("REDIS_TLS", "false")));
        if (!redisPw.isEmpty()) redisCfg.user(env("REDIS_USERNAME", "default")).password(redisPw);
        redis = new Jedis(new HostAndPort(REDIS_HOST, REDIS_PORT), redisCfg.build());
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

        if (PLANT[TANK_STEP].count() < FREE_TANK_MIN) {
            System.err.printf("plant has %d tanks, start_mashing needs free_tank >= %d%n",
                    PLANT[TANK_STEP].count(), FREE_TANK_MIN);
            System.exit(1);
        }

        for (StepState[] row : st) Arrays.fill(row, StepState.PENDING);
        for (int[] row : current) Arrays.fill(row, -1);

        // ---- Kafka ----
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(pp);

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        cp.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "5000");   // notice new machine.* topics
        KafkaConsumer<String, byte[]> cons = new KafkaConsumer<>(cp);
        cons.subscribe(STATUS_TOPIC_PATTERN);

        hbStart();

        Instant warm = Instant.now();
        while (Duration.between(warm, Instant.now()).toSeconds() < WARMUP_S) {
            pollKafka(cons);
            hbPoll();
        }

        t0 = Instant.now();
        boolean timedOut = false;

        while (!allDone()) {
            if (Duration.between(t0, Instant.now()).toSeconds() >= RUN_TIMEOUT_S) {
                timedOut = true;
                break;
            }
            fireEdges();
            pollKafka(cons);
            hbPoll();
        }

        int nfailed = 0;
        for (int b = 0; b < started; b++) if (failed[b]) nfailed++;

        if (timedOut)
            System.out.printf("%nTIMEOUT after %d s, some machines never finished%n", RUN_TIMEOUT_S);
        System.out.printf("%nActual: %d hours, batches started: %d, failed batches: %d%n",
                simHour(), started, nfailed);

        hbStop();
        cons.close();
        producer.close();
        redis.close();
        System.exit(timedOut || nfailed > 0 ? 1 : 0);
    }
}