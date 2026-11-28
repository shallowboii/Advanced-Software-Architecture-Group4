import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake machines for testing the schedulers
 *
 * Start this BEFORE the scheduler:
 *   mvn -q compile exec:java -Dexec.mainClass=MockMachines
 *   mvn -q compile exec:java -Dexec.mainClass=MockMachines -Dexec.args="--fail batch2:boiling"
 *
 */
public class MockMachines {
    static final String BROKERS = System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:9092");
    static final String[] TYPES = {
        "mashing", "lautering", "boiling", "whirlpooling", "fermentation", "filtering"
    };
    static final ObjectMapper JSON = new ObjectMapper();
    static final Set<String> busy = ConcurrentHashMap.newKeySet();
    static KafkaProducer<String, String> producer;
    static Instant t0 = Instant.now();

    static void log(String s) {
        System.out.printf("[mock +%3ds] %s%n", Duration.between(t0, Instant.now()).toSeconds(), s);
    }

    static void status(String type, String machineType, int id, String batch,
                       String code, String reason) {
        ObjectNode m = JSON.createObjectNode();
        m.put("type", type);
        m.put("machine_type", machineType);
        m.put("machine_id", id);
        m.put("batch_id", batch);
        m.put("message_id", batch + "-" + machineType + "-" + type.toLowerCase());
        m.put("ts_utc", Instant.now().toString());
        if (!code.isEmpty()) { m.put("error_code", code); m.put("error_reason", reason); }
        producer.send(new ProducerRecord<>("machine." + machineType,
                machineType + "-" + id, m.toString()));
    }

    public static void main(String[] args) throws Exception {
        Set<String> failAt = new HashSet<>();   // entries like "batch2:boiling"
        for (int i = 0; i < args.length; i++)
            if (args[i].equals("--fail") && i + 1 < args.length) failAt.add(args[++i]);

        // create the topics up front so nothing is missed on first use
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", BROKERS))) {
            List<NewTopic> topics = java.util.Arrays.stream(TYPES)
                    .map(t -> new NewTopic("machine." + t, 1, (short) 1)).toList();
            try { admin.createTopics(topics).all().get(); }
            catch (Exception ignored) { /* some already exist */ }
        }

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(pp);

        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "mock-machines-" + UUID.randomUUID());
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        KafkaConsumer<String, String> cons = new KafkaConsumer<>(cp);
        cons.subscribe(java.util.Arrays.stream(TYPES).map(t -> "machine." + t).toList());
        while (cons.assignment().isEmpty()) cons.poll(Duration.ofMillis(200));
        cons.poll(Duration.ofMillis(200));

        t0 = Instant.now();
        log("ready, waiting for START commands" + (failAt.isEmpty() ? "" : " (failing " + failAt + ")"));

        ScheduledExecutorService timer = Executors.newScheduledThreadPool(4);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { timer.shutdownNow(); producer.close(); }));

        while (true) {
            for (ConsumerRecord<String, String> r : cons.poll(Duration.ofMillis(100))) {
                JsonNode j;
                try { j = JSON.readTree(r.value()); } catch (Exception e) { continue; }
                if (!j.path("type").asText("").equals("START")) continue;

                String type = j.path("machine_type").asText();
                int id = j.path("machine_id").asInt();
                String batch = j.path("batch_id").asText();
                double durS = j.path("params").path("duration_s").asDouble(1);
                String key = type + "-" + id;

                if (!busy.add(key))
                    log("!!! VIOLATION: START for " + batch + " on " + key + " while it is still busy");

                log(String.format("%s %d starts %s (%.0f s)", type, id, batch, durS));
                status("RUNNING", type, id, batch, "", "");

                timer.schedule(() -> {
                    busy.remove(key);
                    if (failAt.contains(batch + ":" + type)) {
                        log(type + " " + id + " FAILS on " + batch);
                        status("ERROR", type, id, batch, "E_MOCK", "injected failure");
                    } else {
                        log(type + " " + id + " finished " + batch);
                        status("IDLE", type, id, batch, "", "");
                    }
                }, (long) (durS * 1000), TimeUnit.MILLISECONDS);
            }
        }
    }
}