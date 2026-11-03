package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * MQTT transport adapter for technical commands, observations and application acknowledgements.
 * Production rules stay in central services.
 */
@Component
public class MqttGateway implements MqttCallbackExtended {
  private static final Logger log = LoggerFactory.getLogger(MqttGateway.class);
  private final Json json;
  private final FactoryService factory;
  private final Bootstrap bootstrap;
  private MqttAsyncClient client;
  private volatile boolean connecting;

  /**
   * Wires the collaborators for this mqtt gateway; no production transition occurs during
   * construction.
   */
  public MqttGateway(Json json, FactoryService factory, Bootstrap bootstrap) {
    this.json = json;
    this.factory = factory;
    this.bootstrap = bootstrap;
  }

  @Scheduled(fixedDelay = 2000)
  /**
   * Starts or retries the MQTT connection without blocking the production scheduler on broker
   * availability.
   */
  public synchronized void connect() {
    if (!bootstrap.ready || connecting || (client != null && client.isConnected())) return;
    try {
      if (client == null) {
        client =
            new MqttAsyncClient(
                Bootstrap.env("MQTT_URL", "tcp://localhost:1883"),
                "backend-" + Bootstrap.env("HOSTNAME", Store.id()),
                new MemoryPersistence());
        client.setCallback(this);
      }
      MqttConnectOptions options = new MqttConnectOptions();
      options.setCleanSession(false);
      options.setAutomaticReconnect(false);
      options.setUserName(Bootstrap.env("MQTT_USERNAME", "beerfactory"));
      options.setPassword(Bootstrap.env("MQTT_PASSWORD", "dev-only-password").toCharArray());
      options.setKeepAliveInterval(10);
      options.setConnectionTimeout(2);
      options.setMaxInflight(1000);
      connecting = true;
      client.connect(
          options,
          null,
          new IMqttActionListener() {
            /** Clears the connection-attempt flag after Paho accepts the connection. */
            public void onSuccess(IMqttToken token) {
              connecting = false;
            }

            /** Allows a later connection attempt and records the transport failure. */
            public void onFailure(IMqttToken token, Throwable error) {
              connecting = false;
              log.warn("MQTT connection failed: {}", error.getClass().getSimpleName());
            }
          });
    } catch (Exception e) {
      connecting = false;
      log.warn("MQTT unavailable: {}", e.getClass().getSimpleName());
    }
  }

  @Override
  /** Subscribes to equipment heartbeats and critical events after a connection is established. */
  public void connectComplete(boolean reconnect, String serverURI) {
    try {
      client.subscribe(
          new String[] {"factory/+/+/heartbeat", "factory/+/+/events"}, new int[] {0, 1});
    } catch (Exception e) {
      log.warn("MQTT subscribe failed", e);
    }
  }

  @Override
  /**
   * Marks transport connection state unavailable so subsequent scheduling cannot assume a working
   * broker.
   */
  public void connectionLost(Throwable cause) {
    connecting = false;
  }

  @Override
  /**
   * Ignores broker delivery as execution evidence; only application events acknowledge actuator
   * commands.
   */
  public void deliveryComplete(IMqttDeliveryToken token) {}

  @Override
  /**
   * Validates topic identity, delegates technical observations and acknowledges critical events
   * after central persistence.
   */
  public void messageArrived(String topic, MqttMessage message) throws Exception {
    String[] parts = topic.split("/");
    if (parts.length != 4) return;
    JsonNode body = json.read(new String(message.getPayload(), StandardCharsets.UTF_8));
    if (!parts[2].equals(body.path("machine_id").asText()))
      throw new IllegalArgumentException("MQTT identity mismatch");
    if (parts[3].equals("heartbeat")) {
      factory.heartbeat(body);
    } else {
      factory.machineEvent(body);
      send(
          "factory/" + parts[1] + "/" + parts[2] + "/ack",
          json.write(java.util.Map.of("event_id", Json.required(body, "event_id"))),
          1);
    }
  }

  /**
   * Reports current MQTT transport connectivity without claiming any machine has executed a
   * command.
   */
  public boolean connected() {
    return client != null && client.isConnected();
  }

  /**
   * Publishes a non-retained physical command or application acknowledgement with the requested
   * QoS.
   */
  public void send(String topic, String body, int qos) throws MqttException {
    if (!connected()) throw new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED);
    MqttMessage message = new MqttMessage(body.getBytes(StandardCharsets.UTF_8));
    message.setQos(qos);
    message.setRetained(false);
    client.publish(topic, message);
  }

  @PreDestroy
  /** Disconnects and closes MQTT transport during application shutdown. */
  public void close() {
    if (client == null) return;
    try {
      client.disconnectForcibly(1000, 1000);
    } catch (Exception ignored) {
    }
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }
}
