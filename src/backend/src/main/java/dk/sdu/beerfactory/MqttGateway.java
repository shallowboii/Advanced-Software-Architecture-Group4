package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MqttGateway implements MqttCallbackExtended {
    private static final Logger log=LoggerFactory.getLogger(MqttGateway.class);
    private final Json json;
    private final FactoryService factory;
    private final Bootstrap bootstrap;
    private final StringRedisTemplate redis;
    private MqttAsyncClient client;
    private volatile boolean connecting;
    public MqttGateway(Json json,FactoryService factory,Bootstrap bootstrap,StringRedisTemplate redis) {
        this.json=json;this.factory=factory;this.bootstrap=bootstrap;this.redis=redis;
    }
    @Scheduled(fixedDelay=2000)
    public synchronized void connect() {
        if(!bootstrap.ready||connecting||(client!=null&&client.isConnected()))return;
        try {
            if(client==null) {
                client=new MqttAsyncClient(Bootstrap.env("MQTT_URL","tcp://localhost:1883"),"backend-"+Bootstrap.env("HOSTNAME",Store.id()),new MemoryPersistence());
                client.setCallback(this);
            }
            MqttConnectOptions options=new MqttConnectOptions();options.setCleanSession(false);options.setAutomaticReconnect(false);
            options.setUserName(Bootstrap.env("MQTT_USERNAME","beerfactory"));options.setPassword(Bootstrap.env("MQTT_PASSWORD","dev-only-password").toCharArray());
            options.setKeepAliveInterval(10);options.setConnectionTimeout(2);options.setMaxInflight(1000);connecting=true;
            client.connect(options,null,new IMqttActionListener() {
                public void onSuccess(IMqttToken token){connecting=false;}
                public void onFailure(IMqttToken token,Throwable error){connecting=false;log.warn("MQTT connection failed: {}",error.getClass().getSimpleName());}
            });
        } catch(Exception e) { connecting=false;log.warn("MQTT unavailable: {}",e.getClass().getSimpleName()); }
    }
    @Override public void connectComplete(boolean reconnect,String serverURI) {
        try {client.subscribe(new String[]{"factory/+/+/heartbeat","factory/+/+/events"},new int[]{0,1});}
        catch(Exception e){log.warn("MQTT subscribe failed",e);}
    }
    @Override public void connectionLost(Throwable cause){connecting=false;}
    @Override public void deliveryComplete(IMqttDeliveryToken token){}
    @Override public void messageArrived(String topic,MqttMessage message) throws Exception {
        String[] parts=topic.split("/");if(parts.length!=4)return;
        JsonNode body=json.read(new String(message.getPayload(),StandardCharsets.UTF_8));
        if(!parts[2].equals(body.path("machine_id").asText()))throw new IllegalArgumentException("MQTT identity mismatch");
        if(parts[3].equals("heartbeat")) {
            factory.heartbeat(body);
            try{redis.opsForValue().set("machine:"+parts[2],json.write(body),Duration.ofSeconds(6));}catch(Exception ignored){}
        } else {
            factory.machineEvent(body);
            send("factory/"+parts[1]+"/"+parts[2]+"/ack",json.write(java.util.Map.of("event_id",Json.required(body,"event_id"))),1);
        }
    }
    public boolean connected(){return client!=null&&client.isConnected();}
    public void send(String topic,String body,int qos) throws MqttException {
        if(!connected())throw new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED);
        MqttMessage message=new MqttMessage(body.getBytes(StandardCharsets.UTF_8));message.setQos(qos);message.setRetained(false);
        client.publish(topic,message);
    }
    @PreDestroy public void close() {
        if(client==null)return;
        try{client.disconnectForcibly(1000,1000);}catch(Exception ignored){}
        try{client.close();}catch(Exception ignored){}
    }
}
