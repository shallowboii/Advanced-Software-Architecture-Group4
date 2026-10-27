package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import static dk.sdu.beerfactory.Store.*;

@Component
public class Coordinator {
    private static final Logger log=LoggerFactory.getLogger(Coordinator.class);
    private final Store store;private final FactoryService factory;private final Leadership leader;private final MqttGateway mqtt;
    private final KafkaGateway kafka;private final InventoryClient inventory;private final Json json;private final Bootstrap bootstrap;private final LiveSocket live;
    private final AtomicBoolean dispatching=new AtomicBoolean();
    public Coordinator(Store store,FactoryService factory,Leadership leader,MqttGateway mqtt,KafkaGateway kafka,InventoryClient inventory,Json json,Bootstrap bootstrap,LiveSocket live){this.store=store;this.factory=factory;this.leader=leader;this.mqtt=mqtt;this.kafka=kafka;this.inventory=inventory;this.json=json;this.bootstrap=bootstrap;this.live=live;}
    private boolean current(long epoch){return bootstrap.ready&&epoch>0&&leader.epoch()==epoch&&((Number)store.state().get("epoch")).longValue()==epoch;}
    @Scheduled(fixedDelay=300)
    public void permits(){
        long epoch=leader.epoch();if(!bootstrap.ready||epoch==0||!mqtt.connected())return;
        try{
            if(!current(epoch))return;Map<String,Object> state=store.state();
            if(b(state,"stopped")){
                String inhibit="backend-inhibit-"+epoch+"-"+state.get("control_version");
                mqtt.send("factory/line/control",json.write(Map.of("schema_version",1,"command_id",inhibit,"message_id",inhibit,"cmd","STOP","scope","global","epoch",epoch)),1);
            }
            for(Map<String,Object> machine:store.rows("SELECT id,type FROM machine WHERE last_seen>now()-interval '7 seconds'")){
                ObjectNode permit=json.object();String id=id();permit.put("schema_version",1);permit.put("message_id",id);permit.put("command_id",id);permit.put("cmd","PERMIT");permit.put("machine_id",s(machine,"id"));permit.put("epoch",epoch);permit.put("permit_ms",3000);permit.put("sim_time_h",((Number)state.get("sim_time_h")).doubleValue());permit.put("time_scale",((Number)state.get("time_scale")).doubleValue());permit.put("ts_utc",Instant.now().toString());
                if(b(state,"stopped")){String inhibit="backend-inhibit-"+epoch+"-"+state.get("control_version");permit.put("message_id",inhibit);permit.put("command_id",inhibit);permit.put("cmd","STOP");permit.put("scope","global");}
                mqtt.send("factory/"+s(machine,"type")+"/"+s(machine,"id")+"/cmd",json.write(permit),b(state,"stopped")?1:0);
            }
        }catch(Exception e){log.debug("Permit renewal unavailable: {}",e.getClass().getSimpleName());}
    }
    @Scheduled(fixedDelay=150)
    public void schedule(){
        long epoch=leader.epoch();if(!bootstrap.ready||epoch==0||!mqtt.connected()||!dispatching.compareAndSet(false,true))return;
        try{
            if(!current(epoch))return;
            for(Map<String,Object> cancelled:factory.cancelledReservations()){
                inventory.call("POST","/reservations/"+s(cancelled,"id")+"/release",json.object());
                factory.reservationReleased(s(cancelled,"id"),epoch);
            }
            factory.checkHorizon(epoch);factory.scheduleTransfers(epoch);
            Map<String,Object> batch=factory.beginAdmission(epoch);if(batch==null)return;
            try{
                inventory.call("POST","/reserve",json.tree(Map.of("batch_id",s(batch,"id"),"ingredients",j(batch,"recipe").path("ingredients"))));
                if(current(epoch))factory.inventoryReserved(s(batch,"id"),epoch);
            }catch(InventoryClient.Rejected e){factory.inventoryBlocked(s(batch,"id"),e.getMessage());}
            catch(InventoryClient.Unavailable e){factory.inventoryBlocked(s(batch,"id"),e.getMessage());}
        }catch(Exception e){log.error("Scheduler cycle failed",e);}
        finally{dispatching.set(false);}
    }
    @Scheduled(fixedDelay=200)
    public void commands(){
        long epoch=leader.epoch();if(!bootstrap.ready||epoch==0||!mqtt.connected())return;
        try{
            if(!current(epoch))return;
            boolean stopped=b(store.state(),"stopped");
            for(Map<String,Object> command:store.rows("SELECT c.*,m.type FROM command c JOIN machine m ON m.id=c.machine_id WHERE c.acknowledged=false AND (c.last_sent IS NULL OR c.last_sent<now()-interval '1 second') ORDER BY CASE WHEN c.cmd='STOP' THEN 0 ELSE 1 END,c.created_at,c.id LIMIT 100")){
                String cmd=s(command,"cmd");if(stopped&&!Set.of("STOP","SNAPSHOT","ABORT_TRANSFER","COMMIT_TRANSFER").contains(cmd))continue;
                if(!current(epoch))return;
                ObjectNode payload=(ObjectNode)j(command,"payload").deepCopy();payload.put("epoch",epoch);
                mqtt.send("factory/"+s(command,"type")+"/"+s(command,"machine_id")+"/cmd",json.write(payload),1);
                store.db.update("UPDATE command SET last_sent=now(),attempts=attempts+1 WHERE id=?",s(command,"id"));
            }
        }catch(Exception e){log.debug("Command publication will retry: {}",e.getClass().getSimpleName());}
    }
    @Scheduled(fixedDelay=250)
    public void events(){
        long epoch=leader.epoch();if(!bootstrap.ready||epoch==0)return;
        try{
            if(!current(epoch))return;
            for(Map<String,Object> row:store.rows("SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 100")){
                if(!current(epoch))return;kafka.publish(s(row,"topic"),s(row,"message_key"),json.write(j(row,"payload")));
                store.db.update("UPDATE outbox SET sent_at=now() WHERE id=?",row.get("id"));
            }
        }catch(Exception e){log.debug("Event publication will retry: {}",e.getClass().getSimpleName());}
    }
    @Scheduled(fixedDelay=2000)
    public void reconcile(){long epoch=leader.epoch();if(!bootstrap.ready||epoch==0)return;try{factory.reconcile(epoch);}catch(Exception e){log.warn("Recovery pending: {}",e.getClass().getSimpleName());}}
    @Scheduled(fixedDelay=1000)
    public void snapshot(){
        if(!bootstrap.ready)return;
        try{live.publish("snapshot",Map.of("machines",factory.machines(),"line",factory.line(),"orders",factory.orders(),"experiment",factory.experimentStatus()));}
        catch(Exception e){log.debug("Snapshot unavailable: {}",e.getClass().getSimpleName());}
    }
}
