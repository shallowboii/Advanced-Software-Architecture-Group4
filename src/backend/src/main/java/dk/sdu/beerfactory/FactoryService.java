package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.stereotype.Service;

import static dk.sdu.beerfactory.Store.*;

/** Database transactions own material location; machine snapshots supply execution evidence. */
@Service
public class FactoryService {
    private final Store store;
    private final Json json;
    private final LiveSocket live;
    public FactoryService(Store store,Json json,LiveSocket live) { this.store=store;this.json=json;this.live=live; }
    public boolean healthy(){try{return store.count("SELECT count(*) FROM factory_state WHERE id=1")==1;}catch(Exception unavailable){return false;}}
    public static class Conflict extends RuntimeException { public Conflict(String message) { super(message); } }
    public static class Missing extends RuntimeException { public Missing(String message) { super(message); } }
    private Map<String,Object> require(Map<String,Object> value,String name) { if(value==null) throw new Missing(name+" not found");return value; }
    public <T> T locked(Supplier<T> action) { return store.transaction(()->{store.lock();return action.get();}); }
    public JsonNode mutate(String scope,String key,JsonNode request,Supplier<Object> action) {
        String fingerprint;
        try { fingerprint=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(json.write(request).getBytes(StandardCharsets.UTF_8))); }
        catch(Exception e) { throw new IllegalStateException(e); }
        String hash=fingerprint;
        return locked(()-> {
            if(key!=null) {
                if(key.isBlank()||key.length()>160) throw new IllegalArgumentException("invalid Idempotency-Key");
                Map<String,Object> previous=store.one("SELECT fingerprint,response FROM api_request WHERE scope=? AND request_key=?",scope,key);
                if(previous!=null) {
                    if(!hash.equals(s(previous,"fingerprint"))) throw new Conflict("Idempotency-Key already used for another payload");
                    return j(previous,"response");
                }
            }
            JsonNode result=json.tree(action.get());
            if(key!=null) store.db.update("INSERT INTO api_request(scope,request_key,fingerprint,response) VALUES(?,?,?,?::jsonb)",scope,key,hash,json.write(result));
            return result;
        });
    }
    public List<JsonNode> recipes() {
        return store.rows("SELECT DISTINCT ON(recipe_id) definition,recipe_id,version FROM recipe_version ORDER BY recipe_id,version DESC").stream().map(row->{ObjectNode value=(ObjectNode)j(row,"definition").deepCopy();value.put("recipe_version_id",s(row,"recipe_id")+":"+i(row,"version"));return (JsonNode)value;}).toList();
    }
    public Object publishRecipe(JsonNode input,String pathId) {
        if(!input.isObject()) throw new IllegalArgumentException("recipe must be an object");
        ObjectNode recipe=(ObjectNode)input.deepCopy();
        String recipeId=pathId==null?Json.required(recipe,"id"):pathId;
        if(!recipeId.matches("[a-zA-Z0-9_-]{1,80}")) throw new IllegalArgumentException("invalid recipe id");
        long latest=store.count("SELECT COALESCE(max(version),0) FROM recipe_version WHERE recipe_id=?",recipeId);
        if(pathId==null && latest>0) throw new Conflict("recipe already exists");
        if(pathId!=null && latest==0) throw new Missing("recipe not found");
        recipe.put("id",recipeId);recipe.put("version",latest+1);recipe.remove("recipe_version_id");Recipes.validate(recipe);
        store.db.update("INSERT INTO recipe_version(recipe_id,version,definition) VALUES(?,?,?::jsonb)",recipeId,latest+1,json.write(recipe));
        record("RECIPE_PUBLISHED",null,null,null,Map.of("recipe_id",recipeId,"version",latest+1));
        return recipe;
    }
    public Object order(JsonNode input,String operator) {
        String recipeId=Json.required(input,"recipe_id");
        if(store.count("SELECT count(*) FROM recipe_version WHERE recipe_id=?",recipeId)==0) throw new Missing("recipe not found");
        if(!input.path("quantity").isIntegralNumber() || !input.path("quantity").canConvertToInt()) throw new IllegalArgumentException("quantity must be an integer");
        int quantity=input.path("quantity").asInt();if(quantity<1||quantity>10000)throw new IllegalArgumentException("quantity must be between 1 and 10000");
        if(input.has("priority")&&!input.path("priority").isIntegralNumber())throw new IllegalArgumentException("priority must be integer");
        int priority=input.path("priority").asInt(0);Integer version=null;
        if(input.hasNonNull("version")) {
            if(!input.path("version").isIntegralNumber())throw new IllegalArgumentException("version must be integer");
            version=input.path("version").asInt();
            if(store.count("SELECT count(*) FROM recipe_version WHERE recipe_id=? AND version=?",recipeId,version)==0)throw new Missing("recipe version not found");
        }
        String orderId=id();
        store.db.update("INSERT INTO production_order(id,recipe_id,pinned_version,quantity,priority,operator_label) VALUES(?,?,?,?,?,?)",orderId,recipeId,version,quantity,priority,operator);
        record("ORDER_CREATED",null,null,null,Map.of("order_id",orderId,"quantity",quantity,"operator",operator));
        return orders().stream().filter(o->orderId.equals(s(o,"id"))).findFirst().orElseThrow();
    }
    public List<Map<String,Object>> orders() {
        return store.rows("SELECT o.*, (SELECT count(*) FROM batch b WHERE b.order_id=o.id AND b.status<>'RESERVING') AS started, (SELECT count(*) FROM batch b WHERE b.order_id=o.id AND b.status='COMPLETED') AS completed, (SELECT count(*) FROM batch b WHERE b.order_id=o.id AND b.status='DISCARDED') AS discarded FROM production_order o ORDER BY o.created_at,o.id");
    }
    public Object cancelOrder(String orderId,String operator) {
        Map<String,Object> order=require(store.one("SELECT * FROM production_order WHERE id=?",orderId),"order");
        store.db.update("UPDATE production_order SET status='CANCELLED' WHERE id=?",orderId);
        record("ORDER_CANCELLED",null,null,null,Map.of("order_id",orderId,"operator",operator));
        return Map.of("id",orderId,"status","CANCELLED","active_batches_continue",true,"unstarted_cancelled",i(order,"quantity")-i(order,"issued")+store.count("SELECT count(*) FROM batch WHERE order_id=? AND status='RESERVING'",orderId));
    }
    public List<Map<String,Object>> batches(String runId) {
        return runId==null?store.rows("SELECT * FROM batch ORDER BY created_at,id"):store.rows("SELECT * FROM batch WHERE run_id=? ORDER BY created_at,id",runId);
    }
    public List<JsonNode> machines() {
        List<JsonNode> result=new ArrayList<>();
        for(Map<String,Object> row:store.rows("SELECT *,last_seen>now()-interval '7 seconds' AS online FROM machine ORDER BY type,id")) {
            ObjectNode snap=(ObjectNode)j(row,"snapshot").deepCopy();
            snap.put("machine_id",s(row,"id"));snap.put("type",s(row,"type"));snap.put("online",b(row,"online"));snap.put("last_seen",s(row,"last_seen"));
            if(row.get("assigned_batch")!=null)snap.put("assigned_batch",s(row,"assigned_batch"));
            result.add(snap);
        }
        return result;
    }
    public Object line() {
        Map<String,Object> state=store.state();
        state.put("active_wip",activeWip());
        Set<String> awaitingAck=new HashSet<>();
        for(Map<String,Object> command:store.rows("SELECT DISTINCT machine_id FROM command WHERE cmd='STOP' AND payload->>'scope'='global' AND acknowledged=false"))awaitingAck.add(s(command,"machine_id"));
        state.put("machines_pending_stop",machines().stream().filter(m->!m.path("online").asBoolean()||!m.path("global_stop").asBoolean()||awaitingAck.contains(m.path("machine_id").asText())).map(m->m.path("machine_id").asText()).toList());
        if(!b(state,"stopped")) state.put("machines_pending_stop",List.of());
        return state;
    }
    public int activeWip() { return (int)store.count("SELECT count(*) FROM batch WHERE status NOT IN ('COMPLETED','DISCARDED')"); }
    public Object stopLine(boolean stop,String operator) {
        store.db.update("UPDATE factory_state SET stopped=?,control_version=control_version+1 WHERE id=1",stop);
        if(stop) supersedeUnsafeCommands();
        else store.db.update("UPDATE command SET acknowledged=true WHERE cmd='STOP' AND payload->>'scope'='global' AND acknowledged=false");
        for(Map<String,Object> machine:store.rows("SELECT id,type FROM machine")) command(s(machine,"id"),stop?"STOP":"RESUME",Map.of("scope","global"));
        record(stop?"LINE_STOPPED":"LINE_RESUMED",null,null,null,Map.of("operator",operator));
        return line();
    }
    private void supersedeUnsafeCommands() {
        // Inhibition is separate from operations: retries resume after the latch is explicitly cleared.
        store.db.update("UPDATE command SET acknowledged=true WHERE cmd='RESUME' AND acknowledged=false");
    }
    public Object machineControl(String machineId,String operation,JsonNode input,String operator) {
        Map<String,Object> machine=require(store.one("SELECT * FROM machine WHERE id=?",machineId),"machine");
        if(operation.equals("flush")) {
            String batchId=Json.required(input,"batch_id");
            JsonNode snap=j(machine,"snapshot");
            if(!batchId.equals(s(machine,"assigned_batch"))&&!batchId.equals(snap.path("batch_id").asText()))throw new Conflict("machine does not hold the requested batch");
            return flushBatch(batchId,"operator",operator);
        }
        if(operation.equals("stop")) store.db.update("UPDATE command SET acknowledged=true WHERE machine_id=? AND cmd='RESUME' AND acknowledged=false",machineId);
        if(operation.equals("resume"))store.db.update("UPDATE command SET acknowledged=true WHERE machine_id=? AND cmd='STOP' AND payload->>'scope'='local' AND acknowledged=false",machineId);
        command(machineId,operation.equals("stop")?"STOP":"RESUME",Map.of("scope","local"));
        record(operation.equals("stop")?"LOCAL_STOP_REQUESTED":"LOCAL_RESUME_REQUESTED",s(machine,"assigned_batch"),machineId,null,Map.of("operator",operator));
        return Map.of("machine_id",machineId,"requested",operation);
    }
    public Object flushBatch(String batchId,String reason,String operator) {
        Map<String,Object> batch=require(store.one("SELECT * FROM batch WHERE id=?",batchId),"batch");
        if(Domain.terminal(s(batch,"status"))) return batch;
        if(s(batch,"status").equals("RESERVING"))throw new Conflict("reservation is pending; cancel the order or wait for admission");
        markLoss(batch,reason);
        record("FLUSH_REQUESTED",batchId,s(batch,"machine_id"),s(batch,"run_id"),Map.of("reason",reason,"operator",operator));
        abortOrFlush(batch);
        return store.one("SELECT * FROM batch WHERE id=?",batchId);
    }
    private void markLoss(Map<String,Object> batch,String reason) {
        if(Domain.terminal(s(batch,"status")))return;
        if(batch.get("loss_reason")==null)record("LOSS_RECORDED",s(batch,"id"),s(batch,"machine_id"),s(batch,"run_id"),Map.of("reason",reason,"volume_hl",50));
        store.db.update("UPDATE batch SET loss_reason=COALESCE(loss_reason,?),status='FLUSH_PENDING' WHERE id=? AND status NOT IN ('COMPLETED','DISCARDED')",reason,s(batch,"id"));
        batch.put("loss_reason",batch.get("loss_reason")==null?reason:batch.get("loss_reason"));batch.put("status","FLUSH_PENDING");
    }
    private void abortOrFlush(Map<String,Object> batch) {
        Map<String,Object> transfer=store.one("SELECT * FROM transfer WHERE batch_id=? AND status NOT IN ('DONE','ABORTED')",s(batch,"id"));
        if(transfer==null) { command(s(batch,"machine_id"),"FLUSH",Map.of("batch_id",s(batch,"id"),"reason",s(batch,"loss_reason")));return; }
        String state=s(transfer,"status");
        if(state.equals("COMMITTED"))return;
        if(!state.equals("ABORTING")) {
            store.db.update("UPDATE transfer SET status='ABORTING',source_done=false,target_done=false WHERE id=?",s(transfer,"id"));
            store.db.update("UPDATE command SET acknowledged=true WHERE payload->>'transfer_id'=? AND cmd IN ('PREPARE_TRANSFER','EXECUTE_TRANSFER')",s(transfer,"id"));
            command(s(transfer,"source_id"),"ABORT_TRANSFER",Map.of("transfer_id",s(transfer,"id")));
            command(s(transfer,"target_id"),"ABORT_TRANSFER",Map.of("transfer_id",s(transfer,"id")));
        }
    }
    public void heartbeat(JsonNode snapshot) {
        locked(()->{saveSnapshot(snapshot);return null;});
    }
    private void saveSnapshot(JsonNode snapshot) {
        String machineId=Json.required(snapshot,"machine_id"),type=Json.required(snapshot,"type");
        if(!Domain.STAGES.contains(type)||!machineId.matches("[a-z]+-[0-9]+")||!machineId.startsWith(type+"-"))throw new IllegalArgumentException("invalid machine identity");
        long version=snapshot.path("state_version").asLong(0);
        store.db.update("INSERT INTO machine(id,type,snapshot,state_version) VALUES(?,?,?::jsonb,?) ON CONFLICT(id) DO UPDATE SET snapshot=CASE WHEN EXCLUDED.state_version>=machine.state_version THEN EXCLUDED.snapshot ELSE machine.snapshot END,state_version=GREATEST(machine.state_version,EXCLUDED.state_version),last_seen=now()",machineId,type,json.write(snapshot),version);
    }
    public void machineEvent(JsonNode incoming) {
        String eventId=Json.required(incoming,"event_id");
        locked(()-> {
            if(store.db.update("INSERT INTO event_inbox(event_id) VALUES(?) ON CONFLICT DO NOTHING",eventId)==0)return null;
            JsonNode snapshot=incoming.path("snapshot");
            if(snapshot.isObject())saveSnapshot(snapshot);
            String machineId=Json.required(incoming,"machine_id"),event=Json.required(incoming,"event");
            String commandId=Json.str(incoming,"command_id");
            Map<String,Object> acknowledged=commandId==null?null:store.one("SELECT * FROM command WHERE id=? AND machine_id=?",commandId,machineId);
            if(commandId!=null)store.db.update("UPDATE command SET acknowledged=true WHERE id=? AND machine_id=?",commandId,machineId);
            if(event.equals("STOPPED") && incoming.path("scope").asText().equals("global") && acknowledged==null && (commandId==null||!commandId.startsWith("backend-inhibit-")) && !b(store.state(),"stopped"))stopLine(true,"physical:"+machineId);
            String batchId=Json.str(incoming,"batch_id");
            Map<String,Object> batch=batchId==null?null:store.one("SELECT * FROM batch WHERE id=?",batchId);
            String run=batch==null?null:s(batch,"run_id");
            boolean owner=batch!=null&&Domain.validOwner(s(batch,"machine_id"),s(batch,"execution_id"),machineId,Json.str(incoming,"execution_id"));
            boolean transferParticipant=false;
            String incomingTransfer=Json.str(incoming,"transfer_id");
            if(batch!=null&&incomingTransfer!=null){
                Map<String,Object> pending=store.one("SELECT * FROM transfer WHERE id=? AND batch_id=? AND status NOT IN ('DONE','ABORTED')",incomingTransfer,batchId);
                if(pending!=null)transferParticipant=Domain.validOwner(s(pending,"source_id"),s(pending,"source_execution_id"),machineId,Json.str(incoming,"execution_id"))||Domain.validOwner(s(pending,"target_id"),s(pending,"next_execution_id"),machineId,Json.str(incoming,"execution_id"));
            }
            boolean lossEvent=Set.of("SPOILED","FLUSH_REQUESTED").contains(event);
            boolean materialEvent=Set.of("STARTED","READY_TO_TRANSFER","COMPLETED","FLUSHED","SPOILED","FLUSH_REQUESTED").contains(event);
            boolean accepted=!materialEvent||(batch!=null&&!Domain.terminal(s(batch,"status"))&&(owner||(lossEvent&&transferParticipant)));
            if(event.equals("COMPLETED")&&accepted)accepted=Domain.canComplete(i(batch,"stage"),s(batch,"status"),batch.get("loss_reason")!=null);
            ObjectNode canonical=(ObjectNode)incoming.deepCopy();canonical.put("accepted",accepted);
            if(!accepted)canonical.put("original_event",event);
            recordWithId(eventId,accepted?event:"IGNORED_MACHINE_EVENT",batchId,machineId,run,canonical);
            if(event.equals("REJECTED")) {
                store.db.update("INSERT INTO alarm(machine_id,reason,details) VALUES(?,?,?::jsonb)",machineId,"COMMAND_REJECTED",json.write(incoming));
                if(acknowledged!=null && incoming.path("reason").asText().equals("stale_coordinator_epoch")) {
                    Map<String,Object> retry=new LinkedHashMap<>();j(acknowledged,"payload").fields().forEachRemaining(field->retry.put(field.getKey(),field.getValue()));
                    command(machineId,s(acknowledged,"cmd"),retry);
                    return null;
                }
                if(acknowledged!=null && Set.of("PREPARE_TRANSFER","EXECUTE_TRANSFER").contains(s(acknowledged,"cmd"))) {
                    String transferId=Json.str(j(acknowledged,"payload"),"transfer_id");
                    Map<String,Object> pending=store.one("SELECT b.* FROM batch b JOIN transfer t ON t.batch_id=b.id WHERE t.id=? AND t.status IN ('PREPARING','EXECUTING')",transferId);
                    if(pending!=null)abortOrFlush(pending);
                }
                return null;
            }
            if(event.startsWith("TRANSFER_")) { transferEvent(incoming,batch);return null; }
            if(batch==null||Domain.terminal(s(batch,"status")))return null;
            if(!accepted)return null;
            if(lossEvent&&transferParticipant){markLoss(batch,event.equals("SPOILED")?"spoilage":"operator");abortOrFlush(batch);return null;}
            if(!owner)return null;
            switch(event) {
                case "STARTED" -> { if(s(batch,"status").equals("ADMITTED"))store.db.update("UPDATE batch SET status='PROCESSING' WHERE id=?",batchId); }
                case "READY_TO_TRANSFER" -> {
                    if(batch.get("loss_reason")==null && !s(batch,"status").equals("RESERVING"))store.db.update("UPDATE batch SET status='WAITING_TRANSFER' WHERE id=?",batchId);
                }
                case "SPOILED", "FLUSH_REQUESTED" -> {markLoss(batch,event.equals("SPOILED")?"spoilage":"operator");abortOrFlush(batch);}
                case "FLUSHED" -> {
                    if(batch.get("loss_reason")==null)markLoss(batch,"operator");
                    finishBatch(batch,"DISCARDED");
                }
                case "COMPLETED" -> {if(Domain.canComplete(i(batch,"stage"),s(batch,"status"),batch.get("loss_reason")!=null))finishBatch(batch,"COMPLETED");}
                default -> {}
            }
            return null;
        });
    }
    private void finishBatch(Map<String,Object> batch,String outcome) {
        store.db.update("UPDATE batch SET status=?,completed_at=now(),completed_sim_h=? WHERE id=? AND status NOT IN ('COMPLETED','DISCARDED')",outcome,store.simTime(),s(batch,"id"));
        store.db.update("UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE id=? AND assigned_batch=?",s(batch,"machine_id"),s(batch,"id"));
        store.db.update("UPDATE command SET acknowledged=true WHERE payload->>'batch_id'=?",s(batch,"id"));
        String orderId=s(batch,"order_id");
        if(store.count("SELECT count(*) FROM batch WHERE order_id=? AND status NOT IN ('COMPLETED','DISCARDED')",orderId)==0)
            store.db.update("UPDATE production_order SET status='FINISHED' WHERE id=? AND issued=quantity AND status<>'CANCELLED'",orderId);
    }
    private void transferEvent(JsonNode event,Map<String,Object> batch) {
        String transferId=Json.str(event,"transfer_id");if(transferId==null)return;
        Map<String,Object> transfer=store.one("SELECT * FROM transfer WHERE id=?",transferId);if(transfer==null)return;
        if(batch==null)batch=store.one("SELECT * FROM batch WHERE id=?",s(transfer,"batch_id"));
        if(batch==null)return;
        String machine=Json.required(event,"machine_id");
        String role=machine.equals(s(transfer,"source_id"))?"source":machine.equals(s(transfer,"target_id"))?"target":null;
        if(role==null)return;
        String kind=Json.required(event,"event"),state=s(transfer,"status");
        if(kind.equals("TRANSFER_PREPARED")&&state.equals("PREPARING")) {
            store.db.update("UPDATE transfer SET "+role+"_prepared=true WHERE id=?",transferId);
            transfer=store.one("SELECT * FROM transfer WHERE id=?",transferId);
            if(b(transfer,"source_prepared")&&b(transfer,"target_prepared")) {
                store.db.update("UPDATE transfer SET status='EXECUTING' WHERE id=?",transferId);
                Map<String,Object> payload=Map.of("transfer_id",transferId,"duration_h",j(batch,"recipe").path("transfer_h").asDouble());
                command(s(transfer,"source_id"),"EXECUTE_TRANSFER",payload);command(s(transfer,"target_id"),"EXECUTE_TRANSFER",payload);
            }
        } else if(kind.equals("TRANSFER_READY")&&state.equals("EXECUTING")) {
            store.db.update("UPDATE transfer SET "+role+"_ready=true WHERE id=?",transferId);
            transfer=store.one("SELECT * FROM transfer WHERE id=?",transferId);
            if(Domain.canCommitTransfer(b(transfer,"source_ready"),b(transfer,"target_ready"),state,batch.get("loss_reason")!=null)) {
                store.db.update("UPDATE transfer SET status='COMMITTED' WHERE id=?",transferId);
                store.db.update("UPDATE batch SET machine_id=?,stage=stage+1,execution_id=?,status='TRANSFERRING' WHERE id=?",s(transfer,"target_id"),s(transfer,"next_execution_id"),s(batch,"id"));
                store.db.update("UPDATE machine SET assigned_role='owner' WHERE id=?",s(transfer,"target_id"));
                command(s(transfer,"source_id"),"COMMIT_TRANSFER",Map.of("transfer_id",transferId));
            }
        } else if(kind.equals("TRANSFER_COMMITTED")&&state.equals("COMMITTED")) {
            if(b(transfer,role+"_done"))return;
            store.db.update("UPDATE transfer SET "+role+"_done=true WHERE id=?",transferId);
            transfer=store.one("SELECT * FROM transfer WHERE id=?",transferId);
            String discardReason=Json.str(event,"discard_reason");
            if(discardReason!=null&&!discardReason.isBlank())markLoss(batch,Set.of("hold_timeout","interruption_limit","spoilage").contains(discardReason)?"spoilage":"operator");
            if(role.equals("source")) {
                store.db.update("UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE id=? AND assigned_batch=?",machine,s(batch,"id"));
                Map<String,Object> commit=new LinkedHashMap<>();commit.put("transfer_id",transferId);
                if(batch.get("loss_reason")!=null)commit.put("discard_reason",s(batch,"loss_reason"));
                command(s(transfer,"target_id"),"COMMIT_TRANSFER",commit);
            }
            if(b(transfer,"source_done")&&b(transfer,"target_done")) {
                store.db.update("UPDATE transfer SET status='DONE' WHERE id=?",transferId);
                store.db.update("UPDATE batch SET status='PROCESSING' WHERE id=? AND status='TRANSFERRING'",s(batch,"id"));
                if(batch.get("loss_reason")!=null)command(s(batch,"machine_id"),"FLUSH",Map.of("batch_id",s(batch,"id"),"reason",s(batch,"loss_reason")));
            }
        } else if(kind.equals("TRANSFER_ABORTED")&&state.equals("ABORTING")) {
            store.db.update("UPDATE transfer SET "+role+"_done=true WHERE id=?",transferId);
            transfer=store.one("SELECT * FROM transfer WHERE id=?",transferId);
            if(b(transfer,"source_done")&&b(transfer,"target_done")) {
                store.db.update("UPDATE transfer SET status='ABORTED' WHERE id=?",transferId);
                store.db.update("UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE id=? AND assigned_batch=?",s(transfer,"target_id"),s(batch,"id"));
                if(batch.get("loss_reason")!=null)command(s(transfer,"source_id"),"FLUSH",Map.of("batch_id",s(batch,"id"),"reason",s(batch,"loss_reason")));
                else store.db.update("UPDATE batch SET status='WAITING_TRANSFER' WHERE id=?",s(batch,"id"));
            }
        }
    }
    public String command(String machine,String cmd,Map<String,?> payload) {
        ObjectNode body=json.object();body.setAll((ObjectNode)json.tree(payload));String commandId=id();
        body.put("schema_version",1);body.put("message_id",commandId);body.put("command_id",commandId);body.put("machine_id",machine);body.put("cmd",cmd);
        body.put("ts_utc",Instant.now().toString());body.put("sim_time_h",store.simTime());body.put("epoch",((Number)store.state().get("epoch")).longValue());
        store.db.update("INSERT INTO command(id,machine_id,cmd,payload) VALUES(?,?,?,?::jsonb)",commandId,machine,cmd,json.write(body));
        return commandId;
    }
    public void record(String event,String batch,String machine,String run,Object details) {recordWithId(id(),event,batch,machine,run,json.tree(details));}
    private void recordWithId(String eventId,String event,String batch,String machine,String run,JsonNode details) {
        Map<String,Object> state=store.state();String actualRun=run==null?s(state,"run_id"):run;
        ObjectNode body=details.isObject()?(ObjectNode)details.deepCopy():json.object();
        body.put("schema_version",1);body.put("event_id",eventId);body.put("event",event);body.put("batch_id",batch);body.put("machine_id",machine);body.put("run_id",actualRun);body.put("ts_utc",Instant.now().toString());
        if(!body.path("sim_time_h").isNumber())body.put("sim_time_h",((Number)state.get("sim_time_h")).doubleValue());
        Map<String,Object> inserted=store.one("INSERT INTO production_event(event_id,event,batch_id,machine_id,run_id,sim_time_h,payload) VALUES(?,?,?,?,?,?,?::jsonb) ON CONFLICT DO NOTHING RETURNING recorded_seq",eventId,event,batch,machine,actualRun,body.path("sim_time_h").asDouble(),json.write(body));
        if(inserted!=null) {
            body.put("recorded_seq",((Number)inserted.get("recorded_seq")).longValue());
            store.db.update("UPDATE production_event SET payload=?::jsonb WHERE event_id=?",json.write(body),eventId);
            store.db.update("INSERT INTO outbox(topic,message_key,payload) VALUES('batch-events',?,?::jsonb)",batch==null?actualRun:batch,json.write(body));
        }
    }
    public Map<String,Object> beginAdmission(long epoch) {
        return locked(()-> {
            Map<String,Object> state=store.state();
            if(((Number)state.get("epoch")).longValue()!=epoch||b(state,"stopped")||b(state,"experiment_done"))return null;
            Map<String,Object> pending=store.one("SELECT b.* FROM batch b JOIN production_order o ON o.id=b.order_id WHERE b.status='RESERVING' AND o.status<>'CANCELLED' ORDER BY b.created_at LIMIT 1");if(pending!=null)return pending;
            if(!Domain.canAdmit(s(state,"policy"),activeWip(),i(state,"wip_limit"),false,true,true))return null;
            Map<String,Object> order=store.one("SELECT * FROM production_order WHERE issued<quantity AND status<>'CANCELLED' ORDER BY priority DESC,created_at,id LIMIT 1");if(order==null)return null;
            Map<String,Object> machine=freeMachine("mash");if(machine==null)return null;
            Map<String,Object> version=order.get("pinned_version")==null?store.one("SELECT * FROM recipe_version WHERE recipe_id=? ORDER BY version DESC LIMIT 1",s(order,"recipe_id")):store.one("SELECT * FROM recipe_version WHERE recipe_id=? AND version=?",s(order,"recipe_id"),order.get("pinned_version"));
            if(version==null)throw new Conflict("order recipe version unavailable");
            String batchId=id(),execution=id(),versionId=s(version,"recipe_id")+":"+i(version,"version");
            store.db.update("INSERT INTO batch(id,order_id,recipe_id,recipe_version_id,recipe,status,machine_id,execution_id,run_id,admitted_sim_h) VALUES(?,?,?,?,?::jsonb,'RESERVING',?,?,?,?)",batchId,s(order,"id"),s(order,"recipe_id"),versionId,json.write(j(version,"definition")),s(machine,"id"),execution,s(state,"run_id"),state.get("sim_time_h"));
            store.db.update("UPDATE machine SET assigned_batch=?,assigned_role='owner' WHERE id=?",batchId,s(machine,"id"));
            store.db.update("UPDATE production_order SET issued=issued+1,status='RUNNING' WHERE id=?",s(order,"id"));
            return store.one("SELECT * FROM batch WHERE id=?",batchId);
        });
    }
    private Map<String,Object> freeMachine(String type) {
        return store.one("SELECT * FROM machine WHERE type=? AND assigned_batch IS NULL AND snapshot->>'state'='EMPTY' AND COALESCE((snapshot->>'local_stop')::boolean,false)=false AND COALESCE((snapshot->>'global_stop')::boolean,false)=false AND last_seen>now()-interval '7 seconds' ORDER BY id LIMIT 1",type);
    }
    public List<Map<String,Object>> cancelledReservations() {
        return store.rows("SELECT b.* FROM batch b JOIN production_order o ON o.id=b.order_id WHERE b.status='RESERVING' AND o.status='CANCELLED'");
    }
    public void reservationReleased(String batchId,long epoch) {
        locked(()-> {
            if(((Number)store.state().get("epoch")).longValue()!=epoch)return null;
            Map<String,Object> batch=store.one("SELECT b.* FROM batch b JOIN production_order o ON o.id=b.order_id WHERE b.id=? AND b.status='RESERVING' AND o.status='CANCELLED'",batchId);
            if(batch==null)return null;
            store.db.update("UPDATE machine SET assigned_batch=NULL,assigned_role=NULL WHERE assigned_batch=?",batchId);
            store.db.update("DELETE FROM batch WHERE id=?",batchId);
            store.db.update("UPDATE production_order SET issued=issued-1 WHERE id=?",s(batch,"order_id"));
            record("ADMISSION_CANCELLED",batchId,s(batch,"machine_id"),s(batch,"run_id"),Map.of("reason","order_cancelled"));
            return null;
        });
    }
    public void inventoryReserved(String batchId,long epoch) {
        locked(()-> {
            Map<String,Object> batch=store.one("SELECT * FROM batch WHERE id=?",batchId);if(batch==null||!s(batch,"status").equals("RESERVING"))return null;
            if(((Number)store.state().get("epoch")).longValue()!=epoch||b(store.state(),"stopped"))return null;
            if(store.count("SELECT count(*) FROM production_order WHERE id=? AND status='CANCELLED'",s(batch,"order_id"))>0)return null;
            store.db.update("UPDATE batch SET status='ADMITTED',admitted_sim_h=? WHERE id=?",store.simTime(),batchId);
            command(s(batch,"machine_id"),"START",stagePayload(batch,0));
            record("ADMITTED",batchId,s(batch,"machine_id"),s(batch,"run_id"),Map.of("volume_hl",50,"order_id",s(batch,"order_id"),"recipe_version_id",s(batch,"recipe_version_id")));
            return null;
        });
    }
    public void inventoryBlocked(String batchId,String reason) {
        locked(()-> {
            if(store.count("SELECT count(*) FROM production_event WHERE batch_id=? AND event='BLOCKED_INVENTORY'",batchId)==0)
                record("BLOCKED_INVENTORY",batchId,null,null,Map.of("reason",reason));
            return null;
        });
    }
    private Map<String,Object> stagePayload(Map<String,Object> batch,int stage) {
        JsonNode recipe=j(batch,"recipe"),definition=recipe.path("stages").get(stage);
        Map<String,Object> data=new LinkedHashMap<>();
        data.put("batch_id",s(batch,"id"));data.put("execution_id",s(batch,"execution_id"));data.put("recipe_version_id",s(batch,"recipe_version_id"));data.put("run_id",s(batch,"run_id"));data.put("phases",definition.path("phases"));data.put("hold_h",definition.path("hold_h"));data.put("max_pause_h",definition.path("max_pause_h"));data.put("flush_h",recipe.path("flush_h").asDouble(.1));
        return data;
    }
    public void scheduleTransfers(long epoch) {
        locked(()-> {
            Map<String,Object> state=store.state();if(((Number)state.get("epoch")).longValue()!=epoch||b(state,"stopped"))return null;
            List<Map<String,Object>> waiting=store.rows("SELECT b.* FROM batch b JOIN machine m ON m.id=b.machine_id WHERE b.status='WAITING_TRANSFER' AND b.loss_reason IS NULL AND b.stage<5 AND m.last_seen>now()-interval '7 seconds' AND COALESCE((m.snapshot->>'local_stop')::boolean,false)=false ORDER BY COALESCE((m.snapshot->>'hold_deadline_h')::double precision,1e99),b.created_at,b.id");
            for(Map<String,Object> batch:waiting) {
                if(store.count("SELECT count(*) FROM transfer WHERE batch_id=? AND status NOT IN ('DONE','ABORTED')",s(batch,"id"))>0)continue;
                Map<String,Object> target=freeMachine(Domain.STAGES.get(i(batch,"stage")+1));if(target==null)continue;
                String transferId=id(),execution=id();
                store.db.update("INSERT INTO transfer(id,batch_id,source_id,target_id,source_execution_id,next_execution_id) VALUES(?,?,?,?,?,?)",transferId,s(batch,"id"),s(batch,"machine_id"),s(target,"id"),s(batch,"execution_id"),execution);
                store.db.update("UPDATE machine SET assigned_batch=?,assigned_role='receiver' WHERE id=?",s(batch,"id"),s(target,"id"));
                store.db.update("UPDATE batch SET status='TRANSFERRING' WHERE id=?",s(batch,"id"));
                Map<String,Object> source=stagePayload(batch,i(batch,"stage"));source.put("transfer_id",transferId);source.put("role","source");source.put("peer_id",s(target,"id"));
                Map<String,Object> dest=stagePayload(batch,i(batch,"stage")+1);dest.put("execution_id",execution);dest.put("transfer_id",transferId);dest.put("role","target");dest.put("peer_id",s(batch,"machine_id"));
                command(s(batch,"machine_id"),"PREPARE_TRANSFER",source);command(s(target,"id"),"PREPARE_TRANSFER",dest);
                record("TRANSFER_REQUESTED",s(batch,"id"),s(batch,"machine_id"),s(batch,"run_id"),Map.of("transfer_id",transferId,"target_id",s(target,"id")));
            }
            return null;
        });
    }
    public void reconcile(long epoch) {
        locked(()-> {
            if(((Number)store.state().get("epoch")).longValue()!=epoch)return null;
            for(Map<String,Object> row:store.rows("SELECT * FROM command WHERE acknowledged=false AND last_sent<now()-interval '30 seconds'")) {
                if(i(row,"attempts")%30==0)store.db.update("INSERT INTO alarm(machine_id,reason,details) VALUES(?,'COMMAND_UNCONFIRMED',?::jsonb)",s(row,"machine_id"),json.write(row));
            }
            for(Map<String,Object> batch:store.rows("SELECT b.* FROM batch b JOIN machine m ON b.machine_id=m.id WHERE b.status='ADMITTED' AND m.snapshot->>'state'='EMPTY' AND COALESCE((m.snapshot->>'local_stop')::boolean,false)=false AND NOT EXISTS(SELECT 1 FROM command c WHERE c.payload->>'batch_id'=b.id AND c.cmd='START' AND c.acknowledged=false)"))
                command(s(batch,"machine_id"),"START",stagePayload(batch,0));
            // The durable decision is replayed by command IDs. Snapshots never roll ownership back.
            for(Map<String,Object> machine:store.rows("SELECT * FROM machine WHERE assigned_batch IS NULL AND snapshot->>'state'<>'EMPTY' AND snapshot->>'batch_id' IS NOT NULL")) {
                String batchId=j(machine,"snapshot").path("batch_id").asText();
                if(store.count("SELECT count(*) FROM batch WHERE id=?",batchId)==0 && store.count("SELECT count(*) FROM command WHERE machine_id=? AND cmd='STOP' AND acknowledged=false",s(machine,"id"))==0)
                    command(s(machine,"id"),"STOP",Map.of("scope","local"));
            }
            return null;
        });
    }
    public Object startExperiment(JsonNode input) {
        if(activeWip()!=0)throw new Conflict("factory must be empty before starting an experiment");
        if(store.count("SELECT count(*) FROM production_order WHERE issued<quantity AND status NOT IN ('CANCELLED','FINISHED')")>0)throw new Conflict("cancel pending orders before a new experiment");
        String run=Json.required(input,"run_id"),policy=Json.required(input,"policy");
        if(input.has("wip_limit")&&(!input.path("wip_limit").isIntegralNumber()||!input.path("wip_limit").canConvertToInt()))throw new IllegalArgumentException("wip_limit must be an integer");
        int cap=input.path("wip_limit").asInt(20);Domain.canAdmit(policy,0,cap,false,true,true);
        double horizon=Json.number(input,"horizon_h");Domain.positiveFinite(horizon,"horizon_h");
        if(store.count("SELECT count(*) FROM production_event WHERE run_id=?",run)>0)throw new Conflict("run_id already has events; use an isolated factory or a new run_id");
        store.db.update("UPDATE factory_state SET run_id=?,policy=?,wip_limit=?,horizon_h=?,experiment_start_h=EXTRACT(EPOCH FROM(clock_timestamp()-sim_origin))/time_scale,experiment_done=false WHERE id=1",run,policy,cap,horizon);
        record("EXPERIMENT_STARTED",null,null,run,input);
        return experimentStatus();
    }
    public Object stopExperiment() {
        store.db.update("UPDATE factory_state SET experiment_done=true WHERE id=1");
        record("EXPERIMENT_STOPPED",null,null,null,Map.of());
        return experimentStatus();
    }
    public void checkHorizon(long epoch) {
        locked(()-> {
            Map<String,Object> state=store.state();if(((Number)state.get("epoch")).longValue()!=epoch)return null;
            if(!b(state,"experiment_done")&&state.get("horizon_h")!=null && ((Number)state.get("sim_time_h")).doubleValue()-((Number)state.get("experiment_start_h")).doubleValue()>=((Number)state.get("horizon_h")).doubleValue()) stopExperiment();
            return null;
        });
    }
    public Map<String,Object> experimentStatus() {
        Map<String,Object> state=store.state();String run=s(state,"run_id");
        Map<String,Object> result=new LinkedHashMap<>();result.put("run_id",run);result.put("policy",state.get("policy"));result.put("wip_limit",state.get("wip_limit"));
        result.put("started",store.count("SELECT count(*) FROM batch WHERE run_id=? AND status<>'RESERVING'",run));
        long completed=store.count("SELECT count(*) FROM batch WHERE run_id=? AND status='COMPLETED'",run);
        long spoiled=store.count("SELECT count(*) FROM batch WHERE run_id=? AND loss_reason='spoilage'",run);
        long manual=store.count("SELECT count(*) FROM batch WHERE run_id=? AND loss_reason IS NOT NULL AND loss_reason<>'spoilage'",run);
        result.put("completed",completed);result.put("spoiled",spoiled);result.put("operator_discarded",manual);result.put("hl_out",completed*50);result.put("hl_wasted",(spoiled+manual)*50);
        result.put("active_wip",activeWip());result.put("sim_clock_h",state.get("sim_time_h"));result.put("elapsed_h",((Number)state.get("sim_time_h")).doubleValue()-((Number)state.get("experiment_start_h")).doubleValue());result.put("start_sim_h",state.get("experiment_start_h"));result.put("done",state.get("experiment_done"));return result;
    }
    public List<JsonNode> events(Map<String,String> filters,boolean telemetry) {
        StringBuilder sql=new StringBuilder("SELECT "+(telemetry?"payload":"payload || jsonb_build_object('recorded_seq',recorded_seq) AS payload")+" FROM "+(telemetry?"telemetry":"production_event")+" WHERE 1=1");List<Object> args=new ArrayList<>();
        for(String field:List.of("run_id","machine_id","batch_id"))if(filters.containsKey(field)){sql.append(" AND ").append(field).append("=?");args.add(filters.get(field));}
        if(telemetry&&filters.containsKey("sensor")){sql.append(" AND sensor=?");args.add(filters.get("sensor"));}
        if(filters.containsKey("from")){sql.append(" AND ts_utc>=?::timestamptz");args.add(Instant.parse(filters.get("from")).toString());}
        if(filters.containsKey("to")){sql.append(" AND ts_utc<=?::timestamptz");args.add(Instant.parse(filters.get("to")).toString());}
        int limit=Integer.parseInt(filters.getOrDefault("limit","1000")),offset=Integer.parseInt(filters.getOrDefault("offset","0"));
        if(limit<1||limit>10000||offset<0)throw new IllegalArgumentException("limit 1..10000, offset >=0 required");
        if(!telemetry&&filters.containsKey("after")){sql.append(" AND recorded_seq>?");args.add(Long.parseLong(filters.get("after")));}
        String order=filters.getOrDefault("order","asc");
        if(!Set.of("asc","desc").contains(order))throw new IllegalArgumentException("order must be asc or desc");
        String direction=order.equals("desc")?" DESC":" ASC";
        sql.append(telemetry?" ORDER BY ts_utc"+direction+",event_id"+direction:" ORDER BY recorded_seq"+direction);
        sql.append(" LIMIT ? OFFSET ?");args.add(limit);args.add(offset);
        return store.rows(sql.toString(),args.toArray()).stream().map(row->j(row,"payload")).toList();
    }
    public void telemetry(JsonNode sample) {
        String machine=Json.required(sample,"machine_id"),sensor=Json.required(sample,"sensor");double value=Json.number(sample,"value");
        String eventId=sample.path("event_id").asText(sample.path("message_id").asText(null));
        if(eventId==null||eventId.isBlank())throw new IllegalArgumentException("telemetry event/message id required");
        String timestamp=Json.required(sample,"ts_utc");Instant.parse(timestamp);
        store.db.update("INSERT INTO telemetry(event_id,machine_id,batch_id,run_id,recipe_version_id,phase,sensor,value,unit,sim_time_h,ts_utc,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?::timestamptz,?::jsonb) ON CONFLICT DO NOTHING",eventId,machine,Json.str(sample,"batch_id"),Json.str(sample,"run_id"),Json.str(sample,"recipe_version_id"),Json.str(sample,"phase"),sensor,value,Json.str(sample,"unit"),sample.path("sim_time_h").asDouble(),timestamp,json.write(sample));
        live.publish("telemetry",sample);
    }
}
