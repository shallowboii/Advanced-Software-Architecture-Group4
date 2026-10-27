package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import static dk.sdu.beerfactory.Store.*;
import static org.junit.jupiter.api.Assertions.*;

/** Uses a fresh disposable schema in an explicitly supplied PostgreSQL database. */
@EnabledIfEnvironmentVariable(named="TEST_DATABASE_URL",matches=".+")
class FactoryServiceTest {
    private final Json json=RecipesTest.JSON;
    private Store store;
    private FactoryService factory;
    private String schema;
    @BeforeEach void setup(){
        schema="backend_test_"+UUID.randomUUID().toString().replace("-","");
        var ds=new DriverManagerDataSource(System.getenv("TEST_DATABASE_URL"),Bootstrap.env("TEST_DATABASE_USER","beerfactory"),Bootstrap.env("TEST_DATABASE_PASSWORD","dev-only-password"));
        var props=new Properties();props.setProperty("currentSchema",schema);ds.setConnectionProperties(props);
        Flyway.configure().dataSource(ds).schemas(schema).load().migrate();
        store=new Store(new JdbcTemplate(ds),new TransactionTemplate(new DataSourceTransactionManager(ds)),json);
        factory=new FactoryService(store,json,new LiveSocket(json));
        store.db.update("INSERT INTO factory_state(id,epoch,time_scale,policy,wip_limit,run_id) VALUES(1,1,1,'CONWIP',20,'test')");
        store.db.update("INSERT INTO recipe_version(recipe_id,version,definition) VALUES('lager',1,?::jsonb)",json.write(RecipesTest.recipe()));
        for(String type:Domain.STAGES){var snapshot=json.object();snapshot.put("machine_id",type+"-1");snapshot.put("type",type);snapshot.put("state","EMPTY");snapshot.put("state_version",1);snapshot.put("global_stop",false);snapshot.put("local_stop",false);snapshot.putNull("batch_id");factory.heartbeat(snapshot);}
    }
    @AfterEach void teardown(){if(store!=null)store.db.execute("DROP SCHEMA "+schema+" CASCADE");}
    private Map<String,Object> admit(){
        factory.mutate("orders","one-order",json.read("{\"recipe_id\":\"lager\",\"quantity\":1}"),()->factory.order(json.read("{\"recipe_id\":\"lager\",\"quantity\":1}"),"test"));
        var batch=factory.beginAdmission(1);assertNotNull(batch);factory.inventoryReserved(s(batch,"id"),1);
        event("STARTED",s(batch,"machine_id"),batch,null,"PROCESSING",null);
        return batch(s(batch,"id"));
    }
    private Map<String,Object> batch(String id){return store.one("SELECT * FROM batch WHERE id=?",id);}
    private ObjectNode event(String kind,String machine,Map<String,Object> batch,Map<String,Object> transfer,String state,String discard){
        String execution=transfer!=null&&machine.equals(s(transfer,"target_id"))?s(transfer,"next_execution_id"):s(batch,"execution_id");
        var row=store.one("SELECT * FROM machine WHERE id=?",machine);var snapshot=(ObjectNode)j(row,"snapshot").deepCopy();
        snapshot.put("state",state);snapshot.put("state_version",((Number)row.get("state_version")).longValue()+1);
        if(state.equals("EMPTY")){snapshot.putNull("batch_id");snapshot.putNull("execution_id");snapshot.putNull("transfer_id");}
        else{snapshot.put("batch_id",s(batch,"id"));snapshot.put("execution_id",execution);snapshot.put("hold_deadline_h",store.simTime()+10);}
        var event=json.object();event.put("event_id",id());event.put("event",kind);event.put("machine_id",machine);event.put("batch_id",s(batch,"id"));event.put("execution_id",execution);event.put("sim_time_h",store.simTime());event.set("snapshot",snapshot);
        if(transfer!=null)event.put("transfer_id",s(transfer,"id"));if(discard!=null)event.put("discard_reason",discard);
        factory.machineEvent(event);return event;
    }
    private Map<String,Object> prepare(Map<String,Object> batch){
        event("READY_TO_TRANSFER",s(batch,"machine_id"),batch,null,"READY_TO_TRANSFER",null);factory.scheduleTransfers(1);
        var transfer=store.one("SELECT * FROM transfer WHERE batch_id=? AND status='PREPARING'",s(batch,"id"));assertNotNull(transfer);
        event("TRANSFER_PREPARED",s(transfer,"source_id"),batch,transfer,"TRANSFERRING_OUT",null);
        event("TRANSFER_PREPARED",s(transfer,"target_id"),batch,transfer,"RESERVED_IN",null);
        event("TRANSFER_READY",s(transfer,"source_id"),batch,transfer,"TRANSFERRING_OUT",null);
        event("TRANSFER_READY",s(transfer,"target_id"),batch,transfer,"TRANSFERRING_IN",null);
        assertEquals("COMMITTED",s(store.one("SELECT * FROM transfer WHERE id=?",s(transfer,"id")),"status"));
        assertEquals(0,store.count("SELECT count(*) FROM command WHERE machine_id=? AND cmd='COMMIT_TRANSFER' AND payload->>'transfer_id'=?",s(transfer,"target_id"),s(transfer,"id")));
        return transfer;
    }
    @Test void sixStagesReleaseExactlyOnceAndRetainRecipeSnapshot(){
        var batch=admit();String batchId=s(batch,"id");
        var changed=RecipesTest.recipe();changed.put("name","Changed lager");factory.locked(()->factory.publishRecipe(changed,"lager"));
        for(int stage=0;stage<5;stage++){
            var transfer=prepare(batch);
            var sourceAck=event("TRANSFER_COMMITTED",s(transfer,"source_id"),batch,transfer,"EMPTY",null);
            factory.machineEvent(sourceAck);
            assertEquals(1,store.count("SELECT count(*) FROM command WHERE machine_id=? AND cmd='COMMIT_TRANSFER' AND payload->>'transfer_id'=?",s(transfer,"target_id"),s(transfer,"id")));
            batch=batch(batchId);event("TRANSFER_COMMITTED",s(transfer,"target_id"),batch,transfer,"PROCESSING",null);batch=batch(batchId);
            assertEquals(stage+1,i(batch,"stage"));assertEquals("PROCESSING",s(batch,"status"));assertEquals("lager:1",s(batch,"recipe_version_id"));
        }
        var completed=event("COMPLETED","filter-1",batch,null,"EMPTY",null);factory.machineEvent(completed);
        assertEquals("COMPLETED",s(batch(batchId),"status"));assertEquals(0,factory.activeWip());assertEquals(50L,factory.experimentStatus().get("hl_out"));
        assertEquals(1,store.count("SELECT count(*) FROM production_event WHERE event='COMPLETED'"));
        assertEquals(0,store.count("SELECT count(*) FROM machine WHERE assigned_batch IS NOT NULL"));
    }
    @Test void sourceSpoilAfterDecisionCannotStartReceiverOrCountLossTwice(){
        var original=admit();var transfer=prepare(original);String batchId=s(original,"id");
        var ack=event("TRANSFER_COMMITTED",s(transfer,"source_id"),original,transfer,"EMPTY","hold_timeout");factory.machineEvent(ack);
        var commit=store.one("SELECT payload FROM command WHERE machine_id=? AND cmd='COMMIT_TRANSFER'",s(transfer,"target_id"));assertEquals("spoilage",j(commit,"payload").path("discard_reason").asText());
        var current=batch(batchId);assertEquals("FLUSH_PENDING",s(current,"status"));
        event("TRANSFER_COMMITTED",s(transfer,"target_id"),current,transfer,"FLUSH_PENDING","hold_timeout");
        var removal=event("FLUSHED",s(transfer,"target_id"),current,null,"EMPTY",null);factory.machineEvent(removal);
        assertEquals("DISCARDED",s(batch(batchId),"status"));assertEquals(1L,factory.experimentStatus().get("spoiled"));assertEquals(50L,factory.experimentStatus().get("hl_wasted"));assertEquals(0L,factory.experimentStatus().get("completed"));
    }
    @Test void receiverFlushDuringPrepareAbortsBothParticipants(){
        var batch=admit();event("READY_TO_TRANSFER",s(batch,"machine_id"),batch,null,"READY_TO_TRANSFER",null);factory.scheduleTransfers(1);
        var transfer=store.one("SELECT * FROM transfer WHERE batch_id=?",s(batch,"id"));
        event("TRANSFER_PREPARED",s(transfer,"target_id"),batch,transfer,"RESERVED_IN",null);
        event("FLUSH_REQUESTED",s(transfer,"target_id"),batch,transfer,"FLUSH_PENDING",null);
        assertEquals("ABORTING",s(store.one("SELECT * FROM transfer WHERE id=?",s(transfer,"id")),"status"));
        assertEquals(2,store.count("SELECT count(*) FROM command WHERE cmd='ABORT_TRANSFER'"));
        event("TRANSFER_ABORTED",s(transfer,"target_id"),batch,transfer,"EMPTY",null);
        event("TRANSFER_ABORTED",s(transfer,"source_id"),batch,transfer,"FLUSH_PENDING",null);
        assertEquals(1,store.count("SELECT count(*) FROM production_event WHERE event='LOSS_RECORDED'"));
        assertEquals(s(batch,"machine_id"),s(batch(s(batch,"id")),"machine_id"));
        assertNull(store.one("SELECT assigned_batch FROM machine WHERE id=?",s(transfer,"target_id")).get("assigned_batch"));
    }
    @Test void cancelledReservationCannotAdmitAndReleasesAssignment(){
        var body=json.read("{\"recipe_id\":\"lager\",\"quantity\":1}");
        var order=factory.mutate("orders","cancel",body,()->factory.order(body,"test"));
        var pending=factory.beginAdmission(1);factory.locked(()->factory.cancelOrder(order.path("id").asText(),"test"));
        factory.inventoryReserved(s(pending,"id"),1);
        assertEquals("RESERVING",s(batch(s(pending,"id")),"status"));assertEquals(1,factory.cancelledReservations().size());
        factory.reservationReleased(s(pending,"id"),1);assertNull(batch(s(pending,"id")));assertEquals(0,factory.activeWip());
        assertEquals(0,store.count("SELECT count(*) FROM command WHERE cmd='START'"));
    }
    @Test void retryKeysAndGlobalStopAreTransactional(){
        var body=json.read("{\"recipe_id\":\"lager\",\"quantity\":2}");
        var first=factory.mutate("orders","duplicate",body,()->factory.order(body,"test"));
        assertEquals(first,factory.mutate("orders","duplicate",body,()->{throw new AssertionError("reexecuted");}));
        assertThrows(FactoryService.Conflict.class,()->factory.mutate("orders","duplicate",json.object(),()->Map.of()));
        factory.locked(()->factory.stopLine(true,"test"));assertNull(factory.beginAdmission(1));
        factory.locked(()->factory.stopLine(false,"test"));assertNotNull(factory.beginAdmission(1));
        assertEquals(0,store.count("SELECT count(*) FROM command WHERE cmd='STOP' AND acknowledged=false"));
        var pages=factory.events(Map.of("limit","1"),false);assertEquals(1,pages.size());long seq=pages.getFirst().path("recorded_seq").asLong();
        assertTrue(factory.events(Map.of("after",Long.toString(seq)),false).stream().allMatch(e->e.path("recorded_seq").asLong()>seq));
    }
}
