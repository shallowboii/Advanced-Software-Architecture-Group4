package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class Store {
    public final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final Json json;
    public Store(JdbcTemplate db, TransactionTemplate tx, Json json) { this.db=db; this.tx=tx; this.json=json; }
    public <T> T transaction(Supplier<T> action) { return tx.execute(s -> action.get()); }
    public void lock() { db.queryForObject("SELECT id FROM factory_state WHERE id=1 FOR UPDATE", Integer.class); }
    public Map<String,Object> one(String sql,Object...args) {
        List<Map<String,Object>> list=rows(sql,args);
        return list.isEmpty()?null:list.getFirst();
    }
    public List<Map<String,Object>> rows(String sql,Object...args) {
        return db.query(sql,(rs,row)-> {
            Map<String,Object> result=new LinkedHashMap<>();
            for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
                Object value=rs.getObject(i);
                if(value!=null && value.getClass().getName().equals("org.postgresql.util.PGobject")) value=json.read(value.toString());
                else if(value instanceof Timestamp stamp) value=stamp.toInstant().toString();
                result.put(rs.getMetaData().getColumnLabel(i),value);
            }
            return result;
        },args);
    }
    public long count(String sql,Object...args) { Long n=db.queryForObject(sql,Long.class,args); return n==null?0:n; }
    public Map<String,Object> state() { return one("SELECT *, EXTRACT(EPOCH FROM (clock_timestamp()-sim_origin))/time_scale AS sim_time_h FROM factory_state WHERE id=1"); }
    public double simTime() { return ((Number)state().get("sim_time_h")).doubleValue(); }
    public static String id() { return UUID.randomUUID().toString(); }
    public static String s(Map<String,Object> row,String key) { Object value=row.get(key); return value==null?null:value.toString(); }
    public static int i(Map<String,Object> row,String key) { return ((Number)row.get(key)).intValue(); }
    public static boolean b(Map<String,Object> row,String key) { return Boolean.TRUE.equals(row.get(key)); }
    public static JsonNode j(Map<String,Object> row,String key) { return (JsonNode)row.get(key); }
}
