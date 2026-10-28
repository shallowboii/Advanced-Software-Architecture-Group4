package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Small JDBC boundary with explicit transactions and row conversion. The factory row lock
 * serialises material decisions across backend replicas.
 */
@Component
public class Store {
  public final JdbcTemplate jdbc;
  private final TransactionTemplate transactions;
  private final Json json;

  /**
   * Wires the collaborators for this store; no production transition occurs during construction.
   */
  public Store(JdbcTemplate jdbc, TransactionTemplate transactions, Json json) {
    this.jdbc = jdbc;
    this.transactions = transactions;
    this.json = json;
  }

  /** Runs a callback in the configured Spring JDBC transaction boundary. */
  public <T> T transaction(Supplier<T> action) {
    return transactions.execute(s -> action.get());
  }

  /**
   * Locks the singleton factory coordination row before changing material ownership or admission
   * state.
   */
  public void lock() {
    jdbc.queryForObject("SELECT id FROM factory_state WHERE id=1 FOR UPDATE", Integer.class);
  }

  /** Returns the first projected row, or null when the query matches no record. */
  public Map<String, Object> one(String sql, Object... args) {
    List<Map<String, Object>> list = rows(sql, args);
    return list.isEmpty() ? null : list.getFirst();
  }

  /**
   * Maps JDBC rows to named values, decoding JSONB and normalising timestamps for API projections.
   */
  public List<Map<String, Object>> rows(String sql, Object... args) {
    return jdbc.query(
        sql,
        (rs, row) -> {
          Map<String, Object> result = new LinkedHashMap<>();
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            Object value = rs.getObject(i);
            if (value != null && value.getClass().getName().equals("org.postgresql.util.PGobject"))
              value = json.read(value.toString());
            else if (value instanceof Timestamp stamp) value = stamp.toInstant().toString();
            result.put(rs.getMetaData().getColumnLabel(i), value);
          }
          return result;
        },
        args);
  }

  /** Reads one numeric query result as a long. */
  public long count(String sql, Object... args) {
    Long n = jdbc.queryForObject(sql, Long.class, args);
    return n == null ? 0 : n;
  }

  /**
   * Reads durable line state together with the authoritative simulated time derived from PostgreSQL
   * wall time.
   */
  public Map<String, Object> state() {
    return one(
        "SELECT *, EXTRACT(EPOCH FROM (clock_timestamp()-sim_origin))/time_scale AS sim_time_h FROM"
            + " factory_state WHERE id=1");
  }

  /** Returns the current authoritative simulation time in hours. */
  public double simTime() {
    return ((Number) state().get("sim_time_h")).doubleValue();
  }

  /** Allocates an opaque UUID for a central record or actual actuator command. */
  public static String id() {
    return UUID.randomUUID().toString();
  }

  /** Reads a nullable textual value from a named JDBC row column. */
  public static String stringValue(Map<String, Object> row, String key) {
    Object value = row.get(key);
    return value == null ? null : value.toString();
  }

  /** Reads a required integer value from a named JDBC row column. */
  public static int intValue(Map<String, Object> row, String key) {
    return ((Number) row.get(key)).intValue();
  }

  /** Reads a boolean JDBC row flag; absent or non-true values are false. */
  public static boolean flag(Map<String, Object> row, String key) {
    return Boolean.TRUE.equals(row.get(key));
  }

  /** Reads an already decoded JSONB projection from a named JDBC row column. */
  public static JsonNode jsonValue(Map<String, Object> row, String key) {
    return (JsonNode) row.get(key);
  }

  /** Runs a central mutation inside the shared factory row lock and one database transaction. */
  public <T> T locked(Supplier<T> action) {
    return transaction(
        () -> {
          lock();
          return action.get();
        });
  }

  /**
   * Counts all centrally unresolved batches, including pending admissions and material awaiting
   * removal.
   */
  public int activeWip() {
    return (int) count("SELECT count(*) FROM batch WHERE status NOT IN ('COMPLETED','DISCARDED')");
  }

  /** Rejects a missing central record with the API not-found exception. */
  public static Map<String, Object> require(Map<String, Object> value, String name) {
    if (value == null) throw new FactoryService.Missing(name + " not found");
    return value;
  }
}
