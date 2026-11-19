package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Serves append-ordered production history and centrally enriched sensor history for export and the
 * dashboard.
 */
@Service
public class HistoryService {
  private final Store store;
  private final Json json;
  private final LiveSocket live;
  private final ControllerProtocol protocol;

  /**
   * Wires the collaborators for this history service; no production transition occurs during
   * construction.
   */
  public HistoryService(Store store, Json json, LiveSocket live, ControllerProtocol protocol) {
    this.store = store;
    this.json = json;
    this.live = live;
    this.protocol = protocol;
  }

  /**
   * Returns filtered history with explicit ascending export order or descending dashboard order and
   * bounded pagination.
   */
  public List<JsonNode> events(Map<String, String> filters, boolean telemetry) {
    StringBuilder sql =
        new StringBuilder(
            "SELECT "
                + (telemetry
                    ? "payload"
                    : "payload || jsonb_build_object('recorded_seq',recorded_seq) AS payload")
                + " FROM "
                + (telemetry ? "telemetry" : "production_event")
                + " WHERE 1=1");
    List<Object> args = new ArrayList<>();
    for (String field : List.of("run_id", "machine_id", "batch_id"))
      if (filters.containsKey(field)) {
        sql.append(" AND ").append(field).append("=?");
        args.add(filters.get(field));
      }
    if (telemetry && filters.containsKey("sensor")) {
      sql.append(" AND sensor=?");
      args.add(filters.get("sensor"));
    }
    if (filters.containsKey("from")) {
      sql.append(" AND ts_utc>=?::timestamptz");
      args.add(Instant.parse(filters.get("from")).toString());
    }
    if (filters.containsKey("to")) {
      sql.append(" AND ts_utc<=?::timestamptz");
      args.add(Instant.parse(filters.get("to")).toString());
    }
    int limit = Integer.parseInt(filters.getOrDefault("limit", "1000")),
        offset = Integer.parseInt(filters.getOrDefault("offset", "0"));
    if (limit < 1 || limit > 10000 || offset < 0)
      throw new IllegalArgumentException("limit 1..10000, offset >=0 required");
    if (!telemetry && filters.containsKey("after")) {
      sql.append(" AND recorded_seq>?");
      args.add(Long.parseLong(filters.get("after")));
    }
    String order = filters.getOrDefault("order", "asc");
    if (!Set.of("asc", "desc").contains(order))
      throw new IllegalArgumentException("order must be asc or desc");
    String direction = order.equals("desc") ? " DESC" : " ASC";
    sql.append(
        telemetry
            ? " ORDER BY ts_utc" + direction + ",event_id" + direction
            : " ORDER BY recorded_seq" + direction);
    sql.append(" LIMIT ? OFFSET ?");
    args.add(limit);
    args.add(offset);
    return store.rows(sql.toString(), args.toArray()).stream()
        .map(row -> jsonValue(row, "payload"))
        .toList();
  }

  /**
   * Enriches a raw reading through its original actuator command, persists it idempotently and
   * updates the dashboard.
   */
  public void telemetry(JsonNode raw) {
    JsonNode sample = protocol.enrichTelemetry(raw);
    String machine = Json.required(sample, "machine_id"), sensor = Json.required(sample, "sensor");
    double value = Json.number(sample, "value");
    String eventId = sample.path("event_id").asText(sample.path("message_id").asText(null));
    if (eventId == null || eventId.isBlank())
      throw new IllegalArgumentException("telemetry event/message id required");
    String timestamp = Json.required(sample, "ts_utc");
    Instant.parse(timestamp);
    store.jdbc.update(
        "INSERT INTO"
            + " telemetry(event_id,machine_id,batch_id,run_id,recipe_version_id,phase,sensor,value,unit,sim_time_h,ts_utc,payload)"
            + " VALUES(?,?,?,?,?,?,?,?,?,?,?::timestamptz,?::jsonb) ON CONFLICT DO NOTHING",
        eventId,
        machine,
        Json.str(sample, "batch_id"),
        Json.str(sample, "run_id"),
        Json.str(sample, "recipe_version_id"),
        Json.str(sample, "phase"),
        sensor,
        value,
        Json.str(sample, "unit"),
        sample.path("sim_time_h").asDouble(),
        timestamp,
        json.write(sample));
    live.publish("telemetry", sample);
  }
}
