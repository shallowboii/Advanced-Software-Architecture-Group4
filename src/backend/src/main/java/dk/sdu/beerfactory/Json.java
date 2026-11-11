package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/** Centralises JSON encoding and strict scalar validation for API and protocol boundaries. */
@Component
public class Json {
  private final ObjectMapper mapper;

  /** Wires the collaborators for this json; no production transition occurs during construction. */
  public Json(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  /** Parses JSON and exposes malformed input as an explicit validation error. */
  public JsonNode read(String value) {
    try {
      return mapper.readTree(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON", e);
    }
  }

  /** Serialises an API value, private command context or transport envelope as JSON. */
  public String write(Object value) {
    try {
      return mapper.writeValueAsString(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot encode JSON", e);
    }
  }

  /** Creates an empty JSON object for an explicitly constructed envelope. */
  public ObjectNode object() {
    return mapper.createObjectNode();
  }

  /** Converts a Java projection to a JSON tree without coupling callers to ObjectMapper. */
  public JsonNode tree(Object value) {
    return mapper.valueToTree(value);
  }

  /** Reads optional text, returning null for an absent or null field. */
  public static String str(JsonNode node, String name) {
    return node.path(name).isNull() ? null : node.path(name).asText(null);
  }

  /** Requires nonempty bounded text at an API or protocol boundary. */
  public static String required(JsonNode node, String name) {
    JsonNode value = node.path(name);
    if (!value.isTextual() || value.asText().isBlank() || value.asText().length() > 160)
      throw new IllegalArgumentException(name + " must be a nonempty string (max 160)");
    return value.asText();
  }

  /** Requires a numeric finite value; booleans and nonfinite numbers are rejected. */
  public static double number(JsonNode node, String name) {
    if (!node.path(name).isNumber()) throw new IllegalArgumentException(name + " must be a number");
    double value = node.path(name).asDouble();
    if (!Double.isFinite(value)) throw new IllegalArgumentException(name + " must be finite");
    return value;
  }
}
