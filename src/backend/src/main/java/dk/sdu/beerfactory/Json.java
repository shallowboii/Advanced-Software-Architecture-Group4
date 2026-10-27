package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

@Component
public class Json {
    private final ObjectMapper mapper;
    public Json(ObjectMapper mapper) { this.mapper = mapper; }
    public JsonNode read(String value) {
        try { return mapper.readTree(value); } catch (Exception e) { throw new IllegalArgumentException("Invalid JSON", e); }
    }
    public String write(Object value) {
        try { return mapper.writeValueAsString(value); } catch (Exception e) { throw new IllegalArgumentException("Cannot encode JSON", e); }
    }
    public ObjectNode object() { return mapper.createObjectNode(); }
    public JsonNode tree(Object value) { return mapper.valueToTree(value); }
    public static String str(JsonNode node, String name) { return node.path(name).isNull() ? null : node.path(name).asText(null); }
    public static String required(JsonNode node, String name) {
        JsonNode value = node.path(name);
        if (!value.isTextual() || value.asText().isBlank() || value.asText().length()>160) throw new IllegalArgumentException(name + " must be a nonempty string (max 160)");
        return value.asText();
    }
    public static double number(JsonNode node, String name) {
        if (!node.path(name).isNumber()) throw new IllegalArgumentException(name + " must be a number");
        double value=node.path(name).asDouble();
        if(!Double.isFinite(value)) throw new IllegalArgumentException(name + " must be finite");
        return value;
    }
}
