package dk.sdu.beerfactory;

import static dk.sdu.beerfactory.Store.*;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.springframework.stereotype.Service;

/**
 * Builds PRIVATE command context from a recipe snapshot. ControllerProtocol later reduces it to one
 * scalar physical operation.
 */
@Service
public class StageCommands {
  /** Creates the stateless mapper from central recipe snapshots to private command context. */
  public StageCommands() {}

  /**
   * Selects central recipe context for a stage; none of its business IDs are sent directly to
   * equipment.
   */
  public Map<String, Object> stagePayload(Map<String, Object> batch, int stage) {
    JsonNode recipe = jsonValue(batch, "recipe"), definition = recipe.path("stages").get(stage);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("batch_id", stringValue(batch, "id"));
    data.put("execution_id", stringValue(batch, "execution_id"));
    data.put("recipe_version_id", stringValue(batch, "recipe_version_id"));
    data.put("run_id", stringValue(batch, "run_id"));
    data.put("phases", definition.path("phases"));
    data.put("phase_index", stage == intValue(batch, "stage") ? intValue(batch, "phase_index") : 0);
    data.put("hold_h", definition.path("hold_h"));
    data.put("max_pause_h", definition.path("max_pause_h"));
    data.put("flush_h", recipe.path("flush_h").asDouble(.1));
    return data;
  }
}
