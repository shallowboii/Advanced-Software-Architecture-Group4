package dk.sdu.beerfactory;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Verifies the architectural boundary against the actual Jackson serializer. */
class ControllerProtocolTest {
  private final Json json = RecipesTest.JSON;
  private final ControllerProtocol protocol = new ControllerProtocol(null, json);

  /**
   * Rich central context must reduce to one scalar actuator operation and technical fencing data.
   */
  @Test
  void commandSerializationNeverLeaksBusinessContext() {
    ObjectNode context = json.object();
    context.put("command_id", "operation-1");
    context.put("message_id", "operation-1");
    context.put("machine_id", "unitank-1");
    context.put("controller_boot_id", "boot-1");
    context.put("cmd", "PROCESS");
    context.put("expected_state_version", 4);
    context.put("phase_index", 1);
    context.set("phases", RecipesTest.recipe().path("stages").get(4).path("phases"));
    for (String field :
        Set.of(
            "batch_id",
            "execution_id",
            "recipe_version_id",
            "run_id",
            "transfer_id",
            "peer_id",
            "phase",
            "hold_h",
            "max_pause_h",
            "reason")) context.put(field, "private-business-data");
    ObjectNode wire = protocol.actuatorCommand(context, 9);
    assertEquals(2, wire.path("schema_version").asInt());
    assertEquals(9, wire.path("epoch").asInt());
    assertEquals(1, wire.path("duration_h").asDouble());
    assertEquals(20, wire.path("target_temp_c").asDouble());
    assertEquals(4, wire.path("expected_state_version").asInt());
    for (String field :
        Set.of(
            "batch_id",
            "execution_id",
            "recipe_version_id",
            "run_id",
            "transfer_id",
            "peer_id",
            "phase",
            "phases",
            "phase_index",
            "hold_h",
            "max_pause_h",
            "reason")) assertFalse(wire.has(field), field + " leaked onto physical wire");
  }

  /** A transfer reservation only tells equipment which physical end it represents. */
  @Test
  void transferWireHasNoTransferIdentityOrPeerKnowledge() {
    ObjectNode context = json.object();
    context.put("cmd", "PREPARE_TRANSFER");
    context.put("command_id", "prepare-1");
    context.put("controller_boot_id", "boot-1");
    context.put("role", "target");
    context.put("transfer_id", "transfer-secret");
    context.put("peer_id", "other-machine");
    context.put("expected_state_version", 0);
    ObjectNode wire = protocol.actuatorCommand(context, 1);
    assertEquals("target", wire.path("role").asText());
    assertFalse(wire.has("transfer_id"));
    assertFalse(wire.has("peer_id"));
  }
}
