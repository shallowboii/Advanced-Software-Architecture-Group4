package dk.sdu.beerfactory;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class RecipesTest {
  static final Json JSON = new Json(new ObjectMapper());

  static ObjectNode recipe() {
    ObjectNode recipe = JSON.object();
    recipe.put("id", "lager");
    recipe.put("name", "Lager");
    recipe.put("version", 1);
    recipe.put("volume_hl", 50);
    recipe.put("transfer_h", .1);
    recipe.put("flush_h", .1);
    var ingredients = recipe.putObject("ingredients");
    ingredients.put("malt_kg", 900);
    ingredients.put("hops_kg", 20);
    ingredients.put("yeast_kg", 10);
    ingredients.put("water_l", 6000);
    var stages = recipe.putArray("stages");
    for (String type : Domain.STAGES) {
      var stage = stages.addObject();
      stage.put("type", type);
      stage.put("max_pause_h", 10);
      if (type.equals("filter")) stage.putNull("hold_h");
      else stage.put("hold_h", 20);
      var phases = stage.putArray("phases");
      for (String phase :
          type.equals("unitank")
              ? new String[] {"fermentation", "maturation"}
              : new String[] {type}) {
        var p = phases.addObject();
        p.put("name", phase);
        p.put("duration_h", 1);
        p.put("target_temp_c", 20);
      }
    }
    return recipe;
  }

  @Test
  void acceptsSharedUnitankAndNoFilterHold() {
    assertDoesNotThrow(() -> Recipes.validate(recipe()));
  }

  @Test
  void rejectsChangedBatchVolume() {
    var r = recipe();
    r.put("volume_hl", 49);
    assertThrows(IllegalArgumentException.class, () -> Recipes.validate(r));
  }

  @Test
  void rejectsSplitFermentation() {
    var r = recipe();
    ((ObjectNode) r.path("stages").get(4).path("phases").get(1)).put("name", "filter");
    assertThrows(IllegalArgumentException.class, () -> Recipes.validate(r));
  }

  @Test
  void rejectsBooleanIngredient() {
    var r = recipe();
    ((ObjectNode) r.path("ingredients")).put("malt_kg", true);
    assertThrows(IllegalArgumentException.class, () -> Recipes.validate(r));
  }

  @Test
  void rejectsUnknownIngredient() {
    var r = recipe();
    ((ObjectNode) r.path("ingredients")).put("sand_kg", 1);
    assertThrows(IllegalArgumentException.class, () -> Recipes.validate(r));
  }

  @Test
  void rejectsZeroHoldAndNonfiniteDuration() {
    var r = recipe();
    ((ObjectNode) r.path("stages").get(0)).put("hold_h", 0);
    assertThrows(IllegalArgumentException.class, () -> Recipes.validate(r));
    var other = recipe();
    ((ObjectNode) other.path("stages").get(0).path("phases").get(0)).put("duration_h", Double.NaN);
    assertThrows(IllegalArgumentException.class, () -> Recipes.validate(other));
  }
}
