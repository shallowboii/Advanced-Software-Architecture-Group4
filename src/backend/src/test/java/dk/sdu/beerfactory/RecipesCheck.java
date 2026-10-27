package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;

/** Runs against production recipe files using Jackson; no JUnit or running services required. */
public final class RecipesCheck {
    private static int checks;
    private static void rejected(ObjectNode recipe){checks++;try{Recipes.validate(recipe);}catch(IllegalArgumentException expected){return;}throw new AssertionError("invalid recipe accepted");}
    public static void main(String[] args)throws Exception {
        Json json=new Json(new ObjectMapper());
        for(String path:args)for(var recipe:json.read(Files.readString(Path.of(path))).path("recipes")){
            Recipes.validate(recipe);checks++;
            var copy=(ObjectNode)recipe.deepCopy();copy.put("volume_hl",49);rejected(copy);
            copy=(ObjectNode)recipe.deepCopy();((ObjectNode)copy.path("ingredients")).put("malt_kg",true);rejected(copy);
            copy=(ObjectNode)recipe.deepCopy();((ObjectNode)copy.path("ingredients")).put("unknown",1);rejected(copy);
            copy=(ObjectNode)recipe.deepCopy();((ObjectNode)copy.path("stages").get(0)).put("hold_h",0);rejected(copy);
            copy=(ObjectNode)recipe.deepCopy();((ObjectNode)copy.path("stages").get(4).path("phases").get(1)).put("name","fermentation");rejected(copy);
            copy=(ObjectNode)recipe.deepCopy();((ObjectNode)copy.path("stages").get(0).path("phases").get(0)).put("duration_h",Double.NaN);rejected(copy);
        }
        if(checks==0)throw new AssertionError("recipe fixtures required");
        System.out.println("Recipe validation: "+checks+" checks passed");
    }
}
