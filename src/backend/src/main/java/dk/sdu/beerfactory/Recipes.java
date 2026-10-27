package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;

public final class Recipes {
    private Recipes() {}
    public static void validate(JsonNode recipe) {
        Json.required(recipe,"id"); Json.required(recipe,"name");
        if(!recipe.path("version").isIntegralNumber() || recipe.path("version").asInt()<1) throw new IllegalArgumentException("version must be a positive integer");
        if(Json.number(recipe,"volume_hl")!=50) throw new IllegalArgumentException("volume_hl must be 50");
        JsonNode ingredients=recipe.path("ingredients");
        if(!ingredients.isObject() || ingredients.size()!=4) throw new IllegalArgumentException("four ingredients are required");
        for(String key:Set.of("malt_kg","hops_kg","yeast_kg","water_l")) Domain.nonnegativeFinite(Json.number(ingredients,key),key);
        Domain.positiveFinite(Json.number(recipe,"transfer_h"),"transfer_h");
        Domain.positiveFinite(Json.number(recipe,"flush_h"),"flush_h");
        JsonNode stages=recipe.path("stages");
        if(!stages.isArray() || stages.size()!=6) throw new IllegalArgumentException("six ordered machine stages required");
        for(int i=0;i<6;i++) {
            JsonNode stage=stages.get(i);
            if(!stage.path("type").asText().equals(Domain.STAGES.get(i))) throw new IllegalArgumentException("invalid stage order");
            JsonNode phases=stage.path("phases");
            if(!phases.isArray() || phases.isEmpty()) throw new IllegalArgumentException("phases required");
            if(i==4 && (phases.size()!=2 || !phases.get(0).path("name").asText().equals("fermentation") || !phases.get(1).path("name").asText().equals("maturation"))) throw new IllegalArgumentException("unitank requires fermentation then maturation");
            for(JsonNode phase:phases) {
                Json.required(phase,"name"); Domain.positiveFinite(Json.number(phase,"duration_h"),"duration_h");
                Json.number(phase,"target_temp_c");
            }
            if(i<5 || !stage.path("hold_h").isNull()) Domain.positiveFinite(Json.number(stage,"hold_h"),"hold_h");
            Domain.positiveFinite(Json.number(stage,"max_pause_h"),"max_pause_h");
        }
    }
}
