package dk.sdu.beerfactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.DateTimeException;
import java.util.*;
import java.util.function.Supplier;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class ApiController {
    private final FactoryService factory;
    private final InventoryClient inventory;
    private final Bootstrap bootstrap;
    private final Json json;
    public ApiController(FactoryService factory,InventoryClient inventory,Bootstrap bootstrap,Json json) {this.factory=factory;this.inventory=inventory;this.bootstrap=bootstrap;this.json=json;}
    @GetMapping("/health") public ResponseEntity<?> health(){boolean ready=bootstrap.ready&&factory.healthy();return ResponseEntity.status(ready?200:503).body(Map.of("status",ready?"ok":"unavailable"));}
    @GetMapping("/machines") public Object machines(){return factory.machines();}
    @GetMapping("/recipes") public Object recipes(){return factory.recipes();}
    @GetMapping("/orders") public Object orders(){return factory.orders();}
    @GetMapping("/batches") public Object batches(@RequestParam(required=false,name="run_id") String run){return factory.batches(run);}
    @GetMapping("/line") public Object line(){return factory.line();}
    @GetMapping("/events") public Object events(@RequestParam Map<String,String> filters){return factory.events(filters,false);}
    @GetMapping("/telemetry") public Object telemetry(@RequestParam Map<String,String> filters){return factory.events(filters,true);}
    @GetMapping("/inventory") public Object stock(){return inventory.call("GET","/stock",null);}
    @GetMapping("/experiment/status") public Object experiment(){return factory.experimentStatus();}
    private JsonNode mutate(String scope,String key,JsonNode body,Supplier<Object> action){return factory.mutate(scope,key,body==null?json.object():body,action);}
    private String operator(String value){if(value==null||value.isBlank())return "dashboard";if(value.length()>160)throw new IllegalArgumentException("X-Operator too long");return value;}
    @PostMapping("/orders") public Object order(@RequestBody JsonNode body,@RequestHeader(value="Idempotency-Key",required=false) String key,@RequestHeader(value="X-Operator",required=false) String by){return mutate("orders",key,body,()->factory.order(body,operator(by)));}
    @PostMapping("/recipes") public Object recipe(@RequestBody JsonNode body,@RequestHeader(value="Idempotency-Key",required=false) String key){return mutate("recipes",key,body,()->factory.publishRecipe(body,null));}
    @PostMapping("/recipes/{id}/versions") public Object version(@PathVariable String id,@RequestBody JsonNode body,@RequestHeader(value="Idempotency-Key",required=false) String key){return mutate("recipes/"+id,key,body,()->factory.publishRecipe(body,id));}
    @PostMapping("/orders/{id}/cancel") public Object cancel(@PathVariable String id,@RequestHeader(value="Idempotency-Key",required=false) String key,@RequestHeader(value="X-Operator",required=false) String by){return mutate("orders/"+id+"/cancel",key,null,()->factory.cancelOrder(id,operator(by)));}
    @PostMapping("/batches/{id}/flush") public Object flush(@PathVariable String id,@RequestHeader(value="Idempotency-Key",required=false) String key,@RequestHeader(value="X-Operator",required=false) String by){return mutate("batches/"+id+"/flush",key,null,()->factory.flushBatch(id,"operator",operator(by)));}
    @PostMapping("/machines/{id}/{operation:stop|resume|flush}") public Object control(@PathVariable String id,@PathVariable String operation,@RequestBody(required=false) JsonNode body,@RequestHeader(value="Idempotency-Key",required=false) String key,@RequestHeader(value="X-Operator",required=false) String by){return mutate("machines/"+id+"/"+operation,key,body,()->factory.machineControl(id,operation,body==null?json.object():body,operator(by)));}
    @PostMapping("/line/{operation:stop|resume}") public Object lineControl(@PathVariable String operation,@RequestHeader(value="Idempotency-Key",required=false) String key,@RequestHeader(value="X-Operator",required=false) String by){return mutate("line/"+operation,key,null,()->factory.stopLine(operation.equals("stop"),operator(by)));}
    @PostMapping("/experiment/start") public Object start(@RequestBody JsonNode body,@RequestHeader(value="Idempotency-Key",required=false) String key){return mutate("experiment/start",key,body,()->factory.startExperiment(body));}
    @PostMapping("/experiment/stop") public Object stop(@RequestHeader(value="Idempotency-Key",required=false) String key){return mutate("experiment/stop",key,null,factory::stopExperiment);}
    @PostMapping("/inventory/restock") public Object restock(@RequestBody JsonNode body,@RequestHeader(value="Idempotency-Key",required=false) String key){
        if(!body.isObject())throw new IllegalArgumentException("object required");
        ObjectNode request=(ObjectNode)body.deepCopy();
        if(!request.hasNonNull("request_id"))request.put("request_id",key==null?Store.id():key);
        // The Python service persists the idempotency key with the stock update.
        return inventory.call("POST","/restock",request);
    }
    @RestControllerAdvice
    public static class Errors {
        @ExceptionHandler(FactoryService.Conflict.class) public ResponseEntity<?> conflict(Exception e){return error(409,"conflict",e.getMessage());}
        @ExceptionHandler(FactoryService.Missing.class) public ResponseEntity<?> missing(Exception e){return error(404,"not_found",e.getMessage());}
        @ExceptionHandler({IllegalArgumentException.class,DateTimeException.class,HttpMessageNotReadableException.class}) public ResponseEntity<?> invalid(Exception e){return error(400,"invalid_request",e instanceof HttpMessageNotReadableException?"JSON body required":e.getMessage());}
        @ExceptionHandler(InventoryClient.Unavailable.class) public ResponseEntity<?> unavailable(Exception e){return error(503,"inventory_unavailable",e.getMessage());}
        @ExceptionHandler(InventoryClient.Rejected.class) public ResponseEntity<?> rejected(InventoryClient.Rejected e){return ResponseEntity.status(e.status).body(e.body);}
        private ResponseEntity<?> error(int status,String code,String message){return ResponseEntity.status(status).body(Map.of("error",code,"message",message==null?code:message));}
    }
}
