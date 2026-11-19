# Beer Factory equipment protocol version 2

The scheduler knows the beer. Controllers know only equipment state and technical commands. This is a breaking replacement for the previous batch-aware protocol: use matching backend/controller images in a fresh factory.

## Identity, correlation and time

Machine types are `mash`, `lauter`, `kettle`, `whirlpool`, `unitank`, `filter`; instance IDs are `<type>-<ordinal>`. A controller knows its type and physical occupancy, but no batch, order, recipe, phase name, run or business transfer ID. The scheduler stores those mappings in PostgreSQL. It also sequences fermentation and maturation in the same unitank.

JSON uses snake_case and `schema_version:2`. MQTT topics remain `factory/<type>/<id>/cmd`, `heartbeat`, `events`, `ack`, with `factory/line/control` for global STOP. Commands/events/acks use QoS 1, heartbeats QoS 0. Commands are never retained. An acknowledgement of broker delivery is not an acknowledgement of actuator completion.

`command_id` identifies an actual technical instruction and supports same-process retry deduplication. `controller_boot_id` identifies one controller process. `epoch` fences old coordinator leaders. Material commands also carry `expected_state_version`, so a delayed command cannot affect a later equipment operation. Check duplicate IDs before checking the state guard. Neither correlation field identifies beer.

`TIME_SCALE` is real seconds per simulated hour. PERMIT supplies the simulation clock mapping. Technical operation progress pauses during STOP or permit loss. The scheduler alone tracks material age, holding limits, phase deadlines and spoilage. The clock continues during STOP. Default heartbeat interval is two real seconds and permit lifetime is three.

## Commands

Network commands include `schema_version,command_id,cmd,epoch`. Targeted actuator commands additionally require `machine_id,controller_boot_id,expected_state_version`. The schema forbids unknown fields, including business identifiers. Retry the same instruction with its original command ID.

| cmd | Technical payload and behaviour |
| --- | --- |
| PERMIT | `permit_ms,sim_time_h,time_scale,controller_boot_id`; renew permission without clearing STOP. |
| START | `duration_h,target_temp_c`; load an EMPTY machine and begin one operation. |
| PROCESS | `duration_h,target_temp_c`; execute one operation on contents already held in READY. |
| PREPARE_TRANSFER | `role:source|target`; reserve a READY source or EMPTY receiver. No peer or transfer business ID. |
| EXECUTE_TRANSFER | `duration_h`; simulate the reserved pump operation, then report completion and wait. |
| COMMIT_TRANSFER | Set a finished source EMPTY or receiver occupied and READY. Receiver processing requires a separate PROCESS command. |
| ABORT_TRANSFER | Cancel the equipment reservation; source retains contents and receiver becomes EMPTY. |
| RELEASE | Empty READY equipment; the scheduler uses this for final output after filtering. |
| STOP | `scope:local|global`; latch immediate inhibition. STOP remains conservative across delayed messages. |
| RESUME | `scope:local|global`; explicitly reset only that latch. A permit is still needed. |
| FLUSH | Optional `duration_h`; request removal of current contents. During transfer, wait for scheduler resolution before emptying. |
| SNAPSHOT | Report the current technical observation. |

The local `machinectl flush` simulates a physical button and has no batch argument. The dashboard instead identifies a batch to the backend API; the scheduler validates its current assignment before sending a guarded equipment FLUSH.

## Equipment observations

Technical states: `EMPTY`, `READY`, `PROCESSING`, `RESERVED_IN`, `TRANSFERRING_IN`, `TRANSFERRING_OUT`, `FLUSH_PENDING`, `FLUSHING`. STOP is represented by separate latches, not a replacement operation state. There is no cleaning state.

Snapshots expose `machine_id,type,controller_boot_id,state,occupied,duration_h,elapsed_h,target_temp_c,transfer_ready,flush_requested,local_stop,global_stop,stopped,permit_valid,epoch,state_version,machine_sequence,sim_time_h,ts_utc`, and applicable `current_command_id,interrupted_command_id,transfer_role`. See the event JSON schema for the exact object. A frontend phase label or batch number is added by the backend, never emitted by the controller.

Events are `PROCESS_STARTED`, `PROCESS_COMPLETED`, `TRANSFER_PREPARED`, `TRANSFER_READY`, `TRANSFER_COMMITTED`, `TRANSFER_ABORTED`, `RELEASED`, `STOPPED`, `RESUMED`, `FLUSH_REQUESTED`, `FLUSHED`, `REJECTED`, `SNAPSHOT`. Every event includes a technical snapshot and its process/sequence identity. Timer completions refer to the initiating actuator command. Interrupted-command correlation lets the backend associate a local flush with historical equipment work even if delivery is delayed. `contents_discarded` reports physical removal/flush intent during transfer; the scheduler decides the resulting business outcome.

Events remain in RAM until backend acknowledgement `{event_id}`. The backend persists and deduplicates them before acknowledging. Restart loses controller progress, latches and pending events; there is no disk journal. An occupied, assigned or unresolved restart latches backend STOP and invalidates the run. Resume returns a conflict until a fresh factory is started. Central evidence is preserved; missing material is not fabricated or counted as completed.

Kafka `telemetry`, keyed by machine ID, contains only technical sensor data and actuator-command correlation. The backend joins that correlation to its durable command records, rather than assuming a delayed sample belongs to the current occupant. Kafka `batch-events`, keyed by batch ID, contains canonical business events produced by the backend, including ADMITTED, phase/transfer observations, exactly one LOSS_RECORDED per loss, and final COMPLETED. Controller events and canonical business events are different contracts. Event export uses monotonically increasing `recorded_seq`; never paginate by source timestamp.

## HTTP service ports and contracts

Backend port 8080, inventory port 8081, frontend container port 80 (host 8088). Backend `/api/health` returns `{status:"ok"}` only after database initialisation; `/ws` is raw JSON WebSocket, event `{type, data}`. Frontend uses same-origin `/api` and `/ws`.

Inventory: `GET /health`, `GET /stock` -> `{stock:{malt_kg,hops_kg,yeast_kg,water_l}}`; `POST /reserve` body `{batch_id, ingredients:{malt_kg,hops_kg,yeast_kg,water_l}}` -> `{batch_id,status:"reserved",stock}`; insufficient stock returns 409 with `error:"insufficient_stock"`. Existing key with identical quantities returns the previous reservation without deducting again; conflicting quantities return 409. `GET /reservations/{batch_id}`; `POST /reservations/{batch_id}/release` releases once before admission cancellation. `POST /restock` accepts `{request_id, ingredients:{...}}`, idempotently. Validate finite, nonnegative numeric values, reject booleans/unknown fields, update stock and reservations together under a process-local lock. Seed `INITIAL_BATCHES` defaults to 100. State and idempotency history are held in RAM and reset on process restart.

Backend reads recipes from `RECIPE_FILE` (default `/app/config/recipes.json`), env `DATABASE_URL`, `DATABASE_USER`, `DATABASE_PASSWORD`, `REDIS_HOST`, `FACTORY_REDIS_PORT` (numeric, default 6379), `MQTT_URL`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `KAFKA_BROKERS`, `INVENTORY_URL`, `TIME_SCALE`, `POLICY`, `WIP_LIMIT`, `RUN_ID`.

Inventory release of an unknown batch holds a released tombstone in RAM. A delayed reservation for that ID must fail without deducting stock; otherwise cancellation could leak ingredients when reserve and release race.

REST under /api: GET /machines, /recipes, /orders, /batches, /events, /inventory, /line, /telemetry; POST /orders `{recipe_id,quantity,priority:0}`, /recipes (new recipe), /recipes/{id}/versions (new immutable definition), /orders/{id}/cancel, /batches/{id}/flush, /machines/{id}/stop, /machines/{id}/resume, /machines/{id}/flush `{batch_id}`, /line/stop, /line/resume, /inventory/restock. Collection GETs return JSON arrays, inventory retains its object shape. POST /experiment/start `{run_id,policy,wip_limit,horizon_h}` only when no active batch; GET /experiment/status; POST /experiment/stop. Mutations accept `Idempotency-Key` and `X-Operator` headers. Conflicts return 409 `{error, message}`.

Machine records expose technical state plus a backend-enriched production view: `online,last_seen,batch_id,execution_id,recipe_version_id,run_id,phase,phase_index,phase_elapsed_h,phase_duration_h,hold_deadline_h`. Those business fields never appear on the controller wire. Orders expose `id,recipe_id,quantity,started,completed,discarded,priority,status`. Batches expose `id,order_id,recipe_id,recipe_version_id,status,machine_id,stage,volume_hl,created_at,completed_at,run_id`. Line exposes `stopped,policy,wip_limit,active_wip,sim_time_h,machines_pending_stop,restart_blocked,recovery_required,failure_reason`. Experiment status includes `run_id,policy,started,completed,spoiled,operator_discarded,hl_out,hl_wasted,sim_clock_h,done`, plus failure/restart flags and `requires_clean_factory`.

## Recipe definition

Root `{recipes:[...]}`; each recipe `{id,name,version,volume_hl:50,ingredients:{...},stages:[{type,phases:[{name,duration_h,target_temp_c}],hold_h,max_pause_h}],transfer_h:0.1,flush_h:0.1}`. Exactly six stages in the declared order. Uni-tank has exactly fermentation and maturation phases. No code changes to add another recipe with these stages. Reject invalid/negative/nonfinite times and mismatched volume.
