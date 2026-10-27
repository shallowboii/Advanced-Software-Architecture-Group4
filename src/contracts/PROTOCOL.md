# Beer Factory protocol version 1

This is the shared implementation contract. Changes require updating every producer, consumer and fixture.

## Identity and time

Machine types, in order: `mash`, `lauter`, `kettle`, `whirlpool`, `unitank`, `filter`. IDs are `<type>-<ordinal>`. Every batch is 50 hL. No buffers or cleaning. A uni-tank executes fermentation and maturation without releasing the batch between phases.

JSON uses snake_case. Envelope: `schema_version:1`, `message_id`, `machine_id`, `batch_id` (nullable), `execution_id` (nullable), `transfer_id` (nullable), `recipe_version_id` (nullable), `run_id` (nullable), `ts_utc`, `sim_time_h`.

MQTT topics: `factory/<type>/<id>/cmd`, `heartbeat`, `events`, `ack`. Commands/events/acks use QoS 1; heartbeat QoS 0. Global commands use exact subscription `factory/line/control`. Executable commands are never retained. Commands have `command_id`, `cmd`, `epoch` (monotonically increasing coordinator fencing token). Critical events have `event_id`, `event`, `machine_sequence`, `controller_boot_id`; backend acks with `{event_id}`. Replay until acknowledged; backend deduplicates by event_id. Persist commands before acknowledging. Broker delivery alone is not execution acknowledgement.

`TIME_SCALE` means real seconds per simulated hour. PERMIT carries authoritative `sim_time_h` and `time_scale`. A controller extrapolates using monotonic time. Holding/ageing continues during STOP. Processing/transfer/flush progress pauses. Heartbeat defaults to 2 real seconds, permit expiry to 3 real seconds. All expiry limits are checked locally, including while stopped.

## Controller commands

All targeted commands include machine_id. PERMIT and STOP bypass normal state guards; stale epochs cannot re-enable a machine. Command retries use the same ID.

| cmd | Payload and behaviour |
| --- | --- |
| PERMIT | `permit_ms:3000, sim_time_h, time_scale`; establishes epoch and allows operation if no stop latch. Never clears STOP. |
| START | `batch_id, execution_id, recipe_version_id, run_id, phases:[{name,duration_h,target_temp_c}], hold_h, max_pause_h, flush_h?`; requires EMPTY. Optional flush_h overrides the configured default for this batch and survives restart. |
| PREPARE_TRANSFER | `transfer_id, role:source|target, peer_id, batch_id, execution_id, recipe_version_id, run_id, phases, hold_h, max_pause_h, flush_h?`; source must be READY_TO_TRANSFER and target EMPTY. Target persists staged next-stage inputs; source retains ownership. |
| EXECUTE_TRANSFER | `transfer_id, duration_h`; requires matching reservation. Both participants progress for duration_h then emit TRANSFER_READY and wait for COMMIT. |
| COMMIT_TRANSFER | `transfer_id, discard_reason?`; only after TRANSFER_READY and a durable backend commit. Commit the source first; its TRANSFER_COMMITTED preserves any discard_reason, even after emptying. Then commit the target with that reason. A tainted target enters FLUSH_PENDING instead of production. |
| ABORT_TRANSFER | `transfer_id`; cancel pending transfer before commit; source retains batch, target empties. No automatic discard. |
| STOP | `scope:global|local`; immediate latched inhibition, no material movement, no wait for stage completion. |
| RESUME | `scope:global|local`; explicit reset of that latch only. A global reset cannot silently clear a physical/local stop. Requires permit before progress. |
| FLUSH | `batch_id, reason`; marks batch irrecoverable, cancels production and starts timed removal when permitted and not stopped. If transfer unresolved, emits FLUSH_REQUESTED and waits for reconciliation/abort before removal. |
| SNAPSHOT | Reply with current state. |

Operations: EMPTY, RESERVED_IN, PROCESSING, READY_TO_TRANSFER, TRANSFERRING_OUT, TRANSFERRING_IN, FLUSH_PENDING, FLUSHING, RECOVERING, FAULT. Keep local_stop/global_stop and permit separately. No cleaning state. Filter phase completion emits COMPLETED and releases batch atomically. Terminal loss is counted once, never again on later removal.

Events: STARTED, PHASE_COMPLETED, READY_TO_TRANSFER, TRANSFER_PREPARED, TRANSFER_READY, TRANSFER_COMMITTED, TRANSFER_ABORTED, STOPPED, RESUMED, FLUSH_REQUESTED, SPOILED, FLUSHED, COMPLETED, REJECTED, SNAPSHOT. Every event includes current `snapshot`; preserve affected batch/transfer IDs in the event envelope even if snapshot is now empty. Command result events include command_id; rejected commands also include reason. Local flush/stop call the same durable controller handlers as network commands. Command deduplication excludes epoch from the semantic payload so a new leader can replay a durable operation with a newer fencing token.

Snapshot fields: `machine_id,type,state,batch_id,execution_id,transfer_id,recipe_version_id,run_id,phase,phase_index,phase_elapsed_h,phase_duration_h,hold_deadline_h,local_stop,global_stop,stopped,permit_valid,epoch,state_version,sim_time_h,machine_sequence,controller_boot_id`. Heartbeats carry this snapshot at top level, plus schema_version and ts_utc. `stopped` includes expired permit. Snapshot does not move backend ownership on its own; reconcile with durable transfer decision.

Kafka `telemetry`, key machine_id: envelope plus `sensor,value,unit,phase`. Kafka `batch-events`, key batch_id: canonical backend production event with event_id and reason. Critical state events also traverse MQTT to the backend; Kafka telemetry is not a substitute for their acknowledgements. Backend emits ADMITTED at durable admission and exactly one LOSS_RECORDED per lost batch, with reason spoilage or operator. Rejected stale ownership evidence is IGNORED_MACHINE_EVENT with original_event and accepted:false. Export is ordered by monotonically increasing recorded_seq; consumers must not paginate by event timestamp.

## HTTP service ports and contracts

Backend port 8080, inventory port 8081, frontend container port 80 (host 8088). Backend `/api/health` returns `{status:"ok"}` only after database initialisation; `/ws` is raw JSON WebSocket, event `{type, data}`. Frontend uses same-origin `/api` and `/ws`.

Inventory: `GET /health`, `GET /stock` -> `{stock:{malt_kg,hops_kg,yeast_kg,water_l}}`; `POST /reserve` body `{batch_id, ingredients:{malt_kg,hops_kg,yeast_kg,water_l}}` -> `{batch_id,status:"reserved",stock}`; insufficient stock returns 409 with `error:"insufficient_stock"`. Existing key with identical quantities returns the previous reservation without deducting again; conflicting quantities return 409. `GET /reservations/{batch_id}`; `POST /reservations/{batch_id}/release` releases once before admission cancellation. `POST /restock` accepts `{request_id, ingredients:{...}}`, idempotently. Validate finite, nonnegative numeric values, reject booleans/unknown fields, persist stock and reservation state atomically. Storage path `INVENTORY_STATE_FILE`, seed `INITIAL_BATCHES` default 100. Service serialises mutations.

Backend reads recipes from `RECIPE_FILE` (default `/app/config/recipes.json`), env `DATABASE_URL`, `DATABASE_USER`, `DATABASE_PASSWORD`, `REDIS_HOST`, `FACTORY_REDIS_PORT` (numeric, default 6379), `MQTT_URL`, `MQTT_USERNAME`, `MQTT_PASSWORD`, `KAFKA_BROKERS`, `INVENTORY_URL`, `TIME_SCALE`, `POLICY`, `WIP_LIMIT`, `RUN_ID`.

Inventory release of an unknown batch persists a released tombstone. A delayed reservation for that ID must fail without deducting stock; otherwise cancellation could leak ingredients when reserve and release race.

REST under /api: GET /machines, /recipes, /orders, /batches, /events, /inventory, /line, /telemetry; POST /orders `{recipe_id,quantity,priority:0}`, /recipes (new recipe), /recipes/{id}/versions (new immutable definition), /orders/{id}/cancel, /batches/{id}/flush, /machines/{id}/stop, /machines/{id}/resume, /machines/{id}/flush `{batch_id}`, /line/stop, /line/resume, /inventory/restock. Collection GETs return JSON arrays, inventory retains its object shape. POST /experiment/start `{run_id,policy,wip_limit,horizon_h}` only when no active batch; GET /experiment/status; POST /experiment/stop. Mutations accept `Idempotency-Key` and `X-Operator` headers. Conflicts return 409 `{error, message}`.

Machine records expose snapshot fields plus `online,last_seen`. Orders expose `id,recipe_id,quantity,started,completed,discarded,priority,status`. Batches expose `id,order_id,recipe_id,recipe_version_id,status,machine_id,stage,volume_hl,created_at,completed_at,run_id`. Line exposes `stopped,policy,wip_limit,active_wip,sim_time_h,machines_pending_stop`. Experiment status includes `run_id,policy,started,completed,spoiled,operator_discarded,hl_out,hl_wasted,sim_clock_h,done`.

## Recipe definition

Root `{recipes:[...]}`; each recipe `{id,name,version,volume_hl:50,ingredients:{...},stages:[{type,phases:[{name,duration_h,target_temp_c}],hold_h,max_pause_h}],transfer_h:0.1,flush_h:0.1}`. Exactly six stages in the declared order. Uni-tank has exactly fermentation and maturation phases. No code changes to add another recipe with these stages. Reject invalid/negative/nonfinite times and mismatched volume.
