# Architecture and responsibilities

The scheduler tracks beer; machine controllers operate equipment. The original component diagram places durable production data in PostgreSQL and live observations in Redis. No controller database or inventory database is part of this architecture.

## Component boundaries

| Component | Responsibility | State and communication |
| --- | --- | --- |
| C machine Pod | Execute technical operations, report occupancy/progress/sensors, enforce local STOP and run permit | RAM only; MQTT commands/events, Kafka sensor readings |
| Java backend | Orders, recipe versions, batch location, phase sequencing, transfer decisions, expiry and loss accounting | PostgreSQL durable records; Redis live machine registry; MQTT, Kafka and HTTP |
| Python inventory mock | Stock, ingredient reservations and restock | RAM only; HTTP/JSON |
| React frontend / nginx | Display the backend's production view and submit operator requests | HTTP and WebSocket |
| Experiment tools | Submit repeatable demand and analyse recorded results | HTTP and JSONL/CSV artifacts |

A controller does not receive or store batch, recipe, order, run or business transfer identifiers. It receives technical instructions such as a duration and temperature. The scheduler associates actuator command IDs and machine observations with its own production records. A batch label in the dashboard is a backend projection, not knowledge held inside the machine.

## Operation of the line

One physical machine is represented by one C process in one Kubernetes Pod. Six StatefulSets provide stable names; the unitank set has 20 or 40 replicas and every other set has one. StatefulSets provide identity, not local data storage. Fermentation and maturation occur in the same occupied unitank; the scheduler selects and sequences those operations. There are no intermediate buffers and no cleaning step.

Each batch is 50 hL. The scheduler reserves both endpoints before initiating a timed transfer and records its decision centrally. Neither endpoint becomes available merely because an acknowledgement is late. The controller reports technical completion; the backend decides which batch moved, which recipe step comes next, whether a holding limit expired, and whether to record output or waste. Final filter output leaves the modelled factory.

A machine can be flushed using its simulated physical button, without knowing which batch it holds. The scheduler records the resulting loss for the centrally assigned batch. Dashboard flush requests identify the batch at the API boundary so the scheduler can reject a stale user action before sending an equipment instruction.

## STOP, retries and restart limits

Local and global STOP are separate technical latches. Process, transfer and flush progress pause when stopped or when the run permit expires. Simulation time continues; the scheduler checks holding and interruption limits. A fresh permit does not reset STOP. The local Unix socket models physical buttons independently of application brokers; this remains simulated equipment, not a certified emergency circuit.

Command IDs correlate acknowledgements and deduplicate retries in the same controller process. Boot IDs identify that process instance and prevent old queued instructions from executing on a replacement controller. Pending controller events are held in RAM until the backend acknowledges durable receipt in PostgreSQL. Sensor samples go through Kafka; the backend enriches them using its own command/production mapping.

Restarting a controller loses its local execution state. Restarting a busy, assigned or unresolved controller stops and invalidates the run; the backend preserves central evidence and rejects resume instead of inventing physical contents. Start a fresh isolated factory for the next run. An empty unassigned controller can restart with a new boot ID. A backend restart or temporary broker disconnection can reconcile against still-running controllers.

The Python mock also loses stock and reservation history on restart. Its single replica uses Recreate updates; two simultaneous RAM stock copies would be inconsistent. Do not restart the mock during a measurement. Persistence and high availability for a real external inventory service are outside this mock.

## Evidence and scope

PostgreSQL, RabbitMQ and Kafka retain infrastructure volumes. Redis is a rebuildable live registry with heartbeat TTL. Backend replicas share central coordination and use one fenced scheduler. Machine updates use OnDelete to avoid automatically replacing occupied vessels. These choices do not establish high availability of the single-instance infrastructure.

The reduced UPPAAL model describes system-level occupancy and reservation rules. Its abstract ownership variables belong to the system model; they do not imply a controller-side batch database. See `validation-status.md` for executed checks and the remaining experiment, rollout and report work. The group's original hand-in workflow is retained unchanged.
