# Java production backend

Java 21, Spring Boot, Maven and explicit JDBC transactions. Build from the repository root with `docker build -f src/backend/Dockerfile .`, or run `mvn -f src/backend/pom.xml verify`. The shared contract is `src/contracts/PROTOCOL.md`.

## Architectural boundary

The backend owns orders, batches, recipes, phase sequencing, quality deadlines, transfer decisions and experiment measurements. A physical controller knows only its own technical occupancy, actuator operation, elapsed time, setpoint and stop/permit state. It receives no batch, recipe, order, phase, run, execution or transfer identity.

`ProductionJournal` stores private command context in PostgreSQL. `ControllerProtocol` constructs each outgoing schema-2 command from an explicit technical allowlist. `START` loads an empty machine and runs one scalar operation; `PROCESS` runs one operation on already occupied equipment. Both carry only duration and target temperature. In a unitank, the backend sends separate fermentation and maturation operations while the same machine remains occupied. The filter outlet is a separate `RELEASE` command; its acknowledgement produces the central `COMPLETED` event.

Actual actuator command IDs, controller boot IDs and equipment state versions correlate and fence technical messages. They are not disguised batch identifiers: several actuator commands may belong to one centrally tracked phase or transfer. Delayed sensor samples are joined to their original command record, never whichever batch happens to occupy the machine when the sample arrives.

Redis is the live machine registry, with a six-second heartbeat TTL. Expiry or Redis unavailability makes a machine offline and stops permit renewal; PostgreSQL audit snapshots are not a liveness fallback. PostgreSQL retains recipes, orders, batch/assignment decisions, private command associations and canonical history. Kafka retains raw technical telemetry; the backend persists its centrally enriched projection. No local controller database exists.

## Reading the code

| Class | Responsibility |
| --- | --- |
| `ApiController`, `LiveSocket` | HTTP mutations/queries and dashboard projections. |
| `FactoryService` | Application facade and transactional HTTP idempotency. |
| `OrderService`, `Recipes` | Orders and validated immutable recipe versions. |
| `AdmissionScheduler` | PUSH/CONWIP admission, stock reservation and applicable actuator retries. |
| `OperationScheduler`, `StageCommands` | Central phase sequencing, holding deadlines and cumulative interruption budget per phase. |
| `TransferCoordinator` | Central two-end reservation/commit decisions; source release precedes target processing. |
| `BatchLifecycle` | One terminal outcome and one loss record per batch; removal is separate from loss. |
| `MachineEvents`, `ControllerProtocol` | Technical observations, immutable command correlation and physical/business boundary. |
| `MachineRegistry` | Redis liveness, technical observations and explicit controller restart failure handling. |
| `LineService` | Independent local/global STOP latches and tracked acknowledgement barriers. |
| `ProductionJournal`, `Store` | Commands, events, outbox and the shared JDBC transaction boundary. |
| `Coordinator`, `Leadership` | Scheduled network work and one fenced backend coordinator. |
| `MqttGateway`, `KafkaGateway`, `InventoryClient` | Transport adapters with bounded failure handling. |
| `ExperimentService`, `HistoryService` | Run boundaries, counters and reproducible event/sensor exports. |

Material decisions and their resulting commands commit together under one factory row lock. Both backend replicas can ingest events and serve the API; a PostgreSQL advisory lock selects the coordinator, and a monotonically increasing epoch fences publications. Backend leadership handover does not reconstruct a physical controller.

Transfers keep one fixed 50 hL batch in the source until the central commit decision. The target is reserved, never an intermediate buffer. After both transfer timers complete, the source acknowledges release before the target accepts material. Target acceptance never starts processing by itself. The backend first checks whether the source reported discarded contents, then issues either `PROCESS` or `FLUSH`. Holding deadlines and cumulative phase interruption budgets are checked centrally, including while the line is stopped. `LOSS_RECORDED` counts waste once; `FLUSHED` confirms later removal.

## Explicit process-restart boundary

C controller execution state, deduplication and outgoing events live in RAM. A process restart creates an empty controller with a new boot ID. Old broker-queued actuator commands target the old boot and cannot execute on the new one. An empty, unassigned controller may restart normally, and its state-version counter may reset to zero. Messages from retired boots cannot overwrite the new observation.

A restart with assigned or uncertain material fails the run and latches the whole line. The affected batch remains `UNRESOLVED`; the backend does not claim output, invent a discard quantity, reconstruct contents or silently reload it. `/api/line` exposes `restart_blocked`, `recovery_required` and `failure_reason`; `/api/experiment/status` reports `failed`, `done`, `requires_clean_factory` and `failure_reason`. Resume and new-run requests return 409. Preserve/export that run and start a **clean isolated factory** for another experiment. There is no automatic controller recovery protocol.

The Python inventory is also a RAM mock. Reservation IDs, restock IDs and cancellation tombstones are idempotent only during that process lifetime. A restart reseeds stock and clears its ledger. Inventory restart during a measurement invalidates the experiment assumptions; the backend cannot infer it from a successful stock response. Network timeout retries with the same reservation ID are supported while that process remains alive.

## API and operational details

Global STOP persists its latch, broadcasts non-retained inhibition and sends tracked commands. A stopped heartbeat alone does not satisfy the export barrier: `/line.machines_pending_stop` also includes offline machines and unacknowledged tracked STOP commands. Explicit resume cannot clear an independent local latch.

`TIME_SCALE` means real seconds per simulated hour. The PostgreSQL simulation clock is monotonic across backend restarts. Experiment status exposes absolute `sim_clock_h`, `start_sim_h` and relative `elapsed_h`. `/events` defaults to insertion order using `recorded_seq`; paginate with `after=<sequence>&limit=1000` or stable append-only offsets. `/events` and `/telemetry` accept `order=desc` for recent dashboard views. HTTP mutations accept `Idempotency-Key`; conflicting reuse returns 409. `/api/health` checks initialisation plus actual PostgreSQL and Redis reachability.

Redis uses `REDIS_HOST` and numeric `FACTORY_REDIS_PORT` (default 6379). Avoid `REDIS_PORT`, which Kubernetes may inject as a service URL. `X-Operator` is an audit label; this educational deployment assumes the trusted local network described in the root deployment guide.

## Validation

- `src/backend/tools/check-core.sh`: exhaustive pure domain invariants with Java 21 only.
- `mvn -f src/backend/pom.xml verify`: recipe validation and physical-wire boundary tests.
- Set `TEST_DATABASE_URL`, `TEST_DATABASE_USER` and `TEST_DATABASE_PASSWORD` to enable real PostgreSQL integration tests. Each test creates/drops only its own uniquely named schema. Redis is a deterministic TTL test double in those tests; the root Compose smoke exercises actual Redis and buses.
- Integration cases cover the full six-stage pipeline, two central unitank commands, duplicate outcomes, source discard, receiver interruption, central spoilage, cumulative pauses, late telemetry, active/empty boot changes, Redis expiry, cancellation, guarded command retries and STOP barriers.

The root Compose and Kubernetes smoke/failure harnesses validate actual cross-container behaviour. Unit checks alone do not establish distributed runtime or availability results.
