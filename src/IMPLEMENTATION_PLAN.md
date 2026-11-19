# Beer Factory implementation plan

Build a working distributed Beer Production System for ASAAT, including production control, operator actions, persistence, deployment, formal verification and empirical evaluation. Physical equipment and sensor signals are simulated; the services, messages, state transitions and collected results run for real.

This plan incorporates the decisions agreed on 27–28 October 2026 and supersedes the earlier seven-machine prototype plan. The companion [course requirements and evidence checklist](docs/course-requirements.md) maps the implementation and submission work to the course materials. Repository documentation and the application UI use English.

## 1 Agreed scope

| Subject | Decision |
| --- | --- |
| Batch | Fixed 50 hL; one batch per machine; no splitting or blending. |
| Line | Mash tun, lauter tun, kettle, whirlpool with cooling, uni-tank, filter with CO2 dosing. |
| Uni-tank | Fermentation and maturation are consecutive phases in the same machine. There is no transfer between them. |
| Deployment unit | Every machine instance has its own container and Kubernetes Pod. One shared C image is configured by machine type and stable instance ID. |
| Factory size | Default profile: 1 mash, 1 lauter, 1 kettle, 1 whirlpool, 20 uni-tanks, 1 filter. Comparison profile: 40 uni-tanks, other counts unchanged. Small smoke profile: one of each. |
| Buffers | None. A machine holds its finished batch until the next machine accepts it. |
| Connections | Any machine can feed any machine of the next type. Transfers with disjoint endpoints may run concurrently. |
| Transfer | A timed, coordinated operation occupies both machines. Batch ownership changes once at completion. |
| Orders | Operator requests a quantity of a recipe; scheduler admits and executes individual batches. |
| Recipes | Published versions are immutable. Active batches keep their version. A new version can be selected for subsequent batches without redeploying code. |
| Flush | Available at every machine through a local operator input; also available through the supervisor UI and after a hold timeout. Removes the whole batch and records the loss once. |
| Cleaning | No cleaning or CIP subsystem, states, resource queues or cleaning delays. An emptied machine is immediately reusable if not stopped or faulted. |
| Global STOP | Immediately inhibits process progress, transfers and flushing when received. Preserves contents and progress. Monitoring and the simulation clock keep running. Explicit operator reset and resume are required. |
| Output | Completed filtering emits the batch from the system automatically. Packaging, trucking and release approval are outside the boundary. |
| Inventory | Simplified ingredient quantities, reservation and restock. Shortage blocks new admissions, not batches already on the line. |

Physical pumps, sensors and buttons, brewing chemistry and the connection to an external inventory system are placeholders. The controller exposes local input adapters so that simulated button presses exercise the same handlers a future hardware adapter would call. The target cluster is unknown; its storage and exposure configuration remain explicitly labelled MOCK until tested on the actual cluster.

Login remains outside this agreed implementation scope. Local and web actions record their source and supplied operator label, which is not authenticated identity. Earlier project requirements mentioning authentication, CIP, trucks, volume loss or a release gate must be revised, not marked as implemented.

## 2 Process parameters and time

Use the existing ASAAT `machine-line.md` and browser simulator as inputs, with the agreed scope taking precedence. Copy the selected values into versioned recipe fixtures so that the project does not depend on a personal vault at runtime.

| Machine or phase | Ale duration in simulated hours | Lager duration in simulated hours | Maximum wait after finishing |
| --- | --- | --- | --- |
| Mash | 2 | 2 | 1 h |
| Lauter | 2 | 2 | 1 h |
| Kettle | 1 | 1 | 1 h |
| Whirlpool | 1 | 1 | 1 h |
| Uni-tank fermentation | 168 | 336 | Scheduler starts maturation in the same occupied vessel |
| Uni-tank maturation | 168 | 504 | 168 h |
| Filter | 2 | 2 | Automatic output |

The source establishes total tank occupancy of 336 h for Ale and 840 h for Lager. The phase split above is a configurable modelling choice, not a validated brewing claim. It replaces the inconsistent Lager times in the old plan. Start with transfer and flush durations of 0.1 simulated hours each, also explicitly labelled assumptions.

Preserve 50 hL throughout the model. A completed batch produces 50 hL; a discarded batch wastes 50 hL. Do not import the browser simulator's 46 hL packaged-yield calculation.

Maintain two time domains:

- Simulation time determines process progress, transfer duration, holding limits, flush duration and production measurements.
- Real monotonic time determines heartbeat expiry, network timeouts, stop latency, UI latency and deployment measurements.

Persist the simulation epoch and acceleration setting. All components share that mapping. Global STOP does not pause the simulation clock. Process progress is accumulated only while the operation is enabled; wait deadlines continue to advance. A paused in-process batch also has a configurable cumulative interruption budget per recipe phase, initially the corresponding machine's hold limit. Repeated STOP/RESUME must not reset this budget. Pausing fermentation progress is a deliberate simulation abstraction; continuing biological reactions are outside the model.

A ready batch must complete its transfer before its hold deadline. Crossing the deadline marks it spoiled even if a transfer is underway. STOP prevents automatic movement, so spoiled material waits for resume before flushing. Spoiled batches cannot resume production.

Use short recipe fixtures for CI. Scientific runs use the declared recipe durations and a pilot-calibrated acceleration. Do not accelerate so much that normal message latency consumes a substantial portion of the one-hour holding allowance.

## 3 Components and responsibilities

| Component | Implementation | Responsibilities |
| --- | --- | --- |
| Machine controller | C, Paho MQTT, librdkafka, cJSON | Equipment state, scalar duration/temperature operations, sensors, local controls and RAM retry records. No batch, recipe or order knowledge. |
| Production backend | Java 21, compatible pinned Spring Boot release, Maven, JDBC | Orders, recipe versions, scheduler, phase sequencing, expiry, transfer coordination, REST, WebSocket and experiment control. |
| Inventory | Python HTTP service | Ingredient stock, idempotent reservations, restock and reservation status. |
| Frontend | TypeScript, React, Vite, Recharts, nginx | Production dashboard, orders, recipes, machine controls, history and experiment status. |
| RabbitMQ | MQTT plugin | Control commands, machine events, acknowledgements and heartbeat. |
| Kafka | KRaft | Sensor telemetry and replayable production-event stream. |
| PostgreSQL | Relational store with versioned migrations | Authoritative production records, command/event inbox and outbox, transfer decisions, recipes and experiments. |
| Redis | Expiring live machine registry | Heartbeats and equipment observations used by scheduling. Losing Redis does not lose production state. |

Use HTTP/JSON between Java and Python as the required point-to-point, cross-language integration. Specify its request, response and error contracts. Protocol Buffers is an example in the course brief, not a required technology.

The primary systems in the analysis architecture are Production Management (orders, scheduler, recipes), Machine Control (controllers, local inputs, process simulation), and Material Supply (inventory). Monitoring and the supervisor interface connect these systems. This provides the three primary systems requested in Exercise 3 without splitting every Java module into a separate service.

RabbitMQ MQTT supports QoS 1 delivery; application-level deduplication is still required. Use stable client IDs, persistent subscriptions for control traffic, and application acknowledgements after processing; the backend records events durably in PostgreSQL. Never retain executable START or RESUME commands. See [RabbitMQ MQTT documentation](https://www.rabbitmq.com/docs/mqtt).

## 4 State machines and reliable operations

### Machine and batch states

Represent machine operation, stop latch and connectivity separately. An offline machine may still physically contain a batch; OFFLINE must not imply EMPTY.

- Operation: `EMPTY`, `RESERVED_IN`, `PROCESSING`, `READY`, `TRANSFERRING_OUT`, `TRANSFERRING_IN`, `FLUSH_PENDING`, `FLUSHING`.
- Stop latch: local and global causes, held for the controller process lifetime. A stop retains the underlying operation and remaining work.
- Connectivity: `ONLINE`, `STALE`, `OFFLINE`, derived from last receipt time and controller session.
- Batch lifecycle: `ADMITTED`, `PROCESSING`, `WAITING_TRANSFER`, `TRANSFERRING`, `SPOILED`, `FLUSH_PENDING`, `DISCARDED`, `COMPLETED`, `RECOVERY_REQUIRED`.

The backend stores fermentation/maturation phase information while the unitank stays physically occupied. It sends one scalar technical operation at a time. Terminal batch outcomes are immutable. Spoiling and flushing are separate events: loss is booked once when the batch becomes irrecoverable; later removal must not book the loss again.

Each controller keeps technical occupancy, operation progress, equipment reservation, stop latches, command retry records and pending observations in RAM. It knows no batch, recipe, order, run or business transfer ID. The scheduler tracks all material and phase information centrally. There is no local database, checkpoint or volume. A new process starts EMPTY with a new boot ID and no valid permit. If the previous process was occupied, assigned or had outstanding material commands, the backend stops the line and rejects resume. This interrupted simulation requires a fresh factory; missing beer is never recreated from a database snapshot.

### Message contracts

Machine MQTT/Kafka envelopes use schema version 2. They carry machine identity, controller boot ID, technical state/version, command or event correlation, and timestamps. They never carry batch, execution, transfer, recipe, run or named phase fields. The backend enriches observations using its durable actuator-command records before producing business events or REST views.

Commands contain scalar duration/temperature, direction or stop scope as appropriate. Command IDs support transport retries; boot and technical-state guards reject delayed operations. A local physical flush empties current contents without a batch argument. Dashboard batch-targeted flush requests are validated by the scheduler before sending an actuator command.

| Channel | Purpose |
| --- | --- |
| `factory/<type>/<id>/heartbeat` | Full current-state summary every 2 real seconds. |
| `factory/<type>/<id>/cmd` | Targeted commands and transfer protocol steps. |
| `factory/<type>/<id>/events` | State transitions and command results retried from RAM. |
| `factory/<type>/<id>/ack` | Backend acknowledgement that machine events were recorded. |
| `factory/line/control` | Global stop notification and versioned control state; reconnect always reconciles with durable state. |
| Kafka `telemetry` | Sensor samples keyed by machine ID. |
| Kafka `batch-events` | Canonical production events keyed by batch ID. |

Use a PostgreSQL transactional outbox for state change plus command/event publication, and an inbox with unique event IDs for deduplication. Controllers retain critical events in RAM until the backend acknowledges their durable receipt; this queue is lost on process restart. Kafka telemetry may have recorded gaps; critical production events must be replayable without changing outcomes. Stop response must not wait for Kafka or a backend acknowledgement.

### Transfer protocol

1. Lock the source batch and both machine assignments in one database transaction. Confirm adjacent types, source readiness, empty receiver, live sessions and no stop latch. Create a unique transfer record.
2. Send PREPARE to both machines. Each holds its reservation in RAM and responds. Start only after both are prepared.
3. Send technical EXECUTE commands correlated centrally to the same transfer record; do not send its business ID to machines. Both machines stay occupied for the transfer duration. In the simulation, the source remains the logical owner until commit; partial litres are not modelled.
4. Once both report transfer completion, record a single durable COMMIT decision, switch ownership in PostgreSQL, and publish the decision through the outbox.
5. The receiver becomes occupied and READY after COMMIT; the scheduler then issues PROCESS for the next recipe step. The source may become available only after acknowledging its release. Retry each unacknowledged protocol step with the original IDs.

After a backend/connection interruption, replay the stored decision against the same controller boot IDs and reconcile live snapshots. A busy controller process restart instead invalidates the run and latches STOP; no local recovery exists. A timeout is not proof that a transfer failed. Never free endpoints merely because an acknowledgement is missing. Before a commit decision, an operator flush or spoil event can abort the transfer and discard its batch. After commit, discard is routed to the current owner. Both endpoints remain fenced until reconciled; the loss is recorded once. A physical flush during transfer requests removal and inhibits that operation until the scheduler settles the equipment transfer; the controller does not decide batch ownership or loss accounting.

### STOP and local operator controls

Implement one priority stop handler in the C controller. A local socket adapter simulates physical button input independently of the backend, MQTT and Kafka. Provide `machinectl stop`, `machinectl flush` and explicit reset commands through that socket. A future GPIO adapter calls the same handlers.

The red global STOP first durably latches the backend and inhibits scheduling, then broadcasts and sends tracked stop commands to every commissioned machine. Show acknowledgement per machine; an unreachable machine is UNKNOWN, never falsely confirmed stopped. A separate operator harness can invoke local stop handlers through Compose/Kubernetes exec while the application backend or broker is down. This models local inputs, but it is not an implemented hard-wired factory circuit.

Controllers require a short, periodically renewed run permit. If control connectivity fails, the permit expires and inhibits operation independently of receiving STOP. Use a 3 real-second permit as an initial engineering default; local stop targets <=1 second, while disconnected global stop is bounded by permit expiry. Stop latches survive reconnection during the same process lifetime. Fresh permits alone cannot clear them. Process restart loses local state; the backend blocks an interrupted run. Resume requires explicit reset, state reconciliation, fresh permits and disposition of spoiled batches before healthy operations continue.

This separation must be reflected in the report: the local stop latency and network/global stop latency are distinct measurements. The previous blanket claim of <=1 second global stop without a bus is not established by software broadcast.

### Recovery and state ownership

PostgreSQL owns durable coordination and production history. Redis holds the live machine registry with heartbeat TTL; scheduling requires fresh live state. Controllers own execution in RAM. Commands target a controller boot ID so queued commands cannot start material in a replacement process. A missing heartbeat does not mean EMPTY. A busy controller restart stops the run and preserves central evidence.

Inventory reservations use batch ID as the idempotency key. A timed-out reserve request is queried or retried with the same key. Keep stock, reservations and idempotency records in RAM under a lock, using the Python standard library. The mock resets to initial stock after restart, invalidating the experiment; start a fresh isolated factory before a new measurement. Cancellation before admission releases the reservation once; flushing an admitted batch does not refund ingredients.

## 5 Scheduling and recipe changes

Run one active scheduler. Both policies use the same transfer dispatch rules and machine selection; only admission differs.

- PUSH admits the next queued batch whenever a mash tun and inventory are available.
- CONWIP also requires active WIP below a configured cap. WIP includes all admitted batches until output or confirmed removal, including stopped, transferring and spoiled-but-not-removed batches. Start with cap equal to the commissioned uni-tank count, 20 or 40; record the cap independently from machine count. A capacity reduction blocks further admission without cancelling existing work.
- Dispatch ready transfers by nearest holding deadline, then ready time and batch ID. Use a deterministic machine-ID tie break.
- Orders use FIFO by default, with explicit priority for the urgent-order scenario. No implicit preemption of occupied machines.
- Recipe edits publish a new version. On batch admission, resolve and persist the version selected by the order. Orders default to the latest published version at admission; an explicitly pinned order remains pinned. The UI must show that pending admissions can use a newly published version. Active batches never change.
- Operator cancellation of an admitted batch requests its flush. It does not silently create a replacement batch. The order displays completed, discarded and outstanding counts; replacements are an explicit order amendment.

At 20 tanks, ideal Ale cellar capacity is 20/336 batches per hour, approximately 1.43 batches or 71.4 hL per day before transfer and stoppage overhead. At 40 it doubles. These are theoretical capacity bounds, not promised experiment results. A fully balanced 336-tank factory is outside the selected scale.

## 6 Storage and application interfaces

Use versioned database migrations. Initial entities: `recipe`, `recipe_version`, `production_order`, `batch`, `machine_assignment`, `stage_execution`, `transfer`, `line_control`, `coordinator_epoch`, `command`, `event_inbox`, `outbox`, `production_event`, `telemetry`, `alarm`, `experiment_run`.

Raw controller telemetry contains technical machine, boot, command correlation, sensor, unit, value and timestamps only. The backend enriches persisted telemetry with batch, phase, recipe and run from its own command records. Batch/run identifiers must not be lost during persistence. Enforce exclusive machine occupancy and one active location/transfer per batch with database constraints and transactions.

| API group | Operations |
| --- | --- |
| Recipes | List, create, publish a new version, inspect prior versions. |
| Orders | Create quantity/recipe/priority, inspect progress, cancel unstarted quantity, explicitly amend outstanding demand. |
| Machines | List live and last-known state, inspect history, stop/reset a machine, request a batch-targeted flush. |
| Line | STOP, reset/resume, inspect per-machine acknowledgement and unresolved recovery. |
| Batches | Inspect lifecycle, recipe snapshot, location, interventions and terminal outcome. |
| Telemetry | Query by machine, batch, run, sensor and time window; paginate/aggregate history. |
| Inventory | Read stock, restock; internal reserve, reservation lookup and release operations. |
| Experiments | Start isolated run, inspect progress, stop recording, export events and manifest. |
| WebSocket | State, telemetry, alarm, order, batch and line-control changes, plus snapshot/resync after reconnect. |

REST mutations accept idempotency keys. State conflicts return an explicit conflict response. An open inventory circuit breaker blocks admission and exposes the reason; it does not fail unrelated monitoring or in-flight production. Use bounded retries with backoff for network operations.

The frontend shows six stage columns, a compact grid of 20/40 uni-tanks, phase/progress, held batches and deadlines, active pipe transfers, live/history charts, order and recipe controls, stock/restock, manual flush, a prominent global STOP, explicit resume and the experiment dashboard. Flush controls identify the affected batch. Offline status and stopped status remain visually distinct. A displayed zero-waste result must distinguish no completed production from efficient production.

## 7 Availability and deployment

Use six StatefulSets with one application container per machine Pod, stable ordinal IDs and headless Services. Controllers have no persistent volumes: StatefulSets provide identity only. Compose also gives each controller an explicit stable ID. PostgreSQL and brokers retain infrastructure volumes.

Provide Kustomize profiles for smoke, 20-tank and 40-tank factories. The mock-cluster overlay contains replaceable exposure, storage and resource settings. Pin application images by commit digest/tag and third-party image versions. Add startup/readiness/liveness checks, resource budgets, shutdown handling and broker/database persistent storage. Do not restart a healthy process merely because a dependency is unavailable.

Implement two backend replicas for the Kubernetes deployment profile, with a PostgreSQL advisory lock for scheduler leadership and a monotonically increasing coordinator epoch. On lock/session loss the old leader stops publishing. Controllers fence stale epochs, and the new leader reconciles live controller sessions and pending commands before admission. Readiness, handover and permit renewal must allow a normal backend rollout without stopping production. Each API replica can reconstruct current state and resynchronise its WebSocket clients.

Use compatible additive migrations and backwards-compatible message versions during application rollouts. Frontend rolling deployment must not affect production. Controller upgrades drain selected machines before replacing their Pods; for the one-instance brewhouse stages, upgrade during an empty interval or temporarily add another machine and route new work to it. Do not automatically roll or scale down occupied machines. In the long-cycle tank stage, wait for draining or explicitly schedule the interruption rather than disguising it as continuous operation.

The default classroom infrastructure uses a single PostgreSQL primary and single broker instances. That is an explicit availability limitation. M0 must define the availability/deployment boundary for each component, and M7 must document a concrete extension for replicated brokers, managed/HA PostgreSQL, Redis reconstruction/failover and inventory handover with preserved reservations. If the claim includes uninterrupted upgrades of these dependencies, implement and test that extension before marking the requirement verified. It cannot be claimed demonstrated by the mock overlay. The availability evidence must name which components were rolled, which failures were injected and which dependencies remained single points of failure.

Initial measurable QAS targets below are engineering choices to validate, not numbers imposed by the course. Each gets the six-part source/stimulus/environment/artifact/response/measure description in M0.

| Scenario | Acceptance target |
| --- | --- |
| Duplicate delivery or transfer recovery | Zero duplicate stage executions, duplicate terminal outcomes or conflicting batch owners in the injected-failure test matrix. |
| Local stop input | Controller inhibits operation within 1 real second in 20 trials, including backend/broker disconnection. |
| Remote global stop | Every reachable machine acknowledges within 1 real second under the declared test load; disconnected controllers inhibit on the 3-second permit timeout. Report each path separately. |
| Lost heartbeat | Mark a machine offline within 7 real seconds of the last received heartbeat, with the selected 2-second interval and 6-second expiry. |
| Live dashboard | State/sensor changes appear within 2 real seconds in the defined browser/load test. |
| Planned backend rollout | Zero lost/duplicate production transitions and zero permit-expiry production stops during the tested handover; record API/WS interruption separately. |
| Recipe or machine integration | Add a recipe through data or another supported machine through configuration, with no controller/scheduler code change. |
| Unattended operation | Complete a 24-real-hour soak without an unplanned software-caused production stop; report measured downtime, not just process existence. |

## 8 Implementation sequence

Each milestone leaves runnable evidence and updates the requirement trace. Documentation and models evolve alongside the code.

| Step | Work and outputs | Completion gate |
| --- | --- | --- |
| M0 Establish baseline and contracts | Fetch the current group repo, preserve group work, create an implementation branch. Record agreed scope, revised FR/NFR/QAS, message schemas, state transitions, recipe fixtures and analysis/design component views. | Course matrix has no unassigned item; no CIP or separate maturation machine appears in the current design; sample contracts validate. |
| M1 Running communication skeleton | Four application images, Compose, RabbitMQ MQTT, Kafka, PostgreSQL, Redis, migrations and health checks. One C machine sends heartbeat and telemetry to the Java backend and UI. Java calls Python inventory. | Real container-to-container command/reply, telemetry persistence, WebSocket update and Java-to-Python reservation all work. Establish build/test CI now. |
| M2 Controller and local controls | Six equipment configurations, scalar operations, RAM deduplication, simulated sensors, local stop/flush adapter, permits and explicit restart limits. | Duplicate START does not run twice; busy controller restart stops the run without recreating contents; local STOP works with backend/brokers down; expiry does not move material while stopped. |
| M3 Orders and durable coordination | Recipes/versions, orders, admission, in-memory inventory reservation, transactional inbox/outbox, two-end transfer and recovery, automatic filter output. | One 50 hL batch traverses all six machine types once. Connection/backend failures reconcile against the same controller processes without duplicate outcomes. Controller state loss stops the run. |
| M4 Production policies and interventions | PUSH/CONWIP, deadline-based transfers, hold expiry, order cancellation, local/web flush, global STOP/reset/resume, shortage/restock. | Ten short-fixture batches; shortage stops only admission; recipe changes leave active batches intact; multi-machine STOP and transfer-time flush pass fault tests. |
| M5 Complete operator UI | Stage/tank view, recipes/orders, telemetry/history, stock, batch trace, flush/STOP/resume controls and recovery visibility. | Browser walkthrough covers a complete order, new recipe, cancellation, local intervention, loss of heartbeat and reconnection. |
| M6 Formal model and transformation | Current structural/behavioural descriptions, machine-readable timed-statechart subset, PlantUML/UPPAAL generation, queries, naive/refined variants and counterexample-driven test. Start the reduced model in M0; align it with M2-M4 here. | Reproducible model generation and verifier results, explicit abstraction assumptions and at least one real refinement trace. |
| M7 Kubernetes and live deployment | Six StatefulSets with RAM controllers, central infrastructure storage, 20/40 profiles, mock overlay, backend leader handover, immutable published images, controlled controller drain and rollout scripts. | Render and schema validation plus a real local Kubernetes smoke run; backend/frontend rollout during production; empty-machine recreation with a new boot ID and busy-machine restart rejection. Actual target-cluster validation remains separate until access exists. |
| M8 Experiments and availability evidence | Scenario runner, pilot, raw-event export, CSV reduction, statistical/graph scripts, accelerated production studies, measured stop/recovery/rollout scenarios and a real-time soak. | Every result is reproducible from raw files and a run manifest; invalid/incomplete runs are retained and labelled; no predetermined PUSH/CONWIP winner. |
| M9 Report and submission | Architecture trade-offs, formal results, empirical analysis, 8-10-paper related work, contribution record, reproducible usage guide, PDF/LaTeX and repository ZIP. | Clean-machine reproduction, <=10-page double-column PDF, archive includes code/models/queries/data and final hand-in workflow passes. |

The local checkout inspected for this plan is still the initial template commit. `git fetch` was blocked by DNS/network access in this session. M0 must inspect current remote content before implementation; the plan does not assume the remote is empty.

## 9 Formal verification and traceability

The existing `beer-tanks-cip.xml` is historical input. Do not report its old nine query results as verification of the new system. Create a new model without CIP, with a combined tank, blocking transfers, holding limits, stop latches and explicit discard.

Model the critical subset with two or three machines/tanks and a bounded number of batches. Use a documented machine-readable timed-statechart subset as the architectural behaviour description. Generate PlantUML and UPPAAL from it, including locations, guards, clocks, synchronisation and effects. Reject unsupported constructs. Map each generated element back to its architectural element and requirement. This is a defined model transformation with semantic anchoring, not just two unrelated diagrams.

Required properties and evidence:

- Exclusive batch ownership and machine occupancy, including transfer reservations.
- Stage ordering and no change of recipe version after admission.
- STOP prevents process/transfer/flush progress until reset/resume.
- A duplicated/stale command cannot repeat a stage or terminal outcome.
- No spoiled batch is released as completed output.
- Reachability sanity checks, deadlock freedom and conditional progress under stated assumptions about eventual resume, connectivity and available inventory.
- Bounded local stop response in a separate real-time model; do not mix accelerated process hours with network milliseconds.
- A deliberately naive transfer/reservation variant and a refined variant: save an actual counterexample, the resulting design change and its verifier outcome. Turn the trace into an integration regression test.

Maintain `requirement -> feature -> FAA/FDA element -> statechart element -> query/test -> result`. Include appropriate `satisfy`, `verify`, `deriveReqt`, `refine`, `trace` and `copy` relationships with their meanings. Small-model results apply to that abstraction; they are not automatically a proof for 40 tanks or the C/Java implementation. Runtime invariant checks validate the implementation traces against the same ownership and transition rules.

Keep verifier version, invocation, parameters, raw output and counterexample files. Use the installed/licensed UPPAAL environment for verification; enable hosted CI verification only if its licence/runtime setup is supported. CI must always check generated-model freshness and report whether formal verification actually ran.

## 10 Experiment design

Goal template: analyse the production admission policy for the purpose of evaluation with respect to throughput and material loss from the plant manager's perspective in the context of the distributed, simulated Beer Factory. The architecture decision under test is central admission with bounded WIP versus unrestricted admission under the same physical layout and dispatch rules.

Predeclare the primary comparison as scheduler-induced wasted hL under CONWIP versus PUSH, with completed batches/day as a co-reported trade-off. For stochastic trials, H0 is no difference in expected wasted hL; the directional hypothesis is lower waste under CONWIP. Do not claim an efficiency improvement from waste alone if output falls substantially. Freeze a throughput trade-off criterion after the pilot and before the main runs, and report both effects even if the hypothesis fails.

Main factors: policy PUSH/CONWIP and uni-tanks 20/40. Freeze recipe definitions, other machine counts, transfer duration, hold limits, inventory availability, demand and acceleration within each comparison. Set and record WIP cap; do not conflate it with the machine count in result files.

Scenarios:

1. Stable Ale demand with sufficient inventory. Establish the capacity and blocking baseline.
2. A declared change from Ale to a mix containing Lager and a newly published recipe version. Existing batches finish unchanged; compare backlog, throughput and deadline losses.
3. An urgent order plus explicitly selected operator cancellations/flushes and a timed STOP. Forced operator loss is reported separately from scheduler-induced spoilage. No cleaning/changeover time is introduced.

Start with a 42-simulated-day warm-up and 90-day measurement window so that the 35-day Lager tank cycle is observable. Report startup and measured-window metrics separately; record WIP/backlog at the boundary and right-censor unfinished orders instead of counting them as zero-latency completions. Keep the warm-up policy identical to the measured policy and inspect pilot curves before calling the window steady state. Recipe changes in a scenario are intentional transients and must be analysed as such.

Use deterministic scenarios first. If repeated-run inference is needed, introduce explicitly artificial, seeded variation in arrivals and/or operation durations. Generate the complete exogenous input schedule before each paired comparison so policies consume identical inputs even when their execution orders differ. Random sensor display noise alone does not justify 30 repeated production trials.

Run a small pilot for all scenarios and factor combinations. Calibrate acceleration, scheduler lag, sample rate, variance and storage volume. Then freeze the protocol and run 30 paired seeds for the selected main scenario, with the other scenarios as documented supplementary experiments. If outcomes are deterministic, report deterministic comparisons and sensitivity analysis instead of a spurious significance test. Estimate total runtime from the pilot before starting the full matrix.

Collect raw `events.csv`, `machine_intervals.csv`, `orders.csv`, the input schedule, recipe snapshots, and `manifest.json` with run ID, seed, Git SHA, image digests, topology, policy, WIP cap, time scale, environment, resource limits and measurement boundaries. `summary.csv` must be reproducible from these files. Use PostgreSQL canonical production events/outbox IDs as the authoritative export; compare Kafka replay by event ID to verify transport completeness. Live REST counters are projections, not the sole scientific dataset.

Metrics: completed batches per simulated day, output hL, spoiled hL, operator-discarded hL, unresolved-loss count, order lead time, backlog, terminal WIP, and per-machine processing/transfer/wait/stopped/offline time fractions. Validate material balance: admitted volume equals completed, irrecoverably lost and still viable in-process volume, without counting spoiled-but-not-yet-flushed contents twice.

Every run uses an isolated namespace or Compose project, stable IDs and fresh run-specific volumes. Preserve artifacts before teardown. Do not reset the user's demonstration factory. Record failed runs and reasons rather than silently replacing them with successful ones.

Additional architectural measurements: local and global STOP latency, heartbeat detection latency, UI delay, duplicate/reorder recovery, backend deployment handover and a 24-real-hour unattended soak. Accelerated days are not evidence of 24/7 service availability. Log availability, software-caused stoppages, data gaps and recovery time during the soak. A single successful soak is bounded evidence, not proof of indefinite uptime.

Analyse paired differences and uncertainty when stochastic trials justify it. Report validity threats: simplified physics and fixed volume, artificial variability, machine-resource contention, transport latency under acceleration, single-node dependencies, small formal-model bounds and local-versus-target-cluster differences. Explain how the measured results answer the course research questions.

## 11 Repository layout and automation

```text
src/
  IMPLEMENTATION_PLAN.md
  README.md
  contracts/                  JSON schemas and cross-language fixtures
  machine/                    C controller, local operator CLI, tests, Dockerfile
  backend/                    Java services, migrations, tests, Dockerfile
  inventory/                  Python service, RAM stock mock, tests, Dockerfile
  frontend/                   React application, browser tests, nginx, Dockerfile
  config/                     Recipe and topology profiles
  deploy/compose/             Base configuration and generated stable-ID profiles
  deploy/k8s/                 Base and smoke, 20, 40, mock-cluster overlays
  tools/                      Profile generation, operator harness, smoke/rollout checks
  models/                     Behaviour source, transformation, PlantUML, UPPAAL, queries
  experiment/                 Runner, reducer, analysis, scenarios, results
  docs/                       Scope, use cases, QAS, architecture, ADRs, traceability, runbooks
report/GroupTemplate/          Course report and bibliography
.github/workflows/            Build/test/publish and final hand-in
```

Keep application and experiment source under `src`, as required by the repository template. A root Compose file or Makefile can delegate into it.

CI on every PR: schema checks, meaningful C/Java/Python tests, TypeScript build, generated-model checks, four image builds, complete Compose smoke test, manifest validation and report compilation when relevant. No `continue-on-error` on required correctness gates. Upload logs and test artifacts on failure.

Publish four images to GHCR from trusted branch pushes after checks, using unique commit tags and `packages: write`; keep untrusted PR validation read-only and do not publish `latest` from PRs. Resolve the actual repository-owner/package permissions in M0. The publication mechanism follows [GitHub's container publication guidance](https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images). Test that deployment pulls the published images. Publish both required CPU architectures when the chosen local and cluster platforms differ.

Leave the group’s `handin.yml` unchanged, as required by the original plan. Application build/test/publish belongs in `cicd.yml`. Final submission preparation remains a separate group task. The ZIP includes source, contracts, migrations, model sources, transformation code, queries, verifier evidence, experiment data/analysis, usage guide and compilable LaTeX. Do not include credentials or runtime volumes.

## 12 Acceptance and outstanding external inputs

Completion requires all of the following:

- A clean checkout can start the smoke factory, accept an order and complete all six machine types, including both uni-tank phases.
- The 20/40 profiles run with individual machine Pods and correct exclusive occupancy.
- Local/web flush and global STOP work in the agreed states, including an in-flight transfer, without duplicate output/loss.
- Backend restart/reconnect and command retries preserve coordination while controller processes remain alive. Busy controller restart stops the run; it does not recover local execution.
- Inventory exhaustion/restock and new recipe versions work without redeploying or changing active batches.
- Kubernetes application rollout is demonstrated during production, with the observed interruption and remaining infrastructure limitations recorded.
- Formal model generation, query results, a counterexample/refinement and corresponding runtime validation are reproducible.
- Raw experimental evidence, analysis, usage guide and the course report/submission artifacts exist.
- Every course requirement in the companion checklist has an artifact and a truthful evidence status.

Remaining external inputs are target-cluster access/capacity/storage/exposure, current remote-repository state, group collaborator/package permissions and any course instructions published after the materials checked on 27 October. These do not block contracts, local implementation or model work. They do block claiming target-cluster deployment or complete submission verification.
