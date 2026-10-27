# ASAAT course requirements and evidence checklist

This checklist audits the Beer Factory implementation plan against the available ASAAT materials checked on 27 October 2026. It distinguishes mandatory course requirements from the group's earlier proposed system requirements. A planned task is not completed evidence.

The implementation and actual checks now have a separate [validation status](validation-status.md). The planning tables below retain the original acceptance requirements; use that status document to distinguish implemented code, verified checks and outstanding course evidence.

The original prototype plan covered the software stack and Exercises 6 and 7.1, but did not cover the full course. Missing work included formal modelling and transformation, traceability, continuous deployment and availability evidence, architectural comparison, the complete report and final submission artifacts. These are now assigned in [the implementation plan](../IMPLEMENTATION_PLAN.md).

## 1 Primary source documents

Page numbers below are PDF page positions unless noted. Files are available in the local study vault; copy or link them appropriately for the group rather than making the build depend on this computer's absolute paths.

| Source | Location in the ASAAT vault | Relevant content |
| --- | --- | --- |
| S1 | `materials/Lecture 1 - Introduction/Report and case.pdf` | System requirements p. 8; course requirements p. 9; research questions/submission p. 10; report p. 11-13; individual grading/contributions p. 16. |
| S2 | `materials/Lecture 1 - Introduction/exercise_1.pdf` | Repository/collaborators/pipeline p. 2; use cases and justified assumptions p. 2-3. |
| S3 | `materials/Lecture 3 - QAs, Interfaces, and Patterns/asaat_exercise_3_.pdf` | At least three primary systems, subsystem diagram p. 1; peer review p. 2; pitch p. 3. |
| S4 | `materials/Lecture 4 - Engineering Information Representation. Architecture Design and Description Languages/Lecture 4 exercise further details.pdf` | Scope, rationale, measurable FR/NFR, feature model, analysis/design structural models and requirement traceability, p. 1-2. |
| S5 | `materials/Lecture 5 - Requirement Modelling and Behavior Semantics in ADL/Lecture_5__Exercise.pdf` | Requirement relationships p. 1; FAA/FDA ports/data flows and state-machine execution semantics p. 2. |
| S6 | `materials/Lecture 6 - How Technologies shape Software Architecture/exercise_6.pdf` | Communication p. 1-2; containers and image publication p. 2-3; languages p. 3-5; databases p. 5-6. |
| S7 | `materials/Lecture 7 - Evaluating software architecture/asaat_exercise_7.pdf` | Quantitative QA experiment, goal, scenarios, execution and raw data, p. 1. |
| S8 | `materials/Lecture 8 - Formalization of Behavioral Semantics and Operational Constraints. Model-Checking I/Weekly Note 8.pdf` | Timed automata/transition systems, semantic anchoring and model transformation; detailed exercises to be introduced in class. |
| S9 | `materials/General Course Information/course_plan_v2.pdf` | Subsequent formalisation/V&V and results-analysis lectures. Future exercise sheets need a later checklist review. |

The authoritative course brief calls its minimum implementation an architectural prototype. Our implementation is a working project with explicit hardware/domain placeholders, which exceeds that minimum in functionality. The brief still requires the formal and evaluative deliverables.

## 2 System requirements imposed by the course

| Requirement | Source | Planned implementation and evidence | Status and limitation |
| --- | --- | --- | --- |
| Coordinate information to execute and change production | S1 p. 8 | Orders, central scheduler, MQTT control, Kafka telemetry/events, immutable recipe versions, urgent orders and explicit cancellation/flush; M3-M5 and M8 scenario traces. | Planned. |
| Run 24/7 | S1 p. 8 | Durable state, recovery, dependency isolation, controller health, active/standby coordinator and a 24-real-hour soak with measured availability; M2, M7-M8. | Planned bounded evidence. Single broker/database/inventory instances are explicit single points of failure; a soak cannot prove indefinite uptime. |
| Be continuously deployable | S1 p. 8 | Live backend/frontend rollout, fenced leadership handover, compatible schemas/contracts, machine drain/spare strategy, retained journals; M7-M8. | Application deployment demonstration planned. Uninterrupted updates of all infrastructure require the HA option and actual target infrastructure; not established by a mock overlay. |
| Identify essential quality properties and behavioural requirements | S1 p. 8 | Six-part QAS for correctness, efficiency, recovery, local stop, global stop, monitoring, deployability and modifiability; formal queries plus runtime evidence. | M0 and M6-M8. State numeric thresholds as project choices, not instructor-imposed numbers. |

## 3 Required architecture technologies and methods

All rows in this section come from S1 p. 9.

| Course requirement | Concrete coverage | Evidence to produce |
| --- | --- | --- |
| Different programming languages | C controllers, Java backend, Python inventory, TypeScript frontend; Python tools. | Source/builds, language rationale and actual cross-language execution. |
| Different databases | PostgreSQL durable production records, Redis live cache, SQLite controller journals. | Schemas, migrations, cache reconstruction and restart tests; database trade-offs. |
| Different message buses | RabbitMQ MQTT for control, Kafka for telemetry/history. | Working flows on both brokers, contracts and rationale for the split. One broker alone would not cover the stated plurality. |
| Different containers | Individual machine containers, application services and infrastructure containers. | Compose/Kubernetes manifests, running instances and CI-published application images. |
| Point-to-point communication across languages | Java HTTP client to Python inventory using JSON contracts. | Reservation, conflict, timeout/retry and duplicate-request integration tests. Protocol Buffers is an example, not compulsory. |
| Different architectural styles | Event-driven machine integration, client-server REST/WS, service decomposition and internal backend layers. | Analysis/design views and responsibility/interface definitions. |
| Architectural patterns and tactics | Adapter, state machine, central orchestration, bounded retries, circuit breaker, inbox/outbox, deduplication, heartbeat, leader fencing and drain-before-update. | Implemented instances, named alternatives, QAS links and tests; not just a vocabulary list. |
| Requirement representation and formalisation | Revised FR/NFR/QAS, feature links, SysML/ADL relationships, formal properties. | Traceability table and query/test/result links, M0/M6. |
| Structural and behavioural models | Feature model, FAA/FDA/component/deployment views, machine/transfer/STOP statecharts and interaction sequences. | Source plus vector figures; consistency with code and current scope. |
| Semantic anchoring with model transformation | Defined timed-statechart description transformed into UPPAAL; mapping of states, guards, clocks, channels and effects. | Transformation code, supported subset and semantics, reproducibility test and source-element mapping. |
| Formal analysis and refinement through V&V | Bounded UPPAAL models, queries, actual results, counterexample, design refinement and implementation regression. | Raw verifier output and traces, limitations of abstraction and comparison with implementation behaviour. |

## 4 Exercise deliverables beyond working code

| Deliverable | Source | Plan coverage and completion evidence |
| --- | --- | --- |
| Group repository, every member a collaborator, successful artifact pipeline | S2 p. 2 | M0 checks membership/current repo; M1/M9 replace template echo jobs and validate downloadable artifacts. Membership was not checked in this session. |
| Well-developed use cases and justified assumptions | S2 p. 2-3 | M0: order/change production, blocked transfer, hold expiry, operator flush, STOP/recovery and live deployment; six machine types, 50 hL, no buffers/CIP. |
| At least three primary systems with subsystems and hierarchy diagram | S3 p. 1 | Production Management, Machine Control and Material Supply, including their subsystems and interfaces. |
| Peer review and submission to itslearning | S3 p. 2 | Preserve/find the group's submitted review and record how received feedback changes the design. Existing received peer-review notes do not prove outgoing submission. Group administrative completion must be checked. |
| Three-minute pitch covering needs, approach, benefits and alternatives; slides submitted and presented | S3 p. 3 | Locate/verify existing pitch evidence. Update its architecture summary if reused. Coding does not establish that the presentation/submission occurred. |
| Scope, rationale, measurable FR/NFR, feature model, two levels of structural architecture and traceability | S4 p. 1-2 | M0 documentation and M6 model alignment. Include optional/alternative features where justified; do not invent variability solely for the diagram. |
| Requirement relationships and FAA/FDA ports/data flows | S5 p. 1-2 | M0/M6: appropriate satisfy/verify/derive/refine/trace/copy links and typed provided/required ports. |
| State-machine semantics for at least one critical function | S5 p. 2 | M2/M6: controller and transfer states with triggers, guards, effects, entry/exit behaviour and independently represented stop state. |
| Communication integration points, protocols, justifications and QA effects | S6 p. 1-2 | Contracts, control/data split, error/retry semantics, architecture diagrams and ADRs. |
| At least two containerised components communicating via a containerised bus; CI builds and pushes images | S6 p. 2-3 | Four application images and actual machine/backend broker interaction; CI evidence plus successful pull. PR validation and trusted publication are separate jobs. |
| Language and database choices with trade-offs | S6 p. 3-6 | M0/M9 rationale by system/subsystem, including complexity and resource cost. |
| Select critical quantitative QA, state goal, design scenarios, execute, collect structured raw data and unexpected behaviour | S7 p. 1 | M8: efficiency experiment, pilot, reproducible raw exports, input schedule, manifest and failures log. |
| Timed formal semantics and model transformation | S8 | M6 explicit statechart-to-UPPAAL mapping. Review future lecture 8-10 sheets once available. |

Exercise 7.1 asks for experiment design and collected data; the final report also requires analysis. Neither the exercise sheet nor the case brief mandates exactly 30 runs. The run count is our methodological choice and must be justified by the pilot and source of variation.

## 5 Research questions and final report

| Required research question | Source | Planned answer |
| --- | --- | --- |
| How can different architectures support the stated requirements? | S1 p. 10 | Compare the chosen central, layered, event-driven design with credible alternatives such as controller choreography and a single-bus design. Relate each to QAS; use the PUSH/CONWIP study as evidence for the admission decision, not as a substitute for architectural comparison. |
| What architectural trade-offs follow from technology choices? | S1 p. 10 | Two brokers vs operational cost, controller persistence vs complexity, central scheduling vs availability, fixed-volume simulation vs fidelity, accelerated time vs transport effects, multiple languages vs maintenance. |
| What properties can be formally specified, verified and validated; what results and refinements follow? | S1 p. 10 | Current model transformation, properties, verifier output, counterexample/refinement and runtime regression, including explicit abstraction limits. |

The report uses the supplied GroupTemplate and has at most 10 double-column pages (S1 p. 10-11). A working allocation is abstract/introduction/problem/approach 1.5 pages, related work 1, use cases/QAS 1, design/analysis 2, formal V&V/refinement 1.5, empirical evaluation 2, conclusion/discussion and references within the remaining space. Validate the actual compiled PDF; do not assume references are exempt from the limit.

Required content from S1 p. 11-13:

- Abstract, introduction and motivation.
- Problem, research questions and approach.
- Related work reviewing **8-10 papers**, with a genuine comparison and context for the contribution. Product documentation alone is not this literature review.
- Use case and quality attribute scenarios.
- Design and analysis modelling, patterns/tactics, rationale and trade-offs.
- Formal specification, verification, validation, counterexamples and architecture refinement.
- Empirical evaluation design, measurements, pilot, analysis and connection to the research questions.
- Conclusion, discussion and future work.

The final submission contains PDF plus compilable LaTeX, and a ZIP of the repository with source code, models, requirement queries, usage guide and repository link (S1 p. 10). Include reproducible experiment evidence and tool/version information. Keep vector figures and a contribution log; the course notes individual grading and asks the group to document who does what (S1 p. 16). Verify the final hand-in workflow rather than assuming the template is sufficient.

## 6 Conflicts with earlier Beer Factory documents

These are the group's previous design choices, not additional universal requirements imposed by the course. Revise or explicitly delimit them in the current requirement baseline.

| Earlier statement | Current treatment |
| --- | --- |
| Separate fermentation and maturation machines | One uni-tank, two phases, one uninterrupted occupancy. Six machine types, seven process phases. |
| CIP, dirty/clean states and cleaning contention | Removed by user decision. Old CIP properties/results are historical and must not be reused as current evidence. |
| 12/24 tanks or balanced hundreds of tanks | 20 and 40 profiles for this implementation; existing sizing notes supply background only. |
| Variable physical yields ending at 46 hL | Fixed 50 hL abstraction and explicit complete-batch losses. |
| Trucks, packaging and brewmaster release | Outside system boundary. The filter's completed batch leaves automatically. |
| Authentication FR-1/NFR-9 and verified user identity in audit | Deferred in agreed scope. Record action source/operator label but do not claim authenticated attribution or security compliance. |
| Real closed-loop brewing control and detailed chemistry | Sensor/process simulation with parameterised stages and alarms. Full industrial control remains a placeholder. |
| Any recipe change immediately changes every stage | Immutable active-batch snapshot; new versions affect future admissions according to order selection. |
| Global STOP succeeds within 1 s even with network failure | Local-input stop target is measured independently. Remote/global stop requires delivery acknowledgements; disconnected operation is inhibited by a permit timeout. Hard-wired global circuitry remains a hardware placeholder. |
| Prototype ready once it builds and runs a batch | Working system plus formal, empirical, availability/deployment and submission evidence. |

## 7 Evidence completion rule

For each row above, record an artifact path, test/query or measured result, commit/tool version, reviewer and one of: planned, implemented but unverified, verified, or explicitly limited. Use the actual result, including failed properties or experiments. Do not label the project as satisfying every requirement merely because every item has a place in the plan.

The revised plan accounts for every requirement identified in S1-S8. Remaining verification gaps are the actual implementation and evidence, current remote/group submission state, target-cluster infrastructure and any later course instructions. The strongest unresolved scope issue is uninterrupted operation/deployment of the single-instance infrastructure; the report must either demonstrate the HA extension or state the limited claim precisely.
