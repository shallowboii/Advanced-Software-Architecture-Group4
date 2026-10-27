# Architecture and decisions

The factory is a collection of independently deployed machine controllers supervised by a durable coordinator. A controller owns its local physical state and enforces stops and time limits; the coordinator owns routing, orders, reservations and the durable transfer decision. A UI disconnect cannot stop a controller's local safety checks.

## Structure

| Component | Responsibility | Durable data / communication |
| --- | --- | --- |
| C machine Pod | One vessel, local buttons, progress, ageing, event replay | SQLite journal, MQTT control/events, Kafka telemetry |
| Java backend replicas | Orders, recipe snapshots, scheduling, transfers, API | PostgreSQL authority, Redis cache, MQTT and Kafka adapters |
| Python inventory | Atomic idempotent ingredient reservations and restock | Atomic file persistence, HTTP/JSON |
| React frontend / nginx | Operator view, controls and live updates | Backend HTTP and WebSocket |
| Experiment tools | Reproducible input schedule and raw observations | HTTP, JSONL/CSV artifacts |

The implementation uses a service-oriented architecture, publish/subscribe for machine observations and direct HTTP request/reply for cross-language inventory allocation. Backend code separates domain rules, persistence, transport and scheduling. These boundaries are architectural decisions, not a claim that every service requires separate teams.

## Key decisions

1. **One machine per Pod.** StatefulSet ordinal and volume preserve machine identity across restart. Six sets represent types; a unitank runs two phases without releasing its batch. `OnDelete` controller upgrades avoid automatic restarts of occupied vessels. Backend/frontend use rolling Deployments.
2. **PostgreSQL as coordination authority.** Reservations, commands, canonical events and outcomes survive process restarts. A database lease and increasing epoch fence coordinator leadership; controllers reject stale commands. Redis is rebuildable and cannot decide ownership.
3. **Source-first durable transfer.** The source retains ownership while both endpoints are reserved and pumping. The coordinator persists COMMIT before issuing it, commits the source, then passes its discard status to the target. Transfer IDs, command IDs and replay prevent retries from creating material. A lost participant remains occupied until reconciliation; timeout is not proof that material disappeared.
4. **Two independent stop latches plus permits.** Local and global resets are distinct. Permit expiry inhibits progress. Simulation clock and ageing continue during STOP, so a spoiled batch must be discarded on resume. A local socket adapter models physical buttons; it is not a certified hard-wired circuit.
5. **Immutable recipe snapshots.** Changes affect future admissions while in-flight batches retain their recipe. Fixed batch volume and one batch per vessel make ownership and waste accounting explicit. Python inventory idempotency resolves retries across the HTTP/database boundary.
6. **Comparable admission policies.** PUSH and CONWIP share dispatch and differ only in admission/WIP control. Experiments preserve actual commands, schedules, recipes, events and provenance. Loss is recorded once independently of later physical flushing.
7. **Critical events versus telemetry.** Critical controller outcomes remain in a SQLite outbox until the backend persists and acknowledges them. Telemetry is synthetic sensor data on Kafka and can be dropped during a broker outage without changing ownership. Canonical batch events originate from the backend's durable outbox.

## Evidence and scope

| Concern | Implementation/evidence location |
| --- | --- |
| Mixed languages and direct interoperability | `backend`, `inventory`, inventory HTTP tests and smoke runner |
| Containerisation and message buses | Four Dockerfiles, generated Compose and Kubernetes profiles |
| Reconfiguration without corrupting active batches | Recipe versions, operator controls, reconfiguration scenario |
| Availability and deployment | Backend replicas, controller journals, `rollout_check.py`, `soak.py` |
| Structural / behavioural descriptions | This document, `architecture.puml`, controller state contract, `models/behaviour.json` |
| Semantic anchoring / transformation | Statechart-to-UPPAAL generator and generated traceability |
| Verification / refinement | Naive and exclusive-reservation model variants, controller tests, raw verifier output |
| Empirical comparison | Seeded paired scenarios, event reduction, intervals and exported chart |

These mechanisms need runtime evidence on a real Docker/Kubernetes environment. Single-instance infrastructure is a documented availability limitation. Formal models prove only their stated abstraction when verification succeeds; they do not certify the entire implementation. Feature/functional decomposition and the course's final 10-page report still require review against the delivered system and actual measured results.
