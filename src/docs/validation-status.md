# Implementation and validation status

Recorded on 27 October 2026. Implementation branch: `feat/beer-factory`. This records observed evidence; it does not mark all course acceptance gates complete.

## Passed locally

| Area | Actual check |
| --- | --- |
| C controller | 14 core scenarios compiled with C11 and strict warnings. Durability/replay, fencing, independent stop latches, expiry during STOP, combined unitank phases, filter release, transfer taint, abort ordering, flush idempotency, recipe flush duration and ordered reconnect backlog. Agent also ran UBSan successfully. |
| Python inventory | 14 domain tests: durable idempotency, concurrent reservations, cancellation races, atomic replace failure, corruption and single-writer protection. |
| Java domain | 4,370 exhaustive guard checks using the actual domain class on Java 21. Agent ran 28 recipe checks with actual Jackson and production fixtures. |
| Frontend | Six domain tests, TypeScript compilation and Vite production build. Agent SSR-rendered all six views/forms against the recipe fixture and 20/40 tank counts. |
| Experiment reduction | Eight tests covering deduplication, contradictory outcomes, canonical loss precedence, measurement windows, sampled intervals and seeded pairing. |
| Deployment/model tooling | Three tests for independent machine identity/storage, combined tank phases and rejecting unsupported model semantics. Python compilation succeeded. |
| Configuration | Docker Compose static configuration validation and Kubernetes Kustomize rendering for full/mock profiles succeeded. These are not runtime tests or server-side Kubernetes schema validation. |
| UPPAAL | Rechecked after the user's environment update: verifyta 5.0.0 recognises the licence. Fixed a reserved-keyword collision (`progress` → `process_ticks`). All five queries pass for the reservation model; the naive variant fails both exclusivity properties as expected and passes the other three. Raw results and actual diagnostic traces are saved under `src/models/results`. |

## Remaining validation

- **Container integration update:** access is now available. Fixed a Kubernetes `REDIS_PORT` service-link collision using `FACTORY_REDIS_PORT` and `enableServiceLinks:false`. Rebuilt and rolled out the backend to `kind-beerfactory-test`; both replicas are ready with zero restarts. The real Kubernetes smoke test passed: one 50 hL batch, all six C controllers, idempotent order, global STOP/resume, Python inventory reservation, Kafka telemetry persistence and one terminal completion. Redis contains six live machine keys. [Recorded evidence](evidence/kubernetes-smoke-2026-10-27.json). The failure harness remains unexecuted.
- **Maven/JDBC update:** the actual Docker build now completes Maven dependency resolution and `verify` successfully: seven tests pass and six opt-in JDBC tests are skipped. The runtime successfully migrates and uses PostgreSQL in Kubernetes. The six dedicated JDBC tests still need execution with `TEST_DATABASE_URL`.
- **Inventory HTTP:** the initial two socket-based tests were blocked during `bind` under the earlier sandbox; the 14 domain tests passed. The later Kubernetes smoke confirms actual Java-to-Python reservation, but the standalone HTTP test suite has not been rerun after access changed.
- **Browser walkthrough:** the earlier Chromium launch was denied by the sandbox. No browser walkthrough has been rerun since access changed.
- **Formal scope:** the earlier licence error no longer occurs. The reduced model has now been verified and real counterexamples recorded for the naive variant. This does not establish all planned properties or correspondence with the full C/Java implementation.
- **Deployment/experiments:** the backend repair rollout and one smoke batch have now run on local Kubernetes. A rollout during active production, 24-hour soak, full production study, registry publication and remote CI run remain unexecuted. Scripts and configurations are ready to collect this evidence; there are no fabricated measurements.
- **Remote baseline:** GitHub fetch failed because DNS/network was unavailable. Work is isolated on a local feature branch; remote changes and merge compatibility still need checking when connectivity returns. No implementation push occurred in this session.

## Plan coverage

| Milestone | Current state and acceptance work remaining |
| --- | --- |
| M0 | Local branch, protocol, schemas, fixtures and architecture rationale implemented. Remote baseline and reviewed FR/NFR/QAS/feature/FAA/FDA trace still need completion. |
| M1 | Four services, Dockerfiles, brokers/databases, migrations, health and CI implemented. Real six-stage communication smoke passed on local Kubernetes; CI execution remains. |
| M2 | Controller implementation and 14 core scenarios verified. Real transport, socket/button latency and container restart acceptance awaits integration. |
| M3–M4 | Durable orders, transfer decisions, inventory, policies, versions, stop/flush and cancellation implemented. One full-batch smoke passed on Kubernetes. Dedicated JDBC and failure harnesses remain. |
| M5 | Operator UI implemented, typechecked and built. Browser walkthrough remains. |
| M6 | Reduced timed-statechart transformation, variants, five queries and traceability implemented and verified with UPPAAL 5.0.0. Actual naive counterexamples retained. Broader properties and implementation trace correspondence remain. |
| M7 | Generated Kubernetes deployments and rollout observer implemented. Local kind smoke execution and a repair rollout are recorded. Published immutable images, occupied-machine upgrades and rollout-during-production evidence remain. |
| M8 | Scenarios, seeded schedules, event exports, reduction, paired comparisons, SVG chart and soak observer implemented. Pilot, acceleration calibration, measurements, run manifests and statistical interpretation remain. |
| M9 | Usage/architecture documentation and hand-in packaging workflow implemented. Existing LaTeX remains the course template; the final report, 8–10-paper review, contribution record, PDF/page-limit check and final submission remain. |

## Reproduce on a host with Docker and network access

From the repository root:

```sh
make up PROFILE=smoke
make smoke
python3 src/tools/failure_check.py --output failure-result.json
```

Then run the CI tests with PostgreSQL, the browser walkthrough, a fresh full-duration pilot and the Kubernetes rollout observer. Use a licensed verifyta for `make formal`. Keep failed runs and raw outputs. Tight full-profile holding limits and accelerated time require a measured transport-lag pilot before interpreting PUSH/CONWIP differences.

The classroom profiles intentionally use single-instance PostgreSQL, RabbitMQ, Kafka and inventory. They do not establish infrastructure-wide 24/7 availability or uninterrupted upgrades of those dependencies. Hardware adapters and synthetic sensors, trusted-network authentication omission, and mocked ingredient/business data remain the declared placeholders.
