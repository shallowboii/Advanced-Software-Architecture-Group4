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

## Blocked or not executed

- **Container integration:** Docker CLI exists but no daemon is available. The installed Docker app could not launch. The smoke/failure harnesses, production C transport, full Spring Boot runtime and actual Java-to-Python/MQTT/Kafka flows have not run here.
- **Maven/JDBC:** Maven dependencies are unavailable and network DNS is blocked. An attempted local PostgreSQL initialisation failed at shared-memory allocation under the sandbox. Six JDBC integration tests are configured in CI with a real PostgreSQL service. Temporary external API stubs allowed a syntax check, which is explicitly not a successful dependency build.
- **Inventory HTTP:** both socket-based tests fail during `bind` with `PermissionError: Operation not permitted`. The 14 domain tests pass; the full inventory test suite is not green in this environment. CI runs the HTTP tests without substituting mocks.
- **Browser walkthrough:** Chromium launch was denied by the macOS sandbox. No browser interaction or visual screenshot test is claimed.
- **Formal verification:** model generation succeeds, but verifyta reports `License does not cover verifier.` Actual output and `verified:false` are saved under `src/models/results`. No verified proof or counterexample is claimed.
- **Deployment/experiments:** no real cluster rollout, 24-hour soak, full production study, registry publication or remote CI run has occurred. Scripts and configurations are ready to collect this evidence; there are no fabricated measurements.
- **Remote baseline:** GitHub fetch failed because DNS/network was unavailable. Work is isolated on a local feature branch; remote changes and merge compatibility still need checking when connectivity returns. No implementation push occurred in this session.

## Plan coverage

| Milestone | Current state and acceptance work remaining |
| --- | --- |
| M0 | Local branch, protocol, schemas, fixtures and architecture rationale implemented. Remote baseline and reviewed FR/NFR/QAS/feature/FAA/FDA trace still need completion. |
| M1 | Four services, Dockerfiles, brokers/databases, migrations, health and CI implemented. Real communication acceptance awaits Docker/CI. |
| M2 | Controller implementation and 14 core scenarios verified. Real transport, socket/button latency and container restart acceptance awaits integration. |
| M3–M4 | Durable orders, transfer decisions, inventory, policies, versions, stop/flush and cancellation implemented. JDBC and full-batch/failure harnesses await runtime execution. |
| M5 | Operator UI implemented, typechecked and built. Browser walkthrough remains. |
| M6 | Reduced timed-statechart transformation, variants, five queries and traceability implemented. Licensed verification, actual counterexample and broader trace correspondence remain. |
| M7 | Generated Kubernetes deployments and rollout observer implemented. Immutable images, actual cluster execution, occupied-machine upgrade procedure and live rollout evidence remain. |
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
