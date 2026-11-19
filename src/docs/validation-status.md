# Implementation and validation status

Recorded on 28 October 2026 for the equipment-only revision on `feat/beer-factory`. A controller now executes technical instructions; the scheduler owns batch/recipe/phase knowledge. Historical checks of the previous batch-aware controller do not validate this revision.

## Checks of this revision

| Area | Actual check |
| --- | --- |
| Python inventory | All 17 tests passed, including three real HTTP tests. Stock, reservations and idempotency records are process-local RAM. |
| Frontend | Seven domain tests, TypeScript compilation and Vite production build passed. Views and command handling are split into documented modules. |
| Deployment tooling | Four tests passed: HTTP error handling, stable identities without local databases, combined unitank layout, rejection of unsupported formal constructs. Compose configuration and Kustomize smoke rendering passed. |
| Experiment reduction | Eight tests passed. The runner now rejects a controller-restart failure instead of treating the measurement as successful. |
| Formal generation | Regeneration produced no changes to generated models or queries after readability edits. |
| C controller | 14 equipment scenarios passed with strict compiler warnings; production Docker build also passed the real cJSON wire-parser checks rejecting business metadata and malformed input. The running binary links no SQLite library. |
| Java backend | Maven verify passed all 24 tests, including 15 integration scenarios against PostgreSQL 17.6. The integration fixtures use a Redis test double; real Redis is exercised by container smoke. |
| Full Compose factory | The final six-stage smoke passed with idempotent ordering, global STOP/resume, centrally sequenced unitank phases, output and backend-enriched Kafka telemetry. All four production images built. |
| Kubernetes | The same six-stage smoke passed in isolated kind with two backend/frontend replicas, no Pod restarts during the smoke, six equipment-only controller snapshots and only three infrastructure PVCs. Startup restarts are recorded separately. |
| Failure checks | Passed backend, MQTT and Kafka outages, local STOP/resume, physical flush with exactly one central loss, and occupied-controller restart leaving UNRESOLVED evidence and HTTP 409 on resume. |

[Kubernetes evidence](evidence/equipment-kubernetes-2026-10-28.json) records actual images, physical snapshots and restart counts before and after the smoke.

Actual [runtime evidence](evidence/equipment-validation-2026-10-28.json) records source revision, image IDs, empty controller/inventory mount lists and final central state. The [failure report](evidence/equipment-failures-2026-10-28.json) contains the observations for each injected fault. These checks used an isolated factory; its expected final restart STOP was preserved in evidence before cleanup.

## Previous evidence and limits

The previous revision passed one six-stage Kubernetes smoke on 27 October, including global STOP/resume, inventory reservation and Kafka telemetry. Its [recorded evidence](evidence/kubernetes-smoke-2026-10-27.json) is historical: those images still contained controller-side batch tracking and persistence.

UPPAAL 5.0.0 verified the reduced reservation model: all five queries passed; the naive variant failed both exclusivity properties as expected and passed the other three. Raw results and diagnostic traces remain in `src/models/results`. The formal model represents system-level reservations and ownership, not the contents of a controller's memory. These results do not prove every property of the new C/Java implementation.

A browser interaction walkthrough, rollout during active production, 24-hour soak, full 20/40-tank efficiency study and remote GitHub Actions run remain separate evidence work. No throughput measurements are fabricated. Scripts and the experiment instructions describe how to collect them.

## Architectural limitations

Controllers and the Python mock have no persistent storage. A busy controller restart invalidates the run and blocks resume; evidence remains centrally available. An inventory restart resets stock and invalidates a measurement. Use a fresh isolated factory for the next run. Backend/broker interruptions can only reconcile execution that remains present in running controllers.

PostgreSQL, RabbitMQ, Kafka and inventory are single-instance in classroom profiles. This does not establish infrastructure-wide high availability. Physical hardware, brewing chemistry, sensor values, trusted-network authentication omission and ingredient/business data remain declared placeholders.

The group's original `handin.yml` is retained unchanged. The final course report, literature review, contribution record, measured experiments and submission template configuration remain group work; application CI does not complete those requirements.
