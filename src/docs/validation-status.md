# Implementation and validation status

Recorded on 28 October 2026 for the equipment-only revision on `feat/beer-factory`. A controller now executes technical instructions; the scheduler owns batch/recipe/phase knowledge. Historical checks of the previous batch-aware controller do not validate this revision.

## Checks of this revision

| Area | Actual check |
| --- | --- |
| Python inventory | All 17 tests passed, including three real HTTP tests. Stock, reservations and idempotency records are process-local RAM. |
| Frontend | Six domain tests, TypeScript compilation and Vite production build passed. Views and command handling are split into documented modules. |
| Deployment tooling | Three tests passed: stable identities without local databases, combined unitank layout, rejection of unsupported formal constructs. Compose configuration and Kustomize smoke rendering passed. |
| Experiment reduction | Eight tests passed. The runner now rejects a controller-restart failure instead of treating the measurement as successful. |
| Formal generation | Regeneration produced no changes to generated models or queries after readability edits. |
| Controller/backend integration | Pending final checks of the new equipment-only protocol; see runtime evidence added after validation. |

## Previous evidence and limits

The previous revision passed one six-stage Kubernetes smoke on 27 October, including global STOP/resume, inventory reservation and Kafka telemetry. Its [recorded evidence](evidence/kubernetes-smoke-2026-10-27.json) is historical: those images still contained controller-side batch tracking and persistence.

UPPAAL 5.0.0 verified the reduced reservation model: all five queries passed; the naive variant failed both exclusivity properties as expected and passed the other three. Raw results and diagnostic traces remain in `src/models/results`. The formal model represents system-level reservations and ownership, not the contents of a controller's memory. These results do not prove every property of the new C/Java implementation.

A browser interaction walkthrough, rollout during active production, 24-hour soak, full 20/40-tank efficiency study and remote GitHub Actions run remain separate evidence work. No throughput measurements are fabricated. Scripts and the experiment instructions describe how to collect them.

## Architectural limitations

Controllers and the Python mock have no persistent storage. A busy controller restart invalidates the run and blocks resume; evidence remains centrally available. An inventory restart resets stock and invalidates a measurement. Use a fresh isolated factory for the next run. Backend/broker interruptions can only reconcile execution that remains present in running controllers.

PostgreSQL, RabbitMQ, Kafka and inventory are single-instance in classroom profiles. This does not establish infrastructure-wide high availability. Physical hardware, brewing chemistry, sensor values, trusted-network authentication omission and ingredient/business data remain declared placeholders.

The group's original `handin.yml` is retained unchanged. The final course report, literature review, contribution record, measured experiments and submission template configuration remain group work; application CI does not complete those requirements.
