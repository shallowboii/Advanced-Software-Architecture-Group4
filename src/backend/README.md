# Production backend

Java 21 / Spring Boot service. Build from repository root with `docker build -f src/backend/Dockerfile .`, or run `mvn -f src/backend/pom.xml verify`. The shared HTTP/MQTT contract is `src/contracts/PROTOCOL.md`.

PostgreSQL owns batches, immutable recipe snapshots, machine assignments, transfer decisions, event deduplication, commands and the Kafka outbox. The scheduler holds a session advisory lock and increments a fencing epoch on handover. Every state transition and its resulting commands commit in one transaction under the factory coordination row lock. Both replicas may ingest events and serve HTTP; only the leader publishes commands, permits and canonical Kafka events. Controllers own timed physical progress and persist their own journals.

Transfers reserve both machines, wait for two PREPARED and two READY events, then commit ownership durably. The source releases first. Only its release acknowledgement permits the receiver commit; a source discard reason forces receiver removal without production. Loss is recorded once as `LOSS_RECORDED`, separately from `FLUSHED`. Snapshots never override ownership. Pending commands replay after restart with the same semantic ID and current epoch; cached transient fencing rejections receive a fresh command ID. Unexpected contents inhibit a machine and raise recoverable evidence rather than creating material.

A Python inventory reservation uses batch ID as its persistent idempotency key. Timeouts leave the same pending admission for retry. Cancellation releases the reservation before clearing the assignment; the inventory service's release tombstone fences delayed requests. A recipe version published while an order runs affects only subsequently admitted unpinned batches.

Global STOP persists a latch, broadcasts non-retained STOP and sends tracked commands. PERMIT cannot reset a latch. A physical global STOP event also latches the backend. Resume supersedes pending STOP retries in that scope. During a broker or coordinator outage, controllers expire their local permits. Redis stores expendable heartbeat snapshots with a six-second TTL; PostgreSQL remains the recovery source. Kafka outages retain canonical events in the outbox, and telemetry consumption commits offsets only after durable insertion.

Environment variables follow the shared contract. `TIME_SCALE` is real seconds per simulated hour. The PostgreSQL simulation clock survives restart; changing policy/run does not reset it. Experiment status reports absolute `sim_clock_h`, `start_sim_h` and relative `elapsed_h`. `/events` exports insertion order using `recorded_seq`; use `after=<sequence>&limit=1000` or stable append-only `offset` pages. Both `/events` and `/telemetry` accept `order=desc` for recent dashboard views; the default is `asc`. Telemetry supports timestamp, machine, batch, run and sensor filters. HTTP mutations accept `Idempotency-Key`; reuse with different input returns 409. `/api/health` checks initialisation and live database access.

Validation:

- `src/backend/tools/check-core.sh` runs exhaustive admission, ownership, terminal and transfer safety guards with only Java 21.
- `mvn -f src/backend/pom.xml verify` runs guard and recipe tests.
- Set `TEST_DATABASE_URL`, optionally `TEST_DATABASE_USER` and `TEST_DATABASE_PASSWORD`, when running Maven to enable PostgreSQL integration tests. Each test creates and drops a uniquely named schema; the database user needs schema creation permission. These tests cover all six stages, immutable snapshots, duplicate deliveries, source-first transfer taint, idempotent HTTP mutations and STOP admission inhibition.
- `RecipesCheck` can validate supplied production recipe files against the actual Jackson validator without running services.

A successful guard test is not a distributed-system acceptance run. Run the root Compose smoke/failure harness and Kubernetes rollout checks to measure broker reconnection, permit gaps and two-replica handover on the target runtime. The REST surface intentionally relies on the project's trusted local network; `X-Operator` is an audit label, not authentication.
