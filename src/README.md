# Beer Factory

A distributed brewery simulation with C machine controllers, Java 21/Spring Boot coordination, Python inventory and a React/TypeScript operator interface. MQTT carries commands and critical events, Kafka carries telemetry and canonical history, PostgreSQL stores coordination state, Redis caches live views, and each machine persists its journal in SQLite.

Each batch is 50 hL. The production route is mash → lauter → kettle → whirlpool → unitank → filter. Fermentation and maturation share the unitank. Transfers reserve both endpoints; no buffers or cleaning stages exist. Finished filter output leaves the system. Local flush and independent local/global STOP latches are implemented in the controller.

## Run locally

Install Docker with Compose and start its daemon. From the repository root:

```sh
make up PROFILE=smoke
make smoke
```

Open http://localhost:8088. The smoke profile has one machine of each type and shortened phase durations. The full profiles use 20 or 40 unitanks plus the other five machines:

```sh
make up PROFILE=20
make logs PROFILE=20
make down PROFILE=20
```

Use a separate Compose project for each experiment (`COMPOSE_PROJECT_NAME=beer20 make up PROFILE=20`). Profiles share volume names within a project; changing a profile does not reset persisted recipes or production state. Do not reuse smoke volumes for the full-duration factory. `make down` preserves volumes. The dashboard is the normal interface for orders, recipes, stock, machine controls and global STOP.

`TIME_SCALE=2` means two real seconds per simulated hour. Full Ale tank processing takes 336 simulated hours, Lager 840; the short fixture takes two. This is a process simulation with synthetic sensor values, not physical equipment control. Fermentation progress pauses under STOP as an explicit simulation assumption; ageing continues.

## Operator controls

```sh
python3 src/tools/operator.py status --machine unitank-0 --profile 20
python3 src/tools/operator.py stop --machine unitank-0 --profile 20
python3 src/tools/operator.py resume --machine unitank-0 --profile 20
python3 src/tools/operator.py flush --machine unitank-0 --batch BATCH_ID --profile 20
```

These commands use the controller's local socket through container exec, bypassing the application brokers. The dashboard global STOP uses the backend and MQTT. The CLI's `--machine all` uses bounded parallel calls with timeouts and reports unconfirmed machines; it is not a hard-wired emergency circuit. A local stop requires a local reset. Restarting a container or globally resuming cannot silently clear it. Rejected stale flush commands cannot flush a replacement batch.

## Development and verification

Run `make test` with Python 3.13+, a C11 compiler and SQLite development headers. The production C transport also needs Paho MQTT C, cJSON and librdkafka. Build the backend with `mvn -f src/backend/pom.xml verify` using Java 21. Build the UI with `cd src/frontend && npm ci && npm run build` using Node 22. Docker builds install the component dependencies.

The CI workflow builds and tests all four services, starts a smoke factory and checks one complete batch, STOP/resume, idempotent ordering and persisted Kafka telemetry. It then runs `src/tools/failure_check.py` to exercise controller/backend restarts, MQTT/Kafka outages and loss accounting. Run this only in the isolated smoke factory: it deliberately stops services. Successful non-PR builds publish immutable commit tags to GHCR. It does not automatically deploy to an unspecified cluster. The hand-in workflow packages the repository and builds the existing LaTeX report; report authors still need to replace the template and include measured results.

- [Shared protocol](contracts/PROTOCOL.md)
- [Architecture and decisions](docs/architecture.md)
- [Kubernetes deployment](deploy/k8s/README.md)
- [Experiments, raw data and comparisons](experiment/README.md)
- [Model transformation and UPPAAL](models/README.md)
- [Implementation plan](IMPLEMENTATION_PLAN.md)
- [Course requirements](docs/course-requirements.md)
- [Validation status and remaining evidence](docs/validation-status.md)

The supplied deployment credentials are classroom placeholders. Authentication and physical hardware adapters are intentionally outside this implementation. The single-instance database/broker/inventory profile does not provide infrastructure-wide high availability.
