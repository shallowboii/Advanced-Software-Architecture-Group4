# Beer Factory

A distributed brewery simulation with C machine controllers, Java 21/Spring Boot coordination, Python inventory and a React/TypeScript operator interface. MQTT carries commands and critical events, Kafka carries telemetry and canonical history, PostgreSQL stores coordination state, Redis holds the live machine registry, and each machine keeps execution state in RAM. Python inventory is an in-memory mock.

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

Use a separate Compose project for each experiment (`COMPOSE_PROJECT_NAME=beer20 make up PROFILE=20`). Profiles share volume names within a project; changing a profile does not reset persisted recipes or production state. Do not reuse smoke volumes for the full-duration factory. `make down` preserves infrastructure volumes but loses controller and inventory RAM state. Keep these processes running to continue a live run. After a restart, use a fresh isolated factory instead of mixing old coordinator records with reset mock state. The dashboard is the normal interface for orders, recipes, stock, machine controls and global STOP.

`TIME_SCALE=2` means two real seconds per simulated hour. Full Ale tank processing takes 336 simulated hours, Lager 840; the short fixture takes two. This is a process simulation with synthetic sensor values, not physical equipment control. Fermentation progress pauses under STOP as an explicit simulation assumption; ageing continues.

## Operator controls

```sh
python3 src/tools/operator.py status --machine unitank-0 --profile 20
python3 src/tools/operator.py stop --machine unitank-0 --profile 20
python3 src/tools/operator.py resume --machine unitank-0 --profile 20
python3 src/tools/operator.py flush --machine unitank-0 --profile 20
```

These commands use the controller's local socket through container exec, bypassing the application brokers. The dashboard global STOP uses the backend and MQTT. The CLI's `--machine all` uses bounded parallel calls with timeouts and reports unconfirmed machines; it is not a hard-wired emergency circuit. A local stop requires a local reset. Global resume cannot clear a local stop. Restart loses local controller state; if it interrupts assigned or occupied work, the backend blocks the run until a fresh factory is started. A physical flush removes the current contents. The dashboard sends batch-targeted requests to the scheduler, which can reject stale requests before operating equipment.

## Development and verification

Run `make test` with Python 3.13+, a C11 compiler. The production C transport also needs Paho MQTT C, cJSON and librdkafka. Build the backend with `mvn -f src/backend/pom.xml verify` using Java 21. Build the UI with `cd src/frontend && npm ci && npm run build` using Node 22. Docker builds install the component dependencies.

The CI workflow builds and tests all four services, starts a smoke factory and checks one complete batch, STOP/resume, idempotent ordering and persisted Kafka telemetry. It then runs `src/tools/failure_check.py` to exercise controller/backend restarts, MQTT/Kafka outages and loss accounting. Run this only in the isolated smoke factory: it deliberately stops services. Successful non-PR builds publish immutable commit tags to GHCR. It does not automatically deploy to an unspecified cluster. The group’s original `handin.yml` is retained unchanged. Its template placeholders and the final report remain submission work for the group.

- [Shared protocol](contracts/PROTOCOL.md)
- [Architecture and decisions](docs/architecture.md)
- [Kubernetes deployment](deploy/k8s/README.md)
- [Experiments, raw data and comparisons](experiment/README.md)
- [Model transformation and UPPAAL](models/README.md)
- [Implementation plan](IMPLEMENTATION_PLAN.md)
- [Course requirements](docs/course-requirements.md)
- [Validation status and remaining evidence](docs/validation-status.md)

The supplied deployment credentials are classroom placeholders. Authentication and physical hardware adapters are intentionally outside this implementation. The single-instance database/broker/inventory profile does not provide infrastructure-wide high availability.
# Source Code

# Implementation Decisions

## Message Buses

**What do we want?** Two separate message buses:

1. **Production bus** – for the actual production messages (commands to the machines, scheduling, inventory).
2. **Monitoring bus** – for quality control and monitoring of the machines (heartbeats, sensor readings).

We considered 4 types of message buses: **MQTT**, **Kafka**, **RabbitMQ** and **ZeroMQ**.

### MQTT

- MQTT is a lightweight publish/subscribe **protocol**, not a product. To use it you need to run a **broker** (e.g. Eclipse Mosquitto), which acts as a router.
- Clients publish and subscribe to **hierarchical topics**, e.g. `brewery/mashing/heartbeat`.
- The broker delivers each message to the clients that are subscribed to that topic. Messages are not stored for later.
- Docs: https://mqtt.org/

### Kafka

- Kafka is a distributed **event streaming platform**. Messages are stored in a durable log and can be replayed.

> Event streaming is the practice of capturing data in real-time from event sources like databases, sensors, mobile devices, cloud services, and software applications in the form of streams of events; storing these event streams durably for later retrieval; manipulating, processing, and reacting to the event streams in real-time as well as retrospectively; and routing the event streams to different destination technologies as needed.
>
> — [Apache Kafka, Introduction](https://kafka.apache.org/43/getting-started/introduction/)

### RabbitMQ

- RabbitMQ is a traditional **message broker** (AMQP). Producers send messages to an exchange, which routes them into **queues**.
- Consumers acknowledge messages, and a message is removed from the queue once acknowledged. Retries and dead-letter queues are built in.
- Docs: https://www.rabbitmq.com/docs

### ZeroMQ

- ZeroMQ is a messaging **library** with **no broker**. It is as bare-bones as it gets: there is no persistence and no built-in acknowledgement.
- It connects processes directly to each other (peer-to-peer) over TCP or IPC, so each side has to know the other's address.
- Docs: https://zguide.zeromq.org/

## Selected Message Bus: MQTT

| | |
|---|---|
| **Message bus** | MQTT |
| **Integration points** | Between the **per-machine controllers** (publishers) and the **production monitoring subsystem** (machine monitoring and beer monitoring, subscribers), for real-time heartbeats. |
| **Justification of MQTT** | Chosen for its efficiency and fast data exchange. It is optimal for quick processing and delivery of frequent heartbeats from the machines. |
| **Justification of QA** | MQTT promotes **efficiency** (QA-2) by delivering small messages with low bandwidth. Fast heartbeat delivery lets the system detect a failed machine quickly, which supports **availability** (QA-8). |
