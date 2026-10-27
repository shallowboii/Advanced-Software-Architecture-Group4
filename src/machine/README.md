# Machine controller

One `beer-machine` process controls one 50 hL machine. The executable selects `mash`, `lauter`, `kettle`, `whirlpool`, `unitank` or `filter` from `MACHINE_TYPE`. Recipe phases arrive in START or target PREPARE_TRANSFER; fermentation and maturation remain in the same uni-tank. No intermediate storage or cleaning state exists.

## Build and run

From the repository root:

```sh
docker build -f src/machine/Dockerfile -t beer-factory-machine .
```

For a native build, install a C11 compiler, pkg-config, SQLite (with JSON support), Eclipse Paho MQTT C asynchronous library, librdkafka and cJSON, then run `make -C src/machine all`. The Debian Docker build installs these dependencies and runs core tests before linking the production transport. The image runs as UID/GID 10001. Mount `/data` with ownership or Kubernetes `fsGroup: 10001`; each machine must have its own durable volume.

`make -C src/machine check` needs only the compiler and SQLite. It exercises the same production state machine without substituting its persistence layer. `make -C src/machine sanitize` additionally enables AddressSanitizer and UndefinedBehaviorSanitizer where supported.

| Variable | Default |
|---|---|
| `MACHINE_TYPE` | `mash` |
| `MACHINE_ID` | `HOSTNAME`, falling back to `mash-0` |
| `MQTT_URL` | `tcp://rabbitmq:1883` |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | `beerfactory` / `dev-only-password` |
| `KAFKA_BROKERS` | `kafka:9092` |
| `STATE_DB` | `/data/machine.db` |
| `SOCKET_PATH` | `/tmp/machine.sock` |
| `TIME_SCALE` | `2` real seconds per simulated hour |
| `FLUSH_H` | `0.1` simulated hours |
| `HEARTBEAT_MS` | `2000` |

Development credentials belong to the local stack; inject deployment credentials using the environment. IDs may contain ASCII letters, digits, hyphens and underscores. The journal checks its persisted machine identity, and a process lock prevents concurrent controllers using the same SQLite file.

## Simulated physical controls

The local Unix socket is owner-only, independent of MQTT/Kafka connectivity. `machinectl` is installed next to `beer-machine` in `/usr/local/bin`:

```sh
machinectl snapshot
machinectl stop
machinectl resume
machinectl stop --scope global
machinectl flush --batch batch-123 --reason operator
machinectl --socket /tmp/custom-machine.sock snapshot
```

STOP, RESUME and FLUSH enter the same durable handlers as network commands. Snapshot is a read-only health query and does not generate outbox traffic. Global physical STOP sets the local global latch immediately and emits STOPPED with `scope:global`; the coordinator persists the line latch and broadcasts STOP to all other machines. Reset a global line stop through the backend API so it also resets the coordinator latch. A global resume never clears a local operator stop.

The CLI sends newline-delimited JSON and prints its JSON reply. Rejected commands exit nonzero. Flush requires the exact current batch ID, protecting a new batch from delayed operator requests. An accepted flush while stopped is pending until the operator resets STOP and a fresh coordinator permit arrives.

## Durability, timing and transfer ownership

`machine.c` owns all mutations on a single thread. A SQLite transaction commits the snapshot, command result and outgoing critical events together (`WAL`, `synchronous=FULL`). QoS 1 MQTT events replay until an explicit `{event_id}` ACK arrives on the machine ACK topic. Retried command IDs return the original event, including after ACK or restart; conflicting semantic payloads are rejected. Epoch is excluded from the semantic comparison so a new coordinator can retry an existing command safely. New commands are fenced against the current permit epoch. STOP remains conservative even with an older epoch.

Permits are volatile and expire against monotonic time. Duplicate/stale permits cannot extend execution; sim-time references must increase within an epoch. A permit never clears either STOP latch. Work advances only through the portion of a tick covered by a permit. Holding expiry and interruption budgets continue during STOP and restart downtime; ordinary process progress does not advance offline. Checkpoints occur at most every 250 ms between durable transitions. Restart therefore conservatively replays up to 250 ms of simulated work; it cannot replay an already persisted terminal transition. A new boot ID and fresh permit are required after restart. A graceful SIGTERM checkpoints without inventing a physical STOP latch.

Transfer PREPARE reserves both machines, with the source retaining ownership. EXECUTE runs the timed pipe movement at each end; TRANSFER_READY fences both ends pending the backend's durable decision. COMMIT is source-first: the backend waits for source TRANSFER_COMMITTED before committing the receiver. Source COMMIT carries `discard_reason` when an operator flush or expiry raced with the decision. The target accepts that reason and enters FLUSH_PENDING without starting its recipe. Abort-before-prepare with no active reservation persists a tombstone and leaves any material untouched; late PREPARE cannot resurrect that reservation. Unresolved transfers never flush locally without reconciliation. Commit/abort ownership bookkeeping can finish during STOP, but material processing and removal stay inhibited.

Automatic loss reasons are `hold_timeout` and `interruption_limit`; physical flush defaults to `operator`. A source event retains affected batch/transfer identifiers after its snapshot becomes empty. Terminal events and their IDs survive restart; consumers still must deduplicate them. Filter completion atomically emits COMPLETED and empties the last machine, where material leaves this system.

Paho callback threads only enqueue messages; Kafka callbacks only report delivery outcomes. MQTT heartbeat/outbox publication is asynchronous, Kafka telemetry uses a bounded producer queue, and local clients have bounded request/read time. This keeps a disconnected broker from blocking physical controls. Kafka telemetry is best-effort temperature plus density/pH/CO2 simulation; it never determines ownership. Critical event delivery uses MQTT and the durable outbox. See `../contracts/PROTOCOL.md` for the complete wire contract.

## Verification scope

The offline suite covers independent stop latches, permit expiry, duplicate IDs and epoch handover, event ACK/replay, restart interruption expiry, uni-tank phase continuity, final filter output, holding expiry during STOP, commit/discard races, inflight flush/abort and abort-before-prepare reordering. A full stack run additionally requires the transport development libraries or a working Docker daemon and brokers; core tests do not claim to verify broker integration.
