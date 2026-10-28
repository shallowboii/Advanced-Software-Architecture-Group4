# Equipment controller in C

One process simulates one physical machine. It knows its identity, occupancy, actuator state, temperature setpoint and timer. The six configured equipment types are `mash`, `lauter`, `kettle`, `whirlpool`, `unitank` and `filter`.

The controller does **not** know batches, orders, recipes, production phases or material ownership. The main scheduler owns those concepts. For example, fermentation and maturation are two scalar operations sent to the same occupied uni-tank; the controller neither names them nor decides when the second should start. Holding limits and spoilage decisions also belong to the scheduler.

All execution state, command deduplication and pending critical messages live only in RAM. There is no database, journal, checkpoint, state directory or persistence substitute. A process restart starts `EMPTY`, resets counters and stop latches, creates a fresh `controller_boot_id`, and requires a fresh permit. It does not recover an interrupted operation or remembered contents. The backend detects this boot change and handles the failed production run; an empty snapshot is not evidence that a physical restart safely recovered material.

## Reading the implementation

| File | Responsibility and useful starting points |
|---|---|
| `machine.h` | Technical state and command structures; no production entities. |
| `machine.c` | `bf_command` handles deduplication and guards, then dispatches to small actuator handlers. `bf_tick` advances only equipment timers. |
| `wire.c` | `bf_parse_command` validates schema version 2 and rejects every unknown field. |
| `main.c` | `service_cycle` handles physical input, queued MQTT input, timer updates and asynchronous publication. |
| `machinectl.c` | Sends one physical-input simulation command through a local Unix socket. |
| `tests/test_core.c` | Deterministic equipment behavior without external services or libraries. |
| `tests/test_wire.c` | Production JSON parser tests, including rejection of business metadata and legacy commands. |

Every function has an English comment describing its role or invariant. `.clang-format` keeps the C layout consistent. The core is single-thread-owned: MQTT callbacks only enqueue input under a mutex, and Kafka callbacks only report delivery outcomes.

## Commands and observations

Commands use `schema_version: 2`. A network actuator request includes `command_id`, `machine_id`, `epoch`, and `expected_state_version`; the backend also binds it to `controller_boot_id`. STOP remains conservative and may inhibit equipment regardless of an old epoch/boot. Local physical actions act on the equipment's current contents and do not require a scheduler version.

| Command | Equipment behavior |
|---|---|
| `START` | On `EMPTY`, load and run one `duration_h` / `target_temp_c` operation. |
| `PROCESS` | On occupied, unreserved `READY`, run another scalar operation. |
| `PREPARE_TRANSFER` | Reserve a source or receiver using only `role: source\|target`. |
| `EXECUTE_TRANSFER` | Run the reserved pipe for `duration_h`, then report `TRANSFER_READY` and wait. |
| `COMMIT_TRANSFER` | Empty the source or leave the receiver occupied and `READY`; never automatically start processing. |
| `ABORT_TRANSFER` | Cancel a reservation; source keeps contents and receiver returns empty. |
| `RELEASE` | Explicitly empty occupied, unreserved `READY` equipment, including final output. |
| `FLUSH` | Request removal of current contents; optional `duration_h` overrides `FLUSH_H`. |
| `STOP` / `RESUME` | Set/reset one local or global stop latch. |
| `PERMIT` | Renew a short execution lease; never clears a stop latch. |
| `SNAPSHOT` | Return equipment state. Local health queries are read-only and create no event traffic. |

An operation timer emits `PROCESS_COMPLETED` and leaves the machine occupied in `READY`. Only the scheduler can send the next `PROCESS`, transfer or `RELEASE`. A physical flush during an unresolved pipe operation emits `FLUSH_REQUESTED` and waits for reconciliation. If a source is released after that request, its `TRANSFER_COMMITTED` event carries `contents_discarded: true`; the scheduler decides how that affects production.

`current_command_id` and `interrupted_command_id` reference actual actuator commands. They are not renamed production identifiers. A timer completion references the initiating command; STOP, RESUME and PERMIT preserve that reference. This lets the backend associate technical events and telemetry with its own records without putting those records inside a machine.

Snapshots contain `state`, `occupied`, `duration_h`, `elapsed_h`, `target_temp_c`, technical pipe flags, stop latches, permit status, counters and boot/command references. See `../contracts/PROTOCOL.md` for the complete schema. Unknown input fields are rejected rather than silently accepting business metadata.

## Timing, retries and restart boundaries

`TIME_SCALE` is real seconds per simulated hour. Timers use monotonic time; wall time only labels observations. Work advances only through the part of a tick covered by a valid permit with both STOP latches clear. An occupied machine can wait in `READY` indefinitely: the scheduler, not this controller, evaluates holding/quality policy.

Pending events remain in process memory until acknowledgement. Original command results remain until process termination, including after their events are acknowledged. Duplicate command IDs replay the original result before state-version guards, so retries do not repeat actuator movement. A new command with an outdated state version is rejected. Structural transitions increment `state_version`; ordinary progress ticks and permit renewals do not. This also prevents a delayed prepare/flush/commit from affecting a newer operation.

Critical events are enqueued in ordered groups on the next loop, approximately every 10 ms. Pending events retry every 500 ms. On MQTT reconnect, the publication cursor resets and the existing RAM history is sent in sequence before new events. A reconnect preserves that history; a process restart loses it. QoS 1 and command deduplication therefore provide retry handling within one boot, not crash recovery or cross-restart exactly-once execution.

Kafka telemetry is best-effort simulated temperature and density/pH/CO2. It carries equipment/boot/actuator references only; backend enrichment adds production context. Telemetry never controls equipment or substitutes for acknowledged critical events.

Accelerated factory profiles require a measured pilot: coordinator scheduling and network latency consume real time while simulation clocks advance. At `TIME_SCALE=2`, one simulated hour lasts two real seconds. Check transfer latency before choosing holding limits or interpreting experiment results.

## Build and local checks

```sh
make -C src/machine check          # C11 compiler and libm only
make -C src/machine check-wire     # additionally needs cJSON
make -C src/machine all            # additionally needs Paho MQTT C and librdkafka
make -C src/machine sanitize       # core tests with ASan + UBSan where supported
docker build -f src/machine/Dockerfile -t beerfactory-machine:local .
```

The Docker build runs the core and real cJSON wire tests before linking production MQTT/Kafka adapters. The image runs as UID/GID 10001 and requires no persistent volume. The only filesystem endpoint is the temporary owner-only Unix socket used for physical-input simulation.

| Environment variable | Default |
|---|---|
| `MACHINE_TYPE` | `mash` |
| `MACHINE_ID` | `HOSTNAME`, otherwise `mash-0` |
| `MQTT_URL` | `tcp://rabbitmq:1883` |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | `beerfactory` / `dev-only-password` |
| `KAFKA_BROKERS` | `kafka:9092` |
| `SOCKET_PATH` | `/tmp/machine.sock` |
| `TIME_SCALE` | `2` |
| `FLUSH_H` | `0.1` |
| `HEARTBEAT_MS` | `2000` |

Deployment manifests inject matching broker credentials. Environment parsing rejects nonpositive/nonfinite numeric settings.

## Simulated physical controls

```sh
machinectl snapshot
machinectl status
machinectl stop
machinectl resume
machinectl stop --scope global
machinectl flush
machinectl flush --duration 0.3
machinectl --socket /tmp/another-machine.sock snapshot
```

These controls work independently of application broker connectivity. A global physical STOP immediately latches the addressed controller and emits `STOPPED` with global scope; the backend distributes the line stop, while the operator helper can also fan out direct local inputs. Reset the line through the backend API so its latch is reset too. Global resume does not clear a local STOP.

The CLI flush operates on whatever is physically present now. It takes no batch identifier or business reason. Accepted flushing waits while stopped or while a pipe reservation remains unresolved. Repeated flush commands do not reset a running timer. The CLI prints JSON and returns a nonzero exit code for rejection or communication failure.
