# Scheduling

Two independent parts live here:

| Part | Files | What it is |
|---|---|---|
| Scheduler service (Kafka) | `src/scheduler.h`, `src/scheduler.c`, `src/machine_table.c`, `src/dispatch.c`, `src/scheduler_main.c` | Follows every machine over Kafka (`machine_kafka`), keeps a table of their states and calls `dispatch()` on every status. |
| Greedy planner | `src/greedy_scheduler.c`, `src/structure.*`, `src/machine.h` | Offline planning experiment; not connected to Kafka. |

## Scheduler service

- **Statuses in:** all six topics (`MK_MACHINE_TYPES`), consumer group `scheduler`. Each status updates the machine's row: state, batch, error, last `ts_utc`.
- **Policy:** `dispatch(s, changed)` in `src/dispatch.c`, called after every status. It is empty; the scheduling policy goes there.
- **Commands out:** `scheduler_start(s, type, id, batch, params, n)`, `scheduler_stop(...)`, `scheduler_flush(...)`.

```bash
make test     # machine table, no broker
make run      # the service (KAFKA_BROKERS, default localhost:9092)
```

On Windows run them through the dev container, with Kafka up (`docker compose up -d` in `machine_commons/machine_kafka`):

```bash
../../../machine_commons/dev/run.sh make test run
```
