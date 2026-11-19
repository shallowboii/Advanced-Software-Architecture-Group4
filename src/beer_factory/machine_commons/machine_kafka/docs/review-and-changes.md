# machine_kafka: review and change log

Review of the prototype in commit `ee681bc` ("kafka prototype") and what was
changed for schema v2 (sections 1–5), then the simplifications of schema v3
after the team review (section 6). Sections 1–4 describe v2 as it was; the
current design is described in [kafka-design.md](kafka-design.md).

## 1. Review of the prototype

### Good, kept

- Small, single-threaded API around an opaque `mk_bus_t`; no librdkafka or
  cJSON types in the header; results as `mk_result_t`.
- Idempotent producer, `linger.ms=0`, retry until the broker is back,
  `MK_ERR_FATAL` only for real fatal errors, one retry on a full queue,
  delivery reports served from `mk_poll`.
- Offsets committed manually after the handler; malformed messages logged
  with topic/partition/offset, committed and skipped.
- Strict validation: no silent truncation (`copy_str`), `schema_version`,
  unknown fields ignored, encode and decode share one rule set.
- Key = `machine_id` for per-machine order.
- Tests for encode/decode and a real broker round trip.

### Bugs and gaps

| # | Finding | Effect | Fixed by |
|---|---|---|---|
| 1 | Header and README claimed "exactly once" via `mk_dedup` (last 32 `message_id`s in memory). | A start re-sent by a restarted scheduler has a new id; after a machine restart the ring is empty. Both run the step again. | Step ledger keyed on `(batch_id, step)`, persisted (change 4). |
| 2 | `mk_dedup_seen` records the id before the machine decides; in the README example a BUSY start is recorded as "seen". | Harmless today, wrong once refusals matter. | Ledger: refusals stored separately, by `message_id` (`X` entries). |
| 3 | Default `auto.offset.reset=earliest`. | A replacement machine without committed offsets reads the whole topic history and can re-run old starts. | Default `latest` for machines (change 5). |
| 4 | Commands and replies on one topic. | Every machine decodes every other machine's done/error; the scheduler reads its own starts. | Separate `.commands` / `.status` topics (change 1). |
| 5 | A machine filtered only on `machine_id`; nothing checked that the message's `machine_type` matches the topic. | A misrouted mashing start for id 3 on the boiling topic would be executed by boiler 3. | `mk_poll` skips messages whose `(machine_type, type)` doesn't map to the topic. |
| 6 | `MK_MAX_JSON_LEN = 1024` was documented as "every valid message fits", but all strings accepted any character. Characters JSON escapes as `\u00XX` take 6 bytes: 95 + 95 + 63 + 127 characters → about 2.3 KB. | `mk_send` of a valid message could fail with `MK_ERR_ENCODE`. Unlikely, but the guarantee was false. | Identifiers restricted to `[A-Za-z0-9._:-]` (never escaped); only `error.reason` is free text; limit 4096; worst case computed in a test (1473 bytes). |
| 7 | `machine_type` allowed `.` and upper case. | With topics named `machine.<type>.status`, a dot makes names ambiguous; `Boiling` vs `boiling` would be two topics. | `[a-z][a-z0-9_-]*`. |
| 8 | librdkafka's default partitioner (`consistent_random`, CRC32) differs from the Java client's (murmur2). | With more than one partition, the C scheduler and a Java backend would put the same `machine_id` on different partitions, so per-machine order breaks. | `partitioner=murmur2_random`. |
| 9 | `message.timeout.ms=0` (retry forever) also for commands. | A start queued during a broker outage arrives whenever the broker is back, possibly hours later. | Machines drop commands older than 300 s (`ts_utc`) (change 6). |
| 10 | Permanent delivery failures only counted. | The application can't tell which message (e.g. which `done`) was lost. | Optional `on_delivery_failure` callback (change 6). |
| 11 | Topics auto-created by the broker; README used `acks=all` with a single broker. | A typo creates a topic nobody reads; RF 1 makes `acks=all` meaningless for durability. | `scripts/create-topics.sh`, auto-create off in `docker-compose.yml`, warnings and production values documented. |
| 12 | Only `start`/`done`/`error`; no step, no parameters. | Scheduler can't see what a machine is doing; recipes can't be passed (QA-7). | Status `idle`/`running`/`done`/`error`, `step`, `params`, `progress` (changes 2, 3). |
| 13 | `test_bus` "duplicate" re-sent the identical message (same `message_id`). | It only tested the case that was already covered and missed the real ones (finding 1). | New tests (below). |

### Compared to the task's requirements (before → after)

| Requirement | Before | After |
|---|---|---|
| 1. Separate topics | one topic per type | `machine.<type>.commands` / `.status`, chosen by `mk_send` from `type` |
| 2. Status messages | done / error only | idle, running (+progress), done, error; stop/flush defined |
| 3. Recipe params | none | ≤ 16 name → number pairs |
| 4. No step twice | message-id ring in memory | `(batch_id, step)` ledger in a file, refusals by message_id, restart-safe |
| 5. Zero-config machines | earliest; explicit topic lists only | latest + exact `mk_ready`; scheduler regex + earliest |
| 6. Smaller points | — | command expiry, delivery callback, topic script, commit-cost analysis |

## 2. Change log

### Files

| File | Change |
|---|---|
| `machine_kafka.h` | Schema v2 types and fields, topic helpers, params, status helpers, ledger API, `mk_ready`, new config fields, honest guarantee text. |
| `mk_message.c` | New types, `step`/`params`/`progress`, identifier and machine-type character sets, `mk_topic`, `mk_make_command/idle/running`, `mk_param_set/get`. |
| `mk_ledger.c` | **New.** Persistent step ledger (atomic file writes). |
| `mk_bus.c` | Topic from message type; rebalance callback that fixes start offsets and sets `mk_ready`; default `latest`; regex metadata refresh; topic/type check; command expiry; delivery-failure callback; Java-compatible partitioner. `mk_dedup` removed. |
| `tests/test_message.c` | Rewritten for v2: topics, params (round trip, limits, rejects), all statuses, stop/flush, worst-case size. |
| `tests/test_ledger.c` | **New.** Duplicates with new ids, restart from file, interrupted steps, refusals, eviction, corrupt/foreign files. |
| `tests/test_bus.c` | Rewritten: 7 broker scenarios (below). |
| `Makefile` | `mk_ledger.c`, `test-ledger`, pattern rule. |
| `docker-compose.yml`, `scripts/create-topics.sh`, `.gitattributes` | **New.** KRaft broker, topic creation, LF line endings for the script. |
| `README.md`, `docs/kafka-design.md`, this file | Documentation. |

### Public API changes

All of these follow from the six required points; nothing else was changed.

| Change | Required by |
|---|---|
| Removed `mk_dedup_t`, `mk_dedup_init`, `mk_dedup_seen`; added `mk_ledger_*` | 4 |
| `mk_type_t` gained `MK_STOP`, `MK_FLUSH`, `MK_IDLE`, `MK_RUNNING` (numeric values of `MK_DONE`/`MK_ERROR` changed) | 2 |
| `mk_make_start(out, type, id, batch_id, step)`: new `step` argument (part of the duplicate key) | 2, 4 |
| `mk_message_t`: `step`, `params`/`n_params`, `has_progress`/`progress` | 2, 3 |
| New `mk_make_command`, `mk_make_idle`, `mk_make_running`, `mk_param_set/get`, `mk_is_command`, `mk_topic` | 1, 2, 3 |
| `mk_make_error` answers any command (stop/flush too) | 2 |
| `done`/`error` no longer require `reply_to` (e.g. `INTERRUPTED` after a restart answers the original start, but a status may also be unsolicited) | 2, 4 |
| `mk_ready()` | 5 (without it `latest` can skip a start sent right after `idle`) |
| `mk_config_t`: default `offset_reset` is now `latest`; new `command_max_age_s`, `on_delivery_failure`, `delivery_user` | 5, 6 |
| `MK_ERR_IO`, `MK_STATUS_TOPIC_REGEX`, `MK_TOPIC_LEN`, `MK_PATH_LEN`, `MK_MAX_PARAMS`, `MK_LEDGER_SIZE`, `MK_DEFAULT_COMMAND_MAX_AGE_S` | 3, 4, 5, 6 |
| Stricter validation: identifier character set, lower-case `machine_type` without dots, `client_id` must be a valid identifier, `MK_MAX_JSON_LEN` 4096 | 1, 3 (topic names, size bound) |
| `schema_version` 2: v1 messages are rejected | 1, 2 |

No machine or scheduler code used the library yet, so nothing else had to be
migrated.

## 3. Decisions and alternatives

| Decision | Chosen | Alternatives considered |
|---|---|---|
| Topic layout | two topics per type | one topic per machine instance (no filtering, but every new machine needs topics: breaks QA-5); one global pair (every machine reads everything) |
| Message kind on the wire | single `type` field with disjoint values | separate `kind` + `type` fields (redundant, can contradict; the topic already says the direction) |
| Recipe params | name → number object, ≤ 16 | raw JSON string (machines need a JSON parser, no central validation, unbounded size); typed per-step structs (code change per recipe field, against QA-7) |
| Duplicate key | `(batch_id, step)` per machine | `message_id` (fails on scheduler resend); `batch_id` alone (a machine can run several steps of one batch, e.g. fermentation then maturation) |
| Ledger storage | text file, write tmp + fsync + rename, last 16 entries | SQLite (extra dependency for 16 lines); binary struct dump (not inspectable, layout-dependent); Kafka compacted topic (the state must be durable *before* actuation even when the broker is down) |
| Step running at restart | reported `error INTERRUPTED`, never resumed | resume automatically (unsafe for e.g. hop additions; machine-specific); silently forget (could run twice) |
| Refused starts | remembered by `message_id` | not stored (a redelivered BUSY start would run later, after the scheduler gave it to another machine); stored by `(batch_id, step)` (a legitimate retry would be refused forever) |
| Start offset for new groups | resolved in the rebalance callback, then `mk_ready` | rely on `auto.offset.reset` (race: start sent right after `idle` can be skipped) |
| Scheduler offset reset | `earliest` | `latest` (misses the first `idle` on a newly created status topic) |
| Stale commands | receiver drops by `ts_utc` age | finite producer timeout for commands (doesn't cover time spent in the topic) |
| Delivery failures | optional callback + counter | callback only; return codes from `mk_send` (delivery is asynchronous) |
| Commit | synchronous per message | asynchronous / batched (more redeliveries; the ledger would absorb them, so this can change later) |
| Partitions | 1 per topic | 3+ (no consumer could use the parallelism; per-stage total order is lost) |

## 4. Tests

| Test | Needs broker | Covers |
|---|---|---|
| `test-message` | no | topics; start/params round trip (exact doubles); param limits (17th param, bad names, NaN/∞, duplicates, non-numbers) rejected; all statuses; stop/flush; JSON shape; ~20 malformed inputs; unknown fields ignored; worst-case size |
| `test-ledger` | no | same step with new `message_id` → not run, running/done replayed; another step/batch is new; BUSY refusal replayed only for the same message; restart: done/error/interrupted restored from file, `INTERRUPTED` reported once; eviction after 16; corrupt and foreign files rejected |
| `test-bus` | yes | 1. start → running → done → idle on the right topics (raw consumer checks direction and key), params intact; 2. duplicate start with new id → executed once, `running` / `BUSY` while running; 3. machine restart (new bus, reloaded ledger) and a full redelivery (new group, earliest) → nothing runs twice, `INTERRUPTED` reported; 4. reused id: `latest` ignores the old start, `earliest` would receive it, new start after `mk_ready` received; 5. status topic created after the scheduler subscribed is picked up by the regex; 6. garbage, old schema, status on commands topic, wrong machine type, expired start → skipped, next start handled; 7. send to a missing topic → `on_delivery_failure` |

All three pass with `-Wall -Wextra -Wpedantic -Werror` (gcc 14, librdkafka
2.8.0, cJSON 1.7.18), and under AddressSanitizer + UndefinedBehaviorSanitizer
(including leak checks) against the broker from `docker-compose.yml`. The
broker test passed in every run during development (7 runs, 4 of them with
sanitizers).

## 5. Open questions and remaining work

1. **Heartbeat lost in the middle of a batch.** The wort is physically in
   that machine, so the step cannot simply be re-dispatched elsewhere.
   Proposal: mark the machine down, hold the batch, alert the operator; if
   the machine comes back it reports `error INTERRUPTED` (or `done` if it
   finished before the crash and the status was lost), and the operator
   decides (continue, BPS-7 discard, or flush).
2. **Heartbeat payload.** `../machine_mqtt` sends a native C struct with one
   `machine_name` ("boiling-1"). The scheduler needs `machine_type` and
   `machine_id` exactly as on Kafka, and the Java backend needs a portable
   format: suggest JSON `{"machine_type":"boiling","machine_id":1,"ts_utc":...}`.
3. **Retry after `error` / `INTERRUPTED`.** The ledger refuses the same
   `batch_id` on that machine forever (that is the guarantee). A
   deliberate retry needs a way around it: `flush` clearing the ledger entry
   (natural fit for FR-14), or an `attempt` number in the key. Decide with
   FR-14.
4. **Resuming long steps.** Fermentation runs for days; a controller restart
   currently ends the step as `INTERRUPTED`. Machines that can resume safely
   could keep the `R` entry instead. Not needed for the prototype.
5. **Scheduler timeouts and state.** Steps last from one hour to days, so
   timeouts should come from the recipe (`duration_s`) plus a margin, with
   `running` updates as a keepalive. Where the scheduler keeps its own state
   (Redis/Postgres from the component view) is still open.
6. **Who sends `stop`/`flush`:** the scheduler, or the Java backend directly
   from the supervisor UI? If the backend, it needs write access to the
   machine-type topics (the partitioner already matches).
7. **What a machine reports after `stop`.** Probably `error STOPPED`, then
   `idle`; decide with FR-13.
8. ~~**Step vocabulary.**~~ Resolved in v3: no `step`; one start per batch
   and machine (the fermentation tank does fermentation and maturation).
9. **Production deployment.** 3 brokers, RF 3, `min.insync.replicas=2`
   (QA-8); TLS/SASL and ACLs are not addressed.
10. **Clock synchronisation** (NTP) is a requirement for command expiry.
11. **Ledger volume.** Each machine container needs its own persistent volume
    for the ledger file; losing it removes the duplicate protection.
12. **String recipe data** (e.g. hop variety) cannot go into `params`; it
    belongs in the recipe/inventory data, or in a new optional field if a
    machine ever needs it.
13. ~~**Progress update rate.**~~ Resolved in v3: `progress` removed;
    telemetry stays on MQTT.

## 6. Schema v3: simplifications after the team review

| Change | Reason |
|---|---|
| **One topic per machine type**, named after the type (`boiling`, …) instead of `machine.<type>.commands` + `machine.<type>.status`. | Half the topics; a batch's start, running and done sit next to each other in one topic (easier to follow in the Kafka UI). The costs of sharing (machines read statuses, the scheduler reads its own commands) are handled in the library: `mk_config_t.receive` selects the direction, the other one is skipped. Negligible at this volume. |
| **Shorter type names**: `fermentation` (was `fermentation-maturation`), `filtering` (was `filtering-carbonation`). | Match the machine folders; the `machine.` prefix was redundant. |
| **`step` removed.** The ledger keys on `batch_id` alone. | Each machine type does exactly one thing per batch (the fermentation tank moves from fermentation to maturation by itself after one start), so the machine type already names the step. |
| **`reply_to` removed.** Statuses are matched by `(machine_type, machine_id, batch_id)`. | The scheduler doesn't need it; it was mainly for tracing. Refusals (BUSY) used it to find the refused start: replaced by `mk_ledger_refuse(ledger, start, code, reason, &status)`, which stores the refusal by the start's `message_id` itself. |
| **`progress` removed.** | Not used; telemetry goes over MQTT. |
| **Scheduler topic list instead of a regex.** `MK_MACHINE_TYPES` lists all six types; `MK_STATUS_TOPIC_REGEX` removed. | Without a common prefix, a regex can't tell machine topics from other topics. A new type needs a scheduler stage (code change) anyway. |
| **`offset_reset` default follows `receive`**: latest for machines, earliest for the scheduler. | One setting less to get wrong. |
| **Source files without the `mk_` prefix** (`bus.c`, `message.c`, `ledger.c`, `trace.c/.h`). Functions keep `mk_`. | The folder already says what the files are. The function prefix stays because C has one global namespace: `send`, `poll`, `open` would clash with POSIX. |

API changes: `mk_make_start(out, type, id, batch_id)` and
`mk_make_command(out, cmd, type, id, batch_id)` lost `step`;
`mk_make_running(out, start)` lost `progress`; `mk_topic`, `MK_TOPIC_LEN`
and `MK_STATUS_TOPIC_REGEX` removed; `mk_ledger_refuse`, `mk_receive_t`,
`mk_config_t.receive` and `MK_MACHINE_TYPES` added; `schema_version` 3, ledger
file format `mk-ledger 2` (v2 files are rejected as corrupt; delete them).

Tests: `test_bus` scenario 5 now checks that the scheduler reads several
machine types (was: regex picks up a new topic); scenario 1 checks that both
directions share the topic with the right keys; scenario 6 replaces "status on
the commands topic" with an old v2 message. All pass with `-Werror`.
