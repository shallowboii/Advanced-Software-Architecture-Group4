# Inventory mock

This Python standard-library service represents the ingredient inventory as an **in-memory mock**. It exposes HTTP on port 8081 and has no database, file ledger, mounted data directory or recovery mechanism. Restarting the process resets stock and forgets reservations, delivery IDs and cancellation tombstones.

## Run and reset behaviour

```sh
INITIAL_BATCHES=100 python3 src/inventory/server.py
```

`INITIAL_BATCHES` seeds every new process. One seed batch allocates 900 kg malt, 20 kg hops, 10 kg yeast and 6,000 L water. These are fixture quantities, not brewing measurements. `PORT` defaults to 8081; `INVENTORY_HOST` defaults to `0.0.0.0`.

Use one service process for a simulation run. Restarting inventory alone while the backend retains active batches creates a newly seeded mock, not a recovered material balance. Reset the complete experiment when restarting the mock; its numbers do not represent continuous stock accounting across restarts.

## HTTP contract

| Endpoint | Responsibility |
| --- | --- |
| `GET /health` | Report that the HTTP service is available. |
| `GET /stock` | Read currently available ingredient amounts. |
| `POST /reserve` | Reserve ingredients for one `batch_id` before admission. |
| `GET /reservations/{batch_id}` | Read a reservation or cancellation known by this process. |
| `POST /reservations/{batch_id}/release` | Cancel an unadmitted batch, refunding at most once. |
| `POST /restock` | Add a delivery identified by `request_id` at most once. |

Requests use the shared [protocol](../contracts/PROTOCOL.md). Ingredient payloads contain all four keys: `malt_kg`, `hops_kg`, `yeast_kg`, `water_l`. Values must be finite nonnegative numbers up to 1e12 with at most nine fractional digits. Booleans, unknown fields and duplicate JSON keys are rejected.

Retries must reuse the original ID and payload. The response remembered for that ID is returned without repeating its stock change. Reusing the ID with other quantities returns `409 idempotency_conflict`. These records exist only in RAM for this process.

Releasing an unknown batch creates a zero-quantity cancellation tombstone. If the original reserve arrives later, it returns `409 reservation_released` without consuming ingredients. A released ID cannot be reserved again during the same process lifetime. The backend may release only a batch that never entered production; flushing beer does not refund its ingredients.

## Reading the code

`Inventory` owns stock, reservation records and delivery records. Its `threading.Lock` surrounds each check-and-change sequence, preventing two request threads from spending the same stock. Amounts use `Decimal` internally so repeated fractional deductions do not accumulate floating-point error. Responses are detached copies, preventing callers from modifying the stored retry response.

`Reservation` and `Delivery` name the data retained for retry handling. Validation helpers check input before entering the critical section. `Handler` owns HTTP parsing, routing and JSON error responses; it delegates stock rules to `Inventory`. `main()` seeds the process, opens the listener and shuts it down on SIGTERM or SIGINT.

## Verification

```sh
python3 -m unittest discover -s src/inventory -p 'test_*.py' -v
docker build -f src/inventory/Dockerfile -t beerfactory-inventory .
```

Tests cover concurrent reservations, duplicate requests, cancellation races, invalid input, decimal accounting and reset semantics. HTTP tests use a real loopback listener and verify successful workflows and malformed requests. Run the command from the repository root in an environment that permits opening a local port.
