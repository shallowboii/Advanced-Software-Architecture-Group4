# Inventory service

Python standard-library HTTP service on port 8081. Stock and idempotency records are persisted together with atomic file replacement and fsync. An OS file lock prevents two processes from using the same ledger. Mount a persistent volume at `/data`.

Run locally with `INVENTORY_STATE_FILE=/tmp/beer-inventory.json python3 src/inventory/server.py`. `INITIAL_BATCHES` seeds a new ledger only; restarting with another value does not refill stock. One seed batch allocates 900 kg malt, 20 kg hops, 10 kg yeast and 6,000 L water. These are inventory fixture quantities, not brewing measurements.

`GET /health`, `GET /stock`, `POST /reserve`, `GET /reservations/{batch_id}`, `POST /reservations/{batch_id}/release`, `POST /restock` follow [the shared contract](../contracts/PROTOCOL.md). Ingredient payloads contain all four keys: `malt_kg`, `hops_kg`, `yeast_kg`, `water_l`. Quantities are finite nonnegative numbers up to 1e12 with at most nine fractional digits. Mutation retries must reuse the original ID and payload. Released batch IDs cannot be reserved again. The backend must only release a reservation for a batch that never entered production; a flush does not refund stock.

Tests: `python3 -m unittest discover -s src/inventory -p 'test_*.py' -v`.

Build from repository root: `docker build -f src/inventory/Dockerfile -t beerfactory-inventory .`.
