# Production dashboard

English React + TypeScript dashboard for the Beer Factory API. Screens cover the six-stage line, dense 20/40 uni-tank layouts, orders and batch traces, immutable recipe versions, recorded/live telemetry, inventory deliveries, operator controls and experiment counters. Empty and disconnected states are real: the application does not invent machines or sensor readings.

## Run

Use Node 24 or newer:

```sh
cd src/frontend
npm ci
npm run dev
```

Vite proxies `/api` and `/ws` to `localhost:8080`. The production image uses nginx on port 80 and proxies those paths to `backend:8080`; Compose publishes the UI on port 8088. API and WebSocket traffic always use the browser's current origin. Build with `docker build -f src/frontend/Dockerfile -t beerfactory-frontend .` from the repository root.

## Behaviour

- A raw WebSocket receives updates; REST snapshots refresh every three seconds and remain available while the socket reconnects with capped exponential backoff. History combines recorded and live samples with deduplication.
- A stale scheduler/machine snapshot disables ordinary mutations. Global STOP remains available and sends immediately; its status is confirmed by the next scheduler snapshot, including pending machine acknowledgements.
- Every mutation includes `Idempotency-Key` and `X-Operator`. An uncertain outcome retains the original payload and request key for an explicit retry and inhibits further ordinary mutations until resolved. Retries are held in memory for the current page session; avoid reloading during an uncertain operation.
- Flush confirmations name the selected batch. Machine flush includes `batch_id`, preventing a delayed click from discarding a replacement batch. Line resume is explicit, and local stops remain independent.
- Recipe editors publish new versions; active batches retain their admitted version. Time values use simulated hours. Volumes are fixed at 50 hL.
- The operator label is stored locally and used for audit attribution. This classroom application does not provide authentication or physical safety certification.

## Verification

```sh
npm test
npm run build
```

Tests exercise status precedence, bounded progress, recipe invariants, telemetry deduplication and simulation time formatting. The production build checks TypeScript and bundles the application. A running backend is required to exercise end-to-end operator flows. The SVG telemetry chart is implemented locally without a charting dependency; it provides accessible sample count, minimum and maximum descriptions.
