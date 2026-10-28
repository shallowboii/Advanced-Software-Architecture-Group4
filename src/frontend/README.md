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

## Reading the code

The frontend is a client of the backend HTTP/WebSocket API. It does not schedule batches, consume MQTT/Kafka or decide transfer ownership. The scheduler owns orders, batches, recipes and their machine mappings. A physical controller knows its local operation and measurements; it does not know a batch, recipe or order. The machine records displayed here are enriched backend projections combining physical state with centrally maintained production information.

| Module | Responsibility |
| --- | --- |
| `src/App.tsx` | Navigation, selected machine/batch and explicit operator confirmations. |
| `src/useFactory.ts` | REST snapshots, endpoint errors, WebSocket reconnection and the live sample buffer. |
| `src/useCommands.ts` | Pending command count, outcome notices and retries with the original key/body/operator. |
| `src/api.ts` | Same-origin HTTP transport and structured error conversion. |
| `src/actions.ts` | Shared action/confirmation callback types, keeping views independent of retry implementation. |
| `src/Operations.tsx` | Six-stage overview and the registered 20/40 uni-tank grid. |
| `src/MachineDialog.tsx` | Inspection and local controls for one machine. |
| `src/views/` | Separate order, recipe, inventory, experiment and history screens; shared event table/chart. |
| `src/forms.tsx` | Order entry and editing detached recipe drafts. |
| `src/domain.ts` | Pure presentation rules, recipe validation, simulation-time formatting and sample deduplication. |
| `src/types.ts` | TypeScript representations of the shared API contract. |
| `src/ui.tsx` | Accessible dialogs, input fields and small shared presentation elements. |

A typical action starts in a form or confirmation, calls `perform`, and receives a request ID in `useCommands`. A successful response schedules a fresh snapshot; the UI does not assume that a controller has already completed the operation. An uncertain result keeps the original request for retry. Normal actions remain inhibited while it is unresolved; global STOP remains available.

Telemetry takes a separate path: `useFactory` collects a bounded WebSocket buffer, and `HistoryView` requests the latest persisted samples with `order=desc`. `orderedSamples` merges and deduplicates them before sorting by timestamp for the chart. Sensor readings are observations, not command acknowledgements.

The inventory screen displays a Python in-memory mock. Its stock and idempotency records reset when that service restarts. Restarting inventory alone does not reconcile the retained backend batches; use a fresh experiment instead of treating those stock values as recovered production accounting.
