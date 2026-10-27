import { useEffect, useMemo, useState } from "react";
import type { FormEvent } from "react";
import { errorMessage, request } from "./api";
import {
  duration,
  humanize,
  orderedSamples,
  simClock,
  stageLabel,
} from "./domain";
import { Empty, Icon, NumberField, shortId, timestamp } from "./ui";
import { INGREDIENTS } from "./types";
import type {
  Batch,
  FactorySnapshot,
  Ingredients,
  ProductionEvent,
  Recipe,
  Telemetry,
} from "./types";

export type Perform = (
  path: string,
  body: unknown,
  success: string,
) => Promise<boolean>;
export type Confirm = (
  title: string,
  message: string,
  label: string,
  action: () => Promise<boolean>,
  destructive?: boolean,
) => void;

export function OrdersView({
  data,
  createOrder,
  confirm,
  perform,
  viewBatch,
  canAct,
}: {
  data: FactorySnapshot;
  createOrder: () => void;
  confirm: Confirm;
  perform: Perform;
  viewBatch: (id: string) => void;
  canAct: boolean;
}) {
  return (
    <>
      <PageHeading
        eyebrow="PRODUCTION PLANNING"
        title="Orders & batches"
        description="Queue demand, then follow each batch through the line."
        action={
          <button
            className="button primary"
            onClick={createOrder}
            disabled={!canAct || !data.recipes.length}
          >
            <Icon name="plus" />
            New order
          </button>
        }
      />
      <section className="panel">
        <div className="panel-heading">
          <h2>Production orders</h2>
          <span className="quiet-tag">{data.orders.length} orders</span>
        </div>
        {data.orders.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Order</th>
                  <th>Recipe</th>
                  <th>Requested</th>
                  <th>Progress</th>
                  <th>Lost</th>
                  <th>Status</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {data.orders.map((order) => (
                  <tr key={order.id}>
                    <td className="mono" title={order.id}>
                      {shortId(order.id)}
                      {order.priority > 0 && (
                        <span className="priority-mark">
                          Priority {order.priority}
                        </span>
                      )}
                    </td>
                    <td>
                      {data.recipes.find(
                        (recipe) => recipe.id === order.recipe_id,
                      )?.name ?? order.recipe_id}
                    </td>
                    <td>
                      {order.quantity} batches
                      <small>{order.quantity * 50} hL</small>
                    </td>
                    <td>
                      <div className="inline-progress">
                        <span>
                          {order.completed}/{order.quantity}
                        </span>
                        <div className="progress-track">
                          <span
                            style={{
                              width: `${(order.completed / Math.max(1, order.quantity)) * 100}%`,
                            }}
                          />
                        </div>
                      </div>
                    </td>
                    <td className={order.discarded ? "text-danger" : ""}>
                      {order.discarded} batches
                    </td>
                    <td>
                      <span className="quiet-tag">
                        {humanize(order.status)}
                      </span>
                    </td>
                    <td>
                      {!["COMPLETED", "FINISHED", "CANCELLED"].includes(
                        order.status,
                      ) &&
                        order.started < order.quantity && (
                          <button
                            className="text-button"
                            disabled={!canAct}
                            onClick={() =>
                              confirm(
                                "Cancel outstanding demand?",
                                `${order.quantity - order.started} unstarted batches will be cancelled. Batches already on the line will continue.`,
                                "Cancel demand",
                                () =>
                                  perform(
                                    `/orders/${encodeURIComponent(order.id)}/cancel`,
                                    {},
                                    "Outstanding demand cancelled.",
                                  ),
                                true,
                              )
                            }
                          >
                            Cancel remaining
                          </button>
                        )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty title="No orders yet">
            An order specifies the beer and the number of 50 hL batches to
            produce.
          </Empty>
        )}
      </section>
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Batch register</h2>
            <p>Select a batch to inspect its production history.</p>
          </div>
          <span className="quiet-tag">{data.batches.length} batches</span>
        </div>
        {data.batches.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Batch</th>
                  <th>Recipe version</th>
                  <th>Machine</th>
                  <th>Stage</th>
                  <th>Status</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {data.batches.map((batch) => (
                  <tr key={batch.id}>
                    <td>
                      <button
                        className="table-link mono"
                        onClick={() => viewBatch(batch.id)}
                        title={batch.id}
                      >
                        {shortId(batch.id)}
                      </button>
                    </td>
                    <td>
                      {batch.recipe_id}
                      <small className="mono" title={batch.recipe_version_id}>
                        {shortId(batch.recipe_version_id)}
                      </small>
                    </td>
                    <td>{batch.machine_id ?? "Outside the line"}</td>
                    <td>{stageLabel(batch.stage)}</td>
                    <td>
                      <span
                        className={`quiet-tag ${/SPOIL|DISCARD|FLUSH/.test(batch.status) ? "danger" : ""}`}
                      >
                        {humanize(batch.status)}
                      </span>
                    </td>
                    <td className="dim">{timestamp(batch.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <Empty title="No admitted batches">
            Batches appear after the scheduler reserves ingredients and admits
            them to the line.
          </Empty>
        )}
      </section>
    </>
  );
}

export function RecipesView({
  recipes,
  edit,
  canAct,
}: {
  recipes: Recipe[];
  edit: (recipe?: Recipe) => void;
  canAct: boolean;
}) {
  return (
    <>
      <PageHeading
        eyebrow="BREWING DEFINITIONS"
        title="Recipe library"
        description="New versions apply to future admissions. Active batches keep their original recipe."
        action={
          <button
            className="button primary"
            disabled={!canAct}
            onClick={() => edit()}
          >
            <Icon name="plus" />
            Create recipe
          </button>
        }
      />
      {recipes.length ? (
        <div className="recipe-grid">
          {recipes.map((recipe) => (
            <section className="panel recipe-card" key={recipe.id}>
              <div className="recipe-card-top">
                <span className="recipe-icon">
                  <Icon name="recipe" size={27} />
                </span>
                <span className="quiet-tag">Version {recipe.version}</span>
              </div>
              <h2>{recipe.name}</h2>
              <p className="mono dim">{recipe.id}</p>
              <div className="recipe-facts">
                <div>
                  <small>Batch volume</small>
                  <strong>{recipe.volume_hl} hL</strong>
                </div>
                <div>
                  <small>Process duration</small>
                  <strong>
                    {duration(
                      recipe.stages?.reduce(
                        (sum, stage) =>
                          sum +
                          stage.phases.reduce(
                            (total, phase) => total + phase.duration_h,
                            0,
                          ),
                        0,
                      ),
                    )}
                  </strong>
                </div>
              </div>
              <div className="recipe-timeline">
                {recipe.stages?.map((stage) => (
                  <div key={stage.type}>
                    <span>{humanize(stage.type)}</span>
                    <span>
                      {duration(
                        stage.phases.reduce(
                          (sum, phase) => sum + phase.duration_h,
                          0,
                        ),
                      )}
                    </span>
                  </div>
                ))}
              </div>
              <button
                className="button secondary full"
                disabled={!canAct}
                onClick={() => edit(recipe)}
              >
                Publish a new version <Icon name="arrow" size={16} />
              </button>
            </section>
          ))}
        </div>
      ) : (
        <Empty title="No recipes loaded">
          Create a recipe or check the backend recipe configuration.
        </Empty>
      )}
    </>
  );
}

const ingredientNames: Record<keyof Ingredients, string> = {
  malt_kg: "Malt",
  hops_kg: "Hops",
  yeast_kg: "Yeast",
  water_l: "Water",
};

export function InventoryView({
  data,
  perform,
  canAct,
}: {
  data: FactorySnapshot;
  perform: Perform;
  canAct: boolean;
}) {
  const [delivery, setDelivery] = useState<Ingredients>({
    malt_kg: 9000,
    hops_kg: 200,
    yeast_kg: 100,
    water_l: 60000,
  });
  const [busy, setBusy] = useState(false);
  const onSubmit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    try {
      await perform(
        "/inventory/restock",
        { request_id: crypto.randomUUID(), ingredients: delivery },
        "Delivery added to inventory.",
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      <PageHeading
        eyebrow="MATERIAL SUPPLY"
        title="Ingredient inventory"
        description="Ingredients are reserved once when a batch enters production."
      />
      <div className="metrics-grid">
        {INGREDIENTS.map((key) => (
          <div className="metric" key={key}>
            <div className="metric-top">
              <span>{ingredientNames[key]}</span>
              <Icon name={key === "water_l" ? "drop" : "box"} />
            </div>
            <strong>
              {data.inventory
                ? data.inventory.stock[key]?.toLocaleString()
                : "-"}{" "}
              <span className="metric-unit">
                {key === "water_l" ? "L" : "kg"}
              </span>
            </strong>
            <small>Available for new batches</small>
          </div>
        ))}
      </div>
      <div className="two-columns">
        <section className="panel">
          <div className="panel-heading">
            <div>
              <h2>Receive a delivery</h2>
              <p>Add delivered quantities to the available stock.</p>
            </div>
            <Icon name="box" />
          </div>
          <form onSubmit={onSubmit} className="panel-form">
            <div className="form-grid">
              {INGREDIENTS.map((key) => (
                <NumberField
                  key={key}
                  label={ingredientNames[key]}
                  value={delivery[key]}
                  suffix={key === "water_l" ? "L" : "kg"}
                  onChange={(value) =>
                    setDelivery((previous) => ({ ...previous, [key]: value }))
                  }
                />
              ))}
            </div>
            <button className="button primary" disabled={!canAct || busy}>
              <Icon name="plus" />
              {busy ? "Recording delivery…" : "Record delivery"}
            </button>
          </form>
        </section>
        <section className="panel explanation-panel">
          <span className="section-icon">
            <Icon name="box" size={28} />
          </span>
          <h2>Shortages pause admission</h2>
          <p>
            When an ingredient runs out, the scheduler keeps the order queued.
            Restocking lets it admit new batches again.
          </p>
          <p>
            Batches already in production continue. Flushing a batch records
            material loss and does not return ingredients to stock.
          </p>
        </section>
      </div>
    </>
  );
}

export function HistoryView({
  data,
  liveSamples,
}: {
  data: FactorySnapshot;
  liveSamples: Telemetry[];
}) {
  const [machineId, setMachineId] = useState("");
  const [sensor, setSensor] = useState("temp_c");
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [samples, setSamples] = useState<Telemetry[]>([]);
  const [events, setEvents] = useState<ProductionEvent[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!machineId && data.machines.length)
      setMachineId(data.machines[0].machine_id);
  }, [data.machines, machineId]);
  useEffect(() => {
    if (!machineId) return;
    let disposed = false;
    const abort = new AbortController();
    const load = async () => {
      setLoading(true);
      setError("");
      const params = new URLSearchParams({
        machine_id: machineId,
        sensor,
        limit: "1000",
      });
      if (from) params.set("from", new Date(from).toISOString());
      if (to) params.set("to", new Date(to).toISOString());
      if (from && to && Date.parse(from) > Date.parse(to)) {
        setError("The end time must be after the start time.");
        setLoading(false);
        return;
      }
      try {
        const [history, activity] = await Promise.all([
          request<Telemetry[]>(`/telemetry?${params}`, {
            signal: abort.signal,
          }),
          request<ProductionEvent[]>(
            `/events?machine_id=${encodeURIComponent(machineId)}&limit=100`,
            { signal: abort.signal },
          ),
        ]);
        if (!disposed) {
          setSamples(Array.isArray(history) ? history : []);
          setEvents(Array.isArray(activity) ? activity : []);
        }
      } catch (failure) {
        if (!disposed) setError(errorMessage(failure));
      } finally {
        if (!disposed) setLoading(false);
      }
    };
    void load();
    const timer = window.setInterval(() => void load(), 10000);
    return () => {
      disposed = true;
      abort.abort();
      window.clearInterval(timer);
    };
  }, [machineId, sensor, from, to]);
  const readings = useMemo(
    () =>
      orderedSamples(
        [...samples, ...(to ? [] : liveSamples)],
        machineId,
        sensor,
      )
        .filter(
          (sample) => !from || Date.parse(sample.ts_utc) >= Date.parse(from),
        )
        .slice(-1000),
    [samples, liveSamples, machineId, sensor, from, to],
  );
  return (
    <>
      <PageHeading
        eyebrow="PROCESS OBSERVABILITY"
        title="Telemetry & history"
        description="Inspect recorded sensor values and machine transitions."
      />
      <section className="panel">
        <form
          className="filter-bar"
          onSubmit={(event) => event.preventDefault()}
        >
          <label className="field">
            <span>Machine</span>
            <select
              value={machineId}
              onChange={(event) => setMachineId(event.target.value)}
            >
              <option value="" disabled>
                Select a machine
              </option>
              {data.machines.map((machine) => (
                <option key={machine.machine_id}>{machine.machine_id}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span>Sensor</span>
            <select
              value={sensor}
              onChange={(event) => setSensor(event.target.value)}
            >
              <option value="temp_c">Temperature</option>
              <option value="ph">pH</option>
              <option value="density">Density</option>
              <option value="co2">CO₂</option>
            </select>
          </label>
          <label className="field">
            <span>From</span>
            <input
              type="datetime-local"
              value={from}
              onChange={(event) => setFrom(event.target.value)}
            />
          </label>
          <label className="field">
            <span>To</span>
            <input
              type="datetime-local"
              value={to}
              onChange={(event) => setTo(event.target.value)}
            />
          </label>
          <button
            type="button"
            className="button secondary"
            onClick={() => {
              setFrom("");
              setTo("");
            }}
          >
            Live window
          </button>
        </form>
        <div className="panel-heading">
          <div>
            <h2>
              {humanize(sensor)}{" "}
              <span className="dim">
                / {machineId || "No machine selected"}
              </span>
            </h2>
            <p>
              {readings.length} samples{loading ? " · Refreshing…" : ""}
            </p>
          </div>
          <span className="quiet-tag">
            {to ? "Historical" : "Live + recorded"}
          </span>
        </div>
        {error && <p className="inline-error">{error}</p>}
        <TelemetryChart samples={readings} />
      </section>
      <section className="panel">
        <div className="panel-heading">
          <h2>Machine event log</h2>
          <span className="quiet-tag">{events.length} events</span>
        </div>
        <EventTable events={events} />
      </section>
    </>
  );
}

export function TelemetryChart({ samples }: { samples: Telemetry[] }) {
  if (!samples.length)
    return (
      <Empty title="No sensor readings in this window">
        Choose a machine and time range. Samples appear while a machine is
        processing.
      </Empty>
    );
  const width = 900,
    height = 230,
    left = 58,
    right = 20,
    top = 25,
    bottom = 35;
  const values = samples.map((sample) => sample.value);
  const low = Math.min(...values),
    high = Math.max(...values),
    pad = Math.max((high - low) * 0.15, 0.5);
  const minimum = low - pad,
    maximum = high + pad;
  const firstTime = Date.parse(samples[0].ts_utc),
    lastTime = Date.parse(samples[samples.length - 1].ts_utc);
  const x = (sample: Telemetry, index: number) =>
    left +
    (lastTime > firstTime
      ? (Date.parse(sample.ts_utc) - firstTime) / (lastTime - firstTime)
      : index / Math.max(1, samples.length - 1)) *
      (width - left - right);
  const y = (value: number) =>
    top +
    (1 - (value - minimum) / (maximum - minimum)) * (height - top - bottom);
  const points = samples
    .map((sample, index) => `${x(sample, index)},${y(sample.value)}`)
    .join(" ");
  const latest = samples[samples.length - 1];
  return (
    <div className="telemetry-chart">
      <div className="chart-reading">
        <strong>
          {latest.value.toFixed(2)} <small>{latest.unit}</small>
        </strong>
        <span>Latest reading · {timestamp(latest.ts_utc)}</span>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${latest.sensor} for ${latest.machine_id}: ${samples.length} samples, minimum ${low}, maximum ${high} ${latest.unit}`}
      >
        <defs>
          <linearGradient id="chart-area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="#81c4ac" stopOpacity="0.2" />
            <stop offset="100%" stopColor="#81c4ac" stopOpacity="0" />
          </linearGradient>
        </defs>
        {Array.from({ length: 5 }, (_, index) => {
          const value = minimum + ((maximum - minimum) * index) / 4;
          return (
            <g key={index}>
              <line
                x1={left}
                x2={width - right}
                y1={y(value)}
                y2={y(value)}
                stroke="#2b3531"
                strokeDasharray="4 5"
              />
              <text
                x={left - 12}
                y={y(value) + 4}
                textAnchor="end"
                fill="#7e9188"
                fontSize="11"
              >
                {value.toFixed(1)}
              </text>
            </g>
          );
        })}
        <polygon
          points={`${left},${height - bottom} ${points} ${x(latest, samples.length - 1)},${height - bottom}`}
          fill="url(#chart-area)"
        />
        <polyline
          points={points}
          fill="none"
          stroke="#8bcfb5"
          strokeWidth="2.5"
          strokeLinejoin="round"
        />
        <circle
          cx={x(latest, samples.length - 1)}
          cy={y(latest.value)}
          r="4"
          fill="#8bcfb5"
        />
        <text x={left} y={height - 6} fill="#7e9188" fontSize="11">
          {timestamp(samples[0].ts_utc)}
        </text>
        <text
          x={width - right}
          y={height - 6}
          textAnchor="end"
          fill="#7e9188"
          fontSize="11"
        >
          {timestamp(latest.ts_utc)}
        </text>
      </svg>
    </div>
  );
}

export function EventTable({ events }: { events: ProductionEvent[] }) {
  return events.length ? (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Event</th>
            <th>Machine</th>
            <th>Batch</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody>
          {[...events]
            .sort((a, b) => Date.parse(b.ts_utc) - Date.parse(a.ts_utc))
            .map((event, index) => (
              <tr key={event.event_id ?? index}>
                <td className="dim">
                  {timestamp(event.ts_utc)}
                  <small>
                    {event.sim_time_h === undefined
                      ? ""
                      : simClock(event.sim_time_h)}
                  </small>
                </td>
                <td>
                  <span
                    className={`quiet-tag ${/STOP|SPOIL|FLUSH|REJECT/.test(event.event) ? "danger" : ""}`}
                  >
                    {humanize(event.event)}
                  </span>
                </td>
                <td>{event.machine_id ?? "Scheduler"}</td>
                <td className="mono" title={event.batch_id ?? ""}>
                  {shortId(event.batch_id)}
                </td>
                <td>
                  {event.reason ||
                    (event.snapshot?.phase
                      ? humanize(event.snapshot.phase)
                      : "")}
                </td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  ) : (
    <Empty title="No recorded events">
      Events will appear after the machine starts work.
    </Empty>
  );
}

export function ExperimentView({
  data,
  perform,
  confirm,
  canAct,
}: {
  data: FactorySnapshot;
  perform: Perform;
  confirm: Confirm;
  canAct: boolean;
}) {
  const [runId, setRunId] = useState(
    () =>
      `run-${new Date().toISOString().slice(0, 10)}-${crypto.randomUUID().slice(0, 6)}`,
  );
  const [policy, setPolicy] = useState("CONWIP");
  const [wipLimit, setWipLimit] = useState(20);
  const [horizon, setHorizon] = useState(2160);
  const [busy, setBusy] = useState(false);
  const experiment = data.experiment;
  const running = !!experiment?.run_id && !experiment.done;
  const start = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    try {
      await perform(
        "/experiment/start",
        { run_id: runId, policy, wip_limit: wipLimit, horizon_h: horizon },
        "Experiment started.",
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <>
      <PageHeading
        eyebrow="ARCHITECTURE EVALUATION"
        title="Experiments"
        description="Observe production outcomes under controlled scheduling policies."
      />
      <div className="metrics-grid">
        {[
          ["Completed", experiment?.completed ?? 0, "batches"],
          ["Output", experiment?.hl_out ?? 0, "hL"],
          ["Material loss", experiment?.hl_wasted ?? 0, "hL"],
          [
            "Elapsed",
            experiment
              ? duration(experiment.elapsed_h ?? experiment.sim_clock_h)
              : "-",
            "simulated time",
          ],
        ].map(([label, value, unit]) => (
          <div className="metric" key={label}>
            <div className="metric-top">
              <span>{label}</span>
              <Icon name="experiment" />
            </div>
            <strong>{value}</strong>
            <small>{unit}</small>
          </div>
        ))}
      </div>
      <div className="two-columns">
        <section className="panel">
          <div className="panel-heading">
            <div>
              <h2>
                {running ? "Experiment in progress" : "Start an experiment"}
              </h2>
              <p>
                {running
                  ? experiment.run_id
                  : "Start with an empty line and no outstanding orders."}
              </p>
            </div>
            <span className={`quiet-tag ${running ? "ready" : ""}`}>
              {running ? "Running" : "Idle"}
            </span>
          </div>
          <form className="panel-form" onSubmit={start}>
            <label className="field">
              <span>Run identifier</span>
              <input
                required
                pattern="[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"
                maxLength={128}
                value={runId}
                onChange={(event) => setRunId(event.target.value)}
                disabled={running}
              />
            </label>
            <div className="form-grid">
              <label className="field">
                <span>Admission policy</span>
                <select
                  value={policy}
                  onChange={(event) => setPolicy(event.target.value)}
                  disabled={running}
                >
                  <option value="CONWIP">
                    CONWIP · bounded work in progress
                  </option>
                  <option value="PUSH">PUSH · admit when available</option>
                </select>
              </label>
              <NumberField
                label="WIP limit"
                value={wipLimit}
                min={1}
                step="1"
                onChange={setWipLimit}
              />
              <NumberField
                label="Run horizon"
                value={horizon}
                min={1}
                suffix="sim h"
                onChange={setHorizon}
              />
            </div>
            {!running && (data.line?.active_wip ?? 0) > 0 && (
              <p className="form-note">
                Wait for the {data.line?.active_wip} active batches to leave the
                line before starting another run.
              </p>
            )}
            {running ? (
              <button
                type="button"
                className="button secondary"
                disabled={!canAct}
                onClick={() =>
                  confirm(
                    "End this experiment?",
                    "New admissions will stop. Active batches continue; use global STOP to pause all production.",
                    "End experiment",
                    () =>
                      perform(
                        "/experiment/stop",
                        {},
                        "Experiment ended; new admissions stopped.",
                      ),
                  )
                }
              >
                <Icon name="stop" />
                End experiment
              </button>
            ) : (
              <button
                className="button primary"
                disabled={!canAct || busy || (data.line?.active_wip ?? 0) > 0}
              >
                <Icon name="play" />
                {busy ? "Starting…" : "Start experiment"}
              </button>
            )}
          </form>
        </section>
        <section className="panel explanation-panel">
          <span className="section-icon">
            <Icon name="experiment" size={28} />
          </span>
          <h2>Compare the same demand</h2>
          <p>
            Use the experiment runner for isolated, reproducible trials with
            matching order schedules, recipes and random seeds.
          </p>
          <p>
            Operator discards and scheduler-induced spoilage are reported
            separately. A low loss figure is meaningful alongside completed
            production and outstanding work.
          </p>
          <dl className="detail-list">
            <div>
              <dt>Current policy</dt>
              <dd>
                {experiment?.policy ?? data.line?.policy ?? "Unavailable"}
              </dd>
            </div>
            <div>
              <dt>Spoiled batches</dt>
              <dd>{experiment?.spoiled ?? 0}</dd>
            </div>
            <div>
              <dt>Operator discards</dt>
              <dd>{experiment?.operator_discarded ?? 0}</dd>
            </div>
          </dl>
        </section>
      </div>
    </>
  );
}

export function BatchHistory({
  batch,
  close,
  flush,
  canAct,
}: {
  batch: Batch;
  close: () => void;
  flush: () => void;
  canAct: boolean;
}) {
  const [events, setEvents] = useState<ProductionEvent[]>([]);
  const [error, setError] = useState("");
  useEffect(() => {
    let disposed = false;
    void request<ProductionEvent[]>(
      `/events?batch_id=${encodeURIComponent(batch.id)}&limit=500`,
    )
      .then((value) => {
        if (!disposed) setEvents(value);
      })
      .catch((error) => {
        if (!disposed) setError(errorMessage(error));
      });
    return () => {
      disposed = true;
    };
  }, [batch.id]);
  return (
    <>
      <div className="batch-summary">
        <span className="quiet-tag">{humanize(batch.status)}</span>
        <span>{batch.recipe_id} · 50 hL</span>
        <span className="mono">{batch.machine_id ?? "No machine"}</span>
      </div>
      <dl className="detail-list compact">
        <div>
          <dt>Batch ID</dt>
          <dd className="mono">{batch.id}</dd>
        </div>
        <div>
          <dt>Recipe version</dt>
          <dd className="mono">{batch.recipe_version_id}</dd>
        </div>
        <div>
          <dt>Created</dt>
          <dd>{timestamp(batch.created_at)}</dd>
        </div>
        <div>
          <dt>Completed</dt>
          <dd>{timestamp(batch.completed_at ?? undefined)}</dd>
        </div>
      </dl>
      {error && <p className="inline-error">{error}</p>}
      <EventTable events={events} />
      <div className="dialog-actions">
        <button className="button secondary" onClick={close}>
          Close
        </button>
        {!["COMPLETED", "DISCARDED"].includes(batch.status) && (
          <button
            className="button danger-outline"
            disabled={!canAct}
            onClick={flush}
          >
            <Icon name="drop" />
            Discard this batch
          </button>
        )}
      </div>
    </>
  );
}

export function PageHeading({
  eyebrow,
  title,
  description,
  action,
}: {
  eyebrow: string;
  title: string;
  description: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="section-heading">
      <div>
        <div className="eyebrow">{eyebrow}</div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {action}
    </div>
  );
}
