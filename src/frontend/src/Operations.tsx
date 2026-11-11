import type { CSSProperties } from "react";
import {
  duration,
  humanize,
  machineLabel,
  machineTone,
  progress,
} from "./domain";
import { Empty, Icon, shortId, timestamp } from "./ui";
import { STAGES, STAGE_LABEL } from "./types";
import type { FactorySnapshot, Machine } from "./types";

/** Summarize the six-stage line and render each registered unitank from authoritative snapshots. */
export function Operations({
  data,
  selectMachine,
  createOrder,
  openOrders,
  loaded,
  canAct,
}: {
  data: FactorySnapshot;
  selectMachine: (id: string) => void;
  createOrder: () => void;
  openOrders: () => void;
  loaded: boolean;
  canAct: boolean;
}) {
  const tanks = data.machines.filter((machine) => machine.type === "unitank");
  const online = data.machines.filter((machine) => machine.online).length;
  const processing = data.machines.filter(
    (machine) =>
      machine.state === "PROCESSING" && !machine.stopped && machine.online,
  ).length;
  const completed = data.batches.filter(
    (batch) => batch.status === "COMPLETED",
  );
  const lost = data.batches.filter((batch) =>
    ["SPOILED", "DISCARDED", "FLUSH_PENDING"].includes(batch.status),
  );
  const pendingOrders = data.orders.filter(
    (order) => !["COMPLETED", "FINISHED", "CANCELLED"].includes(order.status),
  );
  const activeTransfers = [
    ...new Set(
      data.machines.map((machine) => machine.transfer_id).filter(Boolean),
    ),
  ];
  const output = data.experiment?.run_id
    ? data.experiment.hl_out
    : completed.reduce((sum, batch) => sum + batch.volume_hl, 0);
  const waste = data.experiment?.run_id
    ? data.experiment.hl_wasted
    : lost.reduce((sum, batch) => sum + batch.volume_hl, 0);

  return (
    <>
      <div className="section-heading">
        <div>
          <div className="eyebrow">LIVE PRODUCTION</div>
          <h1>The factory floor</h1>
          <p>Every batch, machine and handover in one place.</p>
        </div>
        <button
          className="button primary"
          onClick={createOrder}
          disabled={!canAct || !data.recipes.length}
        >
          <Icon name="plus" /> New order
        </button>
      </div>
      <div className="metrics-grid">
        <Metric
          label="Machines online"
          value={loaded ? `${online}/${data.machines.length}` : "-"}
          detail={`${processing} processing now`}
          icon="grid"
        />
        <Metric
          label="Work in progress"
          value={loaded ? String(data.line?.active_wip ?? 0) : "-"}
          detail={`${data.line?.policy ?? "Policy unavailable"} · limit ${data.line?.wip_limit ?? "-"}`}
          icon="drop"
        />
        <Metric
          label="Beer completed"
          value={loaded ? `${output.toLocaleString()} hL` : "-"}
          detail={`${completed.length} batches in this view`}
          icon="check"
        />
        <Metric
          label="Material loss"
          value={loaded ? `${waste.toLocaleString()} hL` : "-"}
          detail={
            data.experiment?.run_id
              ? "Current experiment"
              : "Discarded or spoiled batches"
          }
          icon="warning"
          danger={waste > 0}
        />
      </div>

      <section className="panel line-panel">
        <div className="panel-heading">
          <div>
            <h2>Production line</h2>
            <p>One batch per machine · 50 hL per batch</p>
          </div>
          <span className="quiet-tag">
            {activeTransfers.length} active transfers
          </span>
        </div>
        <div className="stage-flow">
          {STAGES.map((type, index) => {
            const machines = data.machines.filter(
              (machine) => machine.type === type,
            );
            const occupied = machines.filter(
              (machine) => !!machine.batch_id,
            ).length;
            return (
              <div className="stage-cell" key={type}>
                <div className="stage-number">
                  {String(index + 1).padStart(2, "0")}
                  {index < STAGES.length - 1 && <Icon name="arrow" size={15} />}
                </div>
                <h3>{type === "unitank" ? "Uni-tanks" : STAGE_LABEL[type]}</h3>
                <span className="stage-count">
                  {occupied}
                  <small> / {machines.length} occupied</small>
                </span>
                <div className="stage-machines">
                  {machines
                    .slice(0, type === "unitank" ? 0 : 6)
                    .map((machine) => (
                      <button
                        key={machine.machine_id}
                        className={`machine-chip ${machineTone(machine)}`}
                        onClick={() => selectMachine(machine.machine_id)}
                      >
                        <span className="status-dot" />
                        <span>{machine.machine_id}</span>
                        <span className="chip-status">
                          {machineLabel(machine)}
                        </span>
                      </button>
                    ))}
                  {type === "unitank" && (
                    <span className="stage-note">
                      Fermentation and maturation
                      <br />
                      in the same vessel
                    </span>
                  )}
                  {machines.length === 0 && (
                    <span className="stage-note">No machines connected</span>
                  )}
                </div>
              </div>
            );
          })}
        </div>
        <div className="flow-footer">
          <Icon name="arrow" size={16} />
          <span>
            Finished beer leaves the system after filtering. Transfers occupy
            both endpoints.
          </span>
        </div>
      </section>

      <div className="floor-columns">
        <section className="panel cellar-panel">
          <div className="panel-heading">
            <div>
              <h2>
                The cellar <span className="count-badge">{tanks.length}</span>
              </h2>
              <p>Each vessel holds one batch through both phases.</p>
            </div>
            <div className="legend">
              <span>
                <i className="active" />
                Fermenting
              </span>
              <span>
                <i className="maturation" />
                Maturing
              </span>
              <span>
                <i className="idle" />
                Available
              </span>
            </div>
          </div>
          {tanks.length ? (
            <div className="tank-grid">
              {tanks
                .sort((a, b) =>
                  a.machine_id.localeCompare(b.machine_id, undefined, {
                    numeric: true,
                  }),
                )
                .map((machine) => (
                  <Tank
                    key={machine.machine_id}
                    machine={machine}
                    onSelect={() => selectMachine(machine.machine_id)}
                  />
                ))}
            </div>
          ) : (
            <Empty title="Waiting for uni-tanks">
              Connected machines will appear here automatically.
            </Empty>
          )}
        </section>
        <section className="panel queue-panel">
          <div className="panel-heading">
            <div>
              <h2>Order queue</h2>
              <p>{pendingOrders.length} open orders</p>
            </div>
            <button className="text-button" onClick={openOrders}>
              View all <Icon name="arrow" size={14} />
            </button>
          </div>
          {pendingOrders.length ? (
            <div className="queue-list">
              {pendingOrders.slice(0, 5).map((order) => {
                const recipe = data.recipes.find(
                  (item) => item.id === order.recipe_id,
                );
                return (
                  <div className="queue-item" key={order.id}>
                    <div className="queue-item-top">
                      <strong>{recipe?.name ?? order.recipe_id}</strong>
                      <span className="quiet-tag">
                        {order.priority > 0
                          ? "Priority"
                          : `${order.quantity * 50} hL`}
                      </span>
                    </div>
                    <div className="progress-track">
                      <span
                        style={{
                          width: `${Math.min(100, ((order.completed + order.discarded) / Math.max(1, order.quantity)) * 100)}%`,
                        }}
                      />
                    </div>
                    <div className="queue-item-bottom">
                      <span>
                        {order.completed} of {order.quantity} completed
                      </span>
                      <span>{humanize(order.status)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <Empty title="The queue is clear">
              Create an order to schedule your next brew.
            </Empty>
          )}
        </section>
      </div>

      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>Recent activity</h2>
            <p>Production transitions and operator actions</p>
          </div>
          <span className="quiet-tag">Latest 8 events</span>
        </div>
        {data.events.length ? (
          <div className="activity-list">
            {[...data.events]
              .sort((a, b) => Date.parse(b.ts_utc) - Date.parse(a.ts_utc))
              .slice(0, 8)
              .map((event, index) => (
                <div className="activity-item" key={event.event_id ?? index}>
                  <span
                    className={`activity-dot ${/SPOIL|FLUSH|REJECT|STOP/.test(event.event) ? "danger" : ""}`}
                  />
                  <div>
                    <strong>{humanize(event.event)}</strong>
                    <span>
                      {event.machine_id ?? "Production scheduler"}
                      {event.batch_id ? ` · ${shortId(event.batch_id)}` : ""}
                      {event.reason ? ` · ${event.reason}` : ""}
                    </span>
                  </div>
                  <time>{timestamp(event.ts_utc)}</time>
                </div>
              ))}
          </div>
        ) : (
          <Empty title="No production events yet">
            Stage completions, transfers and interventions will be recorded
            here.
          </Empty>
        )}
      </section>
    </>
  );
}

/** Present a production counter with the context needed to interpret its scope. */
function Metric({
  label,
  value,
  detail,
  icon,
  danger = false,
}: {
  label: string;
  value: string;
  detail: string;
  icon: string;
  danger?: boolean;
}) {
  return (
    <div className={`metric ${danger ? "danger" : ""}`}>
      <div className="metric-top">
        <span>{label}</span>
        <Icon name={icon} size={17} />
      </div>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

/** Show one vessel, its retained batch phase and inhibition state, and open machine inspection. */
function Tank({
  machine,
  onSelect,
}: {
  machine: Machine;
  onSelect: () => void;
}) {
  const tone = machineTone(machine);
  const percent = progress(machine);
  return (
    <button
      className={`tank ${tone}`}
      onClick={onSelect}
      aria-label={`${machine.machine_id}: ${machineLabel(machine)}${machine.batch_id ? `, ${machine.phase}, ${Math.round(percent)} percent` : ""}`}
    >
      <span className="tank-label">
        {machine.machine_id.replace("unitank-", "T").replace(/^T(\d)$/, "T0$1")}
        <span className="status-dot" />
      </span>
      <span
        className="vessel"
        style={
          {
            "--fill": `${machine.batch_id ? Math.max(8, percent) : 0}%`,
          } as CSSProperties
        }
      >
        <span className="vessel-liquid" />
        <span className="vessel-icon">
          {machine.batch_id ? (
            `${Math.round(percent)}%`
          ) : (
            <Icon name="plus" size={16} />
          )}
        </span>
      </span>
      <span className="tank-state">
        {!machine.online || machine.stopped
          ? machineLabel(machine)
          : machine.phase
            ? humanize(machine.phase)
            : machineLabel(machine)}
      </span>
      <span className="tank-subtitle">
        {machine.batch_id
          ? duration(
              Math.max(0, machine.phase_duration_h - machine.phase_elapsed_h),
            ) + " left"
          : "50 hL capacity"}
      </span>
    </button>
  );
}
