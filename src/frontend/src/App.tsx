import { useEffect, useRef, useState } from "react";
import { ApiError, errorMessage, mutation } from "./api";
import {
  machineLabel,
  machineTone,
  progress,
  duration,
  humanize,
  simClock,
} from "./domain";
import { useFactory } from "./useFactory";
import { Operations } from "./Operations";
import {
  BatchHistory,
  ExperimentView,
  HistoryView,
  InventoryView,
  OrdersView,
  RecipesView,
} from "./Views";
import type { Confirm, Perform } from "./Views";
import { OrderForm, RecipeForm } from "./forms";
import { Dialog, Icon, timestamp } from "./ui";
import type { Recipe } from "./types";

type Tab =
  "floor" | "orders" | "recipes" | "history" | "inventory" | "experiments";
const tabs: { id: Tab; label: string; icon: string }[] = [
  { id: "floor", label: "Factory floor", icon: "grid" },
  { id: "orders", label: "Orders & batches", icon: "orders" },
  { id: "recipes", label: "Recipes", icon: "recipe" },
  { id: "history", label: "Telemetry & history", icon: "chart" },
  { id: "inventory", label: "Inventory", icon: "box" },
  { id: "experiments", label: "Experiments", icon: "experiment" },
];
interface PendingRequest {
  path: string;
  body: unknown;
  success: string;
  operator: string;
  key: string;
}
interface Confirmation {
  title: string;
  message: string;
  label: string;
  action: () => Promise<boolean>;
  destructive: boolean;
}

export default function App() {
  const { data, errors, lastUpdate, connection, telemetry, refresh } =
    useFactory();
  const [tab, setTab] = useState<Tab>("floor");
  const [now, setNow] = useState(Date.now());
  const [operator, setOperator] = useState(() => {
    try {
      return localStorage.getItem("beer-factory-operator") || "operator";
    } catch {
      return "operator";
    }
  });
  const [orderOpen, setOrderOpen] = useState(false);
  const [recipeEdit, setRecipeEdit] = useState<{ recipe?: Recipe } | null>(
    null,
  );
  const [machineId, setMachineId] = useState<string | null>(null);
  const [batchId, setBatchId] = useState<string | null>(null);
  const [confirmation, setConfirmation] = useState<Confirmation | null>(null);
  const [busy, setBusy] = useState(0);
  const [confirming, setConfirming] = useState(false);
  const [notice, setNotice] = useState<{ text: string; error: boolean } | null>(
    null,
  );
  const [retries, setRetries] = useState<PendingRequest[]>([]);
  const toastRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    try {
      localStorage.setItem("beer-factory-operator", operator);
    } catch {
      /* Optional browser preference. */
    }
  }, [operator]);
  useEffect(() => {
    if (!notice || notice.error) return;
    const timer = window.setTimeout(() => setNotice(null), 6000);
    return () => clearTimeout(timer);
  }, [notice]);
  useEffect(() => {
    if (notice) toastRef.current?.showPopover();
  }, [notice]);
  const stale = !lastUpdate || now - lastUpdate > 10000;
  const healthy = !stale && !errors.machines && !errors.line;
  const canAct = !!healthy && busy === 0 && retries.length === 0;
  const selectedMachine = data.machines.find(
    (machine) => machine.machine_id === machineId,
  );
  const selectedBatch = data.batches.find((batch) => batch.id === batchId);
  const pendingStops = Array.isArray(data.line?.machines_pending_stop)
    ? data.line.machines_pending_stop.length
    : (data.line?.machines_pending_stop ?? 0);

  async function execute(command: PendingRequest): Promise<boolean> {
    setBusy((value) => value + 1);
    try {
      await mutation(command.path, command.body, command.operator, command.key);
      setRetries((previous) =>
        previous.filter((item) => item.key !== command.key),
      );
      setNotice({ text: command.success, error: false });
      void refresh();
      return true;
    } catch (error) {
      const uncertain = !(error instanceof ApiError) || error.status >= 500;
      if (uncertain) {
        setRetries((previous) =>
          previous.some((item) => item.key === command.key)
            ? previous
            : [...previous, command],
        );
        setOrderOpen(false);
        setRecipeEdit(null);
        setConfirmation(null);
      } else
        setRetries((previous) =>
          previous.filter((item) => item.key !== command.key),
        );
      setNotice({
        text: `${errorMessage(error)}${uncertain ? " Use Retry to safely repeat this exact operation." : ""}`,
        error: true,
      });
      return false;
    } finally {
      setBusy((value) => value - 1);
    }
  }
  const perform: Perform = (path, body, success) =>
    execute({ path, body, success, operator, key: crypto.randomUUID() });
  const confirm: Confirm = (
    title,
    message,
    label,
    action,
    destructive = false,
  ) => setConfirmation({ title, message, label, action, destructive });
  const flushMachine = () => {
    if (!selectedMachine?.batch_id) return;
    const machine = selectedMachine,
      batch = machine.batch_id;
    setMachineId(null);
    confirm(
      "Flush this batch?",
      `Discard all 50 hL from batch ${batch} in ${machine.machine_id}. This records an operator discard. The command applies only to this selected batch.`,
      "Flush 50 hL",
      () =>
        perform(
          `/machines/${encodeURIComponent(machine.machine_id)}/flush`,
          { batch_id: batch },
          "Flush requested for the selected batch.",
        ),
      true,
    );
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          onClick={(event) => {
            event.preventDefault();
            setTab("floor");
          }}
        >
          <span className="brand-mark">
            bf<span>•</span>
          </span>
          <div>
            BEER FACTORY<small>Production control</small>
          </div>
        </a>
        <div className="nav-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          {tabs.map((item) => (
            <button
              key={item.id}
              className={`nav-item ${tab === item.id ? "selected" : ""}`}
              onClick={() => setTab(item.id)}
              aria-current={tab === item.id ? "page" : undefined}
            >
              <Icon name={item.icon} />
              <span>{item.label}</span>
              {item.id === "orders" &&
                data.orders.filter((order) => order.status === "QUEUED")
                  .length > 0 && (
                  <span className="nav-count">
                    {
                      data.orders.filter((order) => order.status === "QUEUED")
                        .length
                    }
                  </span>
                )}
            </button>
          ))}
        </nav>
        <div className="sidebar-footer">
          <span className="sidebar-emblem">
            <Icon name="recipe" size={32} />
          </span>
          <strong>From grain to glass.</strong>
          <p>Six stages. One connected line.</p>
          <span className="quiet-tag">50 hL / batch</span>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <div className="breadcrumb">
            Workspace <span>/</span>
            <strong>{tabs.find((item) => item.id === tab)?.label}</strong>
          </div>
          <div className="topbar-controls">
            <span
              className={`connection ${stale ? "offline" : connection === "live" ? "active" : "ready"}`}
            >
              <i className="status-dot" />
              {stale
                ? "Disconnected"
                : connection === "live"
                  ? "Live"
                  : "Polling"}
            </span>
            <span className="clock-display">
              <Icon name="clock" size={15} />
              {simClock(data.line?.sim_time_h)}
            </span>
            <button
              className="stop-button"
              onClick={() =>
                void perform(
                  "/line/stop",
                  {},
                  "Global STOP requested. Waiting for machine acknowledgements.",
                )
              }
            >
              <Icon name="stop" size={19} />
              <span>STOP LINE</span>
            </button>
          </div>
        </header>
        <main>
          {data.line?.stopped && (
            <div className="line-stopped">
              <Icon name="stop" />
              <div>
                <strong>Global STOP is active</strong>
                <span>
                  {pendingStops
                    ? `${pendingStops} machines have not acknowledged STOP yet.`
                    : "Machines retain their batches. Holding and interruption limits continue to age."}
                </span>
              </div>
              <button
                className="button danger-outline"
                disabled={!canAct}
                onClick={() =>
                  confirm(
                    "Resume the production line?",
                    "Global STOP will be cleared. Machines with a local STOP remain stopped until resumed individually.",
                    "Resume line",
                    () => perform("/line/resume", {}, "Line resume requested."),
                  )
                }
              >
                <Icon name="play" />
                Resume line
              </button>
            </div>
          )}
          {(stale || Object.keys(errors).length > 0) && (
            <div className="connection-banner" role="status">
              <Icon name="warning" />
              <div>
                <strong>
                  {!lastUpdate
                    ? "Connecting to production services"
                    : stale
                      ? "Production data is stale"
                      : "Some services are unavailable"}
                </strong>
                <span>
                  {Object.keys(errors).length
                    ? Object.entries(errors)
                        .map(([key, value]) => `${humanize(key)}: ${value}`)
                        .join(" · ")
                    : "Waiting for a fresh machine and scheduler snapshot."}
                </span>
              </div>
              <button
                className="icon-button"
                aria-label="Refresh production data"
                onClick={() => void refresh()}
              >
                <Icon name="refresh" />
              </button>
            </div>
          )}
          {retries.map((retry) => (
            <div className="retry-banner" role="alert" key={retry.key}>
              <Icon name="warning" />
              <div>
                <strong>Operation awaiting confirmation</strong>
                <span>
                  {retry.path} · request {retry.key.slice(0, 8)}. Retry uses the
                  original request ID and payload.
                </span>
              </div>
              <button
                className="button secondary"
                disabled={busy > 0}
                onClick={() => void execute(retry)}
              >
                Retry operation
              </button>
            </div>
          ))}
          {tab === "floor" && (
            <Operations
              data={data}
              selectMachine={setMachineId}
              createOrder={() => setOrderOpen(true)}
              openOrders={() => setTab("orders")}
              loaded={lastUpdate !== null}
              canAct={canAct}
            />
          )}
          {tab === "orders" && (
            <OrdersView
              data={data}
              createOrder={() => setOrderOpen(true)}
              confirm={confirm}
              perform={perform}
              viewBatch={setBatchId}
              canAct={canAct}
            />
          )}
          {tab === "recipes" && (
            <RecipesView
              recipes={data.recipes}
              edit={(recipe) => setRecipeEdit({ recipe })}
              canAct={canAct}
            />
          )}
          {tab === "history" && (
            <HistoryView data={data} liveSamples={telemetry} />
          )}
          {tab === "inventory" && (
            <InventoryView
              data={data}
              perform={perform}
              canAct={canAct && !errors.inventory}
            />
          )}
          {tab === "experiments" && (
            <ExperimentView
              data={data}
              perform={perform}
              confirm={confirm}
              canAct={canAct}
            />
          )}
          <footer className="page-footer">
            <span>
              BEER FACTORY <span className="footer-dot">·</span> Production
              workspace
            </span>
            <label>
              Operator{" "}
              <input
                aria-label="Operator name for audit records"
                maxLength={80}
                value={operator}
                onChange={(event) => setOperator(event.target.value)}
              />
            </label>
            <span>
              {lastUpdate
                ? `Updated ${new Date(lastUpdate).toLocaleTimeString()}`
                : "No snapshot received"}
            </span>
          </footer>
        </main>
      </div>
      {notice && (
        <div
          ref={toastRef}
          popover="manual"
          className={`toast ${notice.error ? "error" : ""}`}
          role={notice.error ? "alert" : "status"}
        >
          <Icon name={notice.error ? "warning" : "check"} />
          <span>{notice.text}</span>
          <button
            className="icon-button"
            aria-label="Dismiss notification"
            onClick={() => setNotice(null)}
          >
            <Icon name="close" size={16} />
          </button>
        </div>
      )}
      {orderOpen && (
        <Dialog
          title="Create a production order"
          description="Each batch contains 50 hL. Ingredients are reserved when production begins."
          onClose={() => setOrderOpen(false)}
        >
          <OrderForm
            recipes={data.recipes}
            perform={perform}
            canAct={canAct}
            close={() => setOrderOpen(false)}
          />
        </Dialog>
      )}
      {recipeEdit && (
        <Dialog
          title={
            recipeEdit.recipe
              ? `New version · ${recipeEdit.recipe.name}`
              : "Create a recipe"
          }
          description="All phases, holding limits and transfers use simulated hours."
          onClose={() => setRecipeEdit(null)}
          wide
        >
          <RecipeForm
            recipe={recipeEdit.recipe}
            perform={perform}
            canAct={canAct}
            close={() => setRecipeEdit(null)}
          />
        </Dialog>
      )}
      {selectedMachine && (
        <Dialog
          title={selectedMachine.machine_id}
          description="Machine status and operator controls"
          onClose={() => setMachineId(null)}
          sheet
        >
          <div className="machine-detail-header">
            <span className={`quiet-tag ${machineTone(selectedMachine)}`}>
              <i className="status-dot" />
              {machineLabel(selectedMachine)}
            </span>
            <span>{humanize(selectedMachine.type)} · 50 hL</span>
          </div>
          <div className="machine-phase">
            <div>
              <span>Current phase</span>
              <strong>
                {selectedMachine.phase
                  ? humanize(selectedMachine.phase)
                  : "No active phase"}
              </strong>
            </div>
            <strong>{Math.round(progress(selectedMachine))}%</strong>
          </div>
          <div className="progress-track large">
            <span style={{ width: `${progress(selectedMachine)}%` }} />
          </div>
          <dl className="detail-list">
            <div>
              <dt>Batch</dt>
              <dd className="mono">{selectedMachine.batch_id ?? "Empty"}</dd>
            </div>
            <div>
              <dt>State</dt>
              <dd>{humanize(selectedMachine.state)}</dd>
            </div>
            <div>
              <dt>Phase time remaining</dt>
              <dd>
                {duration(
                  Math.max(
                    0,
                    selectedMachine.phase_duration_h -
                      selectedMachine.phase_elapsed_h,
                  ),
                )}
              </dd>
            </div>
            <div>
              <dt>Holding deadline</dt>
              <dd>
                {selectedMachine.hold_deadline_h
                  ? duration(
                      selectedMachine.hold_deadline_h -
                        selectedMachine.sim_time_h,
                    )
                  : "Not holding"}
              </dd>
            </div>
            <div>
              <dt>Recipe version</dt>
              <dd className="mono">
                {selectedMachine.recipe_version_id ?? "None"}
              </dd>
            </div>
            <div>
              <dt>Transfer</dt>
              <dd className="mono">
                {selectedMachine.transfer_id ?? "No transfer"}
              </dd>
            </div>
            <div>
              <dt>Last seen</dt>
              <dd>{timestamp(selectedMachine.last_seen)}</dd>
            </div>
          </dl>
          <p className="form-note">
            STOP retains the batch. Resume clears the local STOP; global STOP
            must be released separately.
          </p>
          <div className="machine-actions">
            <button
              className="button danger-outline"
              disabled={
                !canAct || !selectedMachine.online || selectedMachine.local_stop
              }
              onClick={() =>
                void perform(
                  `/machines/${encodeURIComponent(selectedMachine.machine_id)}/stop`,
                  {},
                  `${selectedMachine.machine_id}: local STOP requested.`,
                )
              }
            >
              <Icon name="stop" />
              Stop machine
            </button>
            <button
              className="button secondary"
              disabled={
                !canAct ||
                !selectedMachine.online ||
                !selectedMachine.local_stop
              }
              onClick={() =>
                void perform(
                  `/machines/${encodeURIComponent(selectedMachine.machine_id)}/resume`,
                  {},
                  `${selectedMachine.machine_id}: resume requested.`,
                )
              }
            >
              <Icon name="play" />
              Resume machine
            </button>
            <button
              className="button danger-outline full"
              disabled={
                !canAct || !selectedMachine.batch_id || !selectedMachine.online
              }
              onClick={flushMachine}
            >
              <Icon name="drop" />
              Flush selected batch
            </button>
          </div>
        </Dialog>
      )}
      {selectedBatch && (
        <Dialog
          title={`Batch · ${selectedBatch.id}`}
          description="Trace a batch across every machine and transfer."
          onClose={() => setBatchId(null)}
          wide
        >
          <BatchHistory
            batch={selectedBatch}
            close={() => setBatchId(null)}
            canAct={canAct}
            flush={() => {
              const id = selectedBatch.id;
              setBatchId(null);
              confirm(
                "Discard this batch?",
                `Discard all 50 hL from batch ${id}. The scheduler will flush the machine holding this batch.`,
                "Discard 50 hL",
                () =>
                  perform(
                    `/batches/${encodeURIComponent(id)}/flush`,
                    {},
                    "Batch discard requested.",
                  ),
                true,
              );
            }}
          />
        </Dialog>
      )}
      {confirmation && (
        <Dialog
          title={confirmation.title}
          description={confirmation.message}
          onClose={() => {
            if (!confirming) setConfirmation(null);
          }}
        >
          <div className="dialog-actions">
            <button
              className="button secondary"
              disabled={confirming}
              onClick={() => setConfirmation(null)}
            >
              Cancel
            </button>
            <button
              className={`button ${confirmation.destructive ? "danger-solid" : "primary"}`}
              disabled={confirming || !canAct}
              onClick={async () => {
                setConfirming(true);
                try {
                  if (await confirmation.action()) setConfirmation(null);
                } finally {
                  setConfirming(false);
                }
              }}
            >
              {confirming ? "Sending…" : confirmation.label}
            </button>
          </div>
        </Dialog>
      )}
    </div>
  );
}
