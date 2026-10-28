import { useState } from "react";
import type { FormEvent } from "react";
import { duration } from "../domain";
import { Icon, NumberField } from "../ui";
import type { FactorySnapshot } from "../types";
import type { Confirm, Perform } from "../actions";
import { PageHeading } from "./PageHeading";

/** Start isolated runs and show counters; ending a run stops admission, not active machines. */
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
