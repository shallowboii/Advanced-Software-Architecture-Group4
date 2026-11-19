import { useEffect, useMemo, useState } from "react";
import { errorMessage, request } from "../api";
import { humanize, orderedSamples } from "../domain";
import type { FactorySnapshot, ProductionEvent, Telemetry } from "../types";
import { PageHeading } from "./PageHeading";
import { TelemetryChart } from "./TelemetryChart";
import { EventTable } from "./EventTable";

/** Combine recent persisted samples with live telemetry, then sort by measurement time. */
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
      // Fetch the newest bounded window; orderedSamples restores chronological
      // order after merging REST results with WebSocket readings.
      const params = new URLSearchParams({
        machine_id: machineId,
        sensor,
        limit: "1000",
        order: "desc",
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
            `/events?machine_id=${encodeURIComponent(machineId)}&limit=100&order=desc`,
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
