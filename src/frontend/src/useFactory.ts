import { useCallback, useEffect, useRef, useState } from "react";
import { errorMessage, request } from "./api";
import type { FactorySnapshot, Telemetry } from "./types";

const initial: FactorySnapshot = {
  machines: [],
  recipes: [],
  orders: [],
  batches: [],
  inventory: null,
  line: null,
  events: [],
  experiment: null,
};
const endpoints = {
  machines: "/machines",
  recipes: "/recipes",
  orders: "/orders",
  batches: "/batches",
  inventory: "/inventory",
  line: "/line",
  events: "/events?limit=150&order=desc",
  experiment: "/experiment/status",
} as const;

/** Keep the dashboard synchronized without making WebSocket delivery authoritative.
 * REST refreshes supply complete snapshots every three seconds. WebSocket events
 * trigger quicker refreshes and feed a bounded chart buffer. A failed endpoint
 * retains its last value and records an error; only fresh machine AND line data
 * advance lastUpdate, which the app uses to inhibit stale operator actions. */
export function useFactory() {
  const [data, setData] = useState<FactorySnapshot>(initial);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [lastUpdate, setLastUpdate] = useState<number | null>(null);
  const [connection, setConnection] = useState<
    "connecting" | "live" | "polling"
  >("connecting");
  const [telemetry, setTelemetry] = useState<Telemetry[]>([]);
  const alive = useRef(true);
  const loading = useRef(false);
  const queued = useRef(false);

  // Coalesce overlapping timer/event requests into one follow-up snapshot fetch.
  const refresh = useCallback(async () => {
    if (loading.current) {
      queued.current = true;
      return;
    }
    loading.current = true;
    try {
      const entries = Object.entries(endpoints) as [
        keyof FactorySnapshot,
        string,
      ][];
      const results = await Promise.allSettled(
        entries.map(([, path]) => request(path)),
      );
      if (!alive.current) return;
      const patch: Partial<FactorySnapshot> = {};
      const nextErrors: Record<string, string> = {};
      results.forEach((result, index) => {
        const key = entries[index][0];
        if (result.status === "fulfilled") {
          const collection = [
            "machines",
            "recipes",
            "orders",
            "batches",
            "events",
          ].includes(key);
          if (collection && !Array.isArray(result.value))
            nextErrors[key] = "Unexpected collection response";
          else Object.assign(patch, { [key]: result.value });
        } else nextErrors[key] = errorMessage(result.reason);
      });
      setData((previous) => ({ ...previous, ...patch }));
      setErrors(nextErrors);
      if ("machines" in patch && "line" in patch) setLastUpdate(Date.now());
    } finally {
      loading.current = false;
      if (queued.current && alive.current) {
        queued.current = false;
        void refresh();
      }
    }
  }, []);

  useEffect(() => {
    // Per-effect lifetime prevents an old socket from reconnecting after unmount
    // or React StrictMode's development teardown/restart cycle.
    let disposed = false;
    alive.current = true;
    void refresh();
    const poll = window.setInterval(() => {
      void refresh();
    }, 3000);
    let socket: WebSocket | undefined;
    let reconnect: number | undefined;
    let updateTimer: number | undefined;
    let attempts = 0;
    // Reconnect with capped backoff while independent REST polling continues.
    const connect = () => {
      if (disposed) return;
      const current = new WebSocket(
        `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/ws`,
      );
      socket = current;
      current.onopen = () => {
        if (disposed) return;
        attempts = 0;
        setConnection("live");
        void refresh();
      };
      current.onmessage = (message) => {
        if (disposed) return;
        try {
          const payload = JSON.parse(message.data) as {
            type?: string;
            data?: unknown;
          };
          const item = payload.data as Telemetry | undefined;
          if (
            item &&
            typeof item.machine_id === "string" &&
            typeof item.sensor === "string" &&
            Number.isFinite(item.value)
          ) {
            setTelemetry((previous) => [...previous, item].slice(-1800));
          }
          if (updateTimer === undefined)
            updateTimer = window.setTimeout(() => {
              updateTimer = undefined;
              void refresh();
            }, 500);
        } catch {
          /* The polling snapshot restores malformed or missed live updates. */
        }
      };
      current.onclose = () => {
        if (disposed) return;
        setConnection("polling");
        reconnect = window.setTimeout(
          connect,
          Math.min(15000, 1000 * 2 ** attempts++),
        );
      };
      current.onerror = () => current.close();
    };
    connect();
    return () => {
      disposed = true;
      alive.current = false;
      window.clearInterval(poll);
      window.clearTimeout(reconnect);
      window.clearTimeout(updateTimer);
      socket?.close();
    };
  }, [refresh]);

  return { data, errors, lastUpdate, connection, telemetry, refresh };
}
