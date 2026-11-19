import { humanize, simClock } from "../domain";
import { Empty, shortId, timestamp } from "../ui";
import type { ProductionEvent } from "../types";

/** Render canonical machine/operator events with newest activity first. */
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
