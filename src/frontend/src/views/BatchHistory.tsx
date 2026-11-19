import { useEffect, useState } from "react";
import { errorMessage, request } from "../api";
import { humanize } from "../domain";
import { Icon, timestamp } from "../ui";
import type { Batch, ProductionEvent } from "../types";
import { EventTable } from "./EventTable";

/** Fetch the selected batch trace and offer an explicit discard of that same batch ID. */
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
      `/events?batch_id=${encodeURIComponent(batch.id)}&limit=500&order=desc`,
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
