import { humanize, stageLabel } from "../domain";
import { Empty, Icon, shortId, timestamp } from "../ui";
import type { FactorySnapshot } from "../types";
import type { Confirm, Perform } from "../actions";
import { PageHeading } from "./PageHeading";

/** Show demand and admitted batches; cancelling demand leaves active production intact. */
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
