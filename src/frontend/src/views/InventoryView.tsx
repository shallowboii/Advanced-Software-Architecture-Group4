import { useState } from "react";
import type { FormEvent } from "react";
import { Icon, NumberField } from "../ui";
import { INGREDIENTS } from "../types";
import type { FactorySnapshot, Ingredients } from "../types";
import type { Perform } from "../actions";
import { PageHeading } from "./PageHeading";
const ingredientNames: Record<keyof Ingredients, string> = {
  malt_kg: "Malt",
  hops_kg: "Hops",
  yeast_kg: "Yeast",
  water_l: "Water",
};

/** Display available ingredients and submit a delivery to the in-memory inventory mock. */
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
