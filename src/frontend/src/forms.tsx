import { useState } from "react";
import type { FormEvent } from "react";
import { newRecipe, validateRecipe, humanize } from "./domain";
import { NumberField } from "./ui";
import { INGREDIENTS, STAGE_LABEL } from "./types";
import type { Recipe } from "./types";
import type { Perform } from "./actions";

/** Submit recipe demand in fixed 50 hL batches; successful submission closes the form. */
export function OrderForm({
  recipes,
  perform,
  canAct,
  close,
}: {
  recipes: Recipe[];
  perform: Perform;
  canAct: boolean;
  close: () => void;
}) {
  const [recipeId, setRecipeId] = useState(recipes[0]?.id ?? "");
  const [quantity, setQuantity] = useState(10);
  const [priority, setPriority] = useState(false);
  const [busy, setBusy] = useState(false);
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (
      !recipeId ||
      !Number.isSafeInteger(quantity) ||
      quantity < 1 ||
      quantity > 10000
    )
      return;
    setBusy(true);
    try {
      if (
        await perform(
          "/orders",
          { recipe_id: recipeId, quantity, priority: priority ? 1 : 0 },
          "Production order created.",
        )
      )
        close();
    } finally {
      setBusy(false);
    }
  };
  return (
    <form className="dialog-form" onSubmit={submit}>
      <label className="field">
        <span>Recipe</span>
        <select
          required
          value={recipeId}
          onChange={(event) => setRecipeId(event.target.value)}
        >
          {recipes.map((recipe) => (
            <option key={recipe.id} value={recipe.id}>
              {recipe.name} · v{recipe.version}
            </option>
          ))}
        </select>
      </label>
      <NumberField
        label="Number of batches"
        value={quantity}
        min={1}
        max={10000}
        step="1"
        onChange={setQuantity}
        suffix="× 50 hL"
      />
      <div className="order-total">
        <span>Total requested volume</span>
        <strong>
          {Number.isFinite(quantity) ? (quantity * 50).toLocaleString() : "0"}{" "}
          hL
        </strong>
      </div>
      <label className="checkbox-field">
        <input
          type="checkbox"
          checked={priority}
          onChange={(event) => setPriority(event.target.checked)}
        />
        <span>
          Prioritize this order
          <small>Admit before normal-priority queued demand.</small>
        </span>
      </label>
      <div className="dialog-actions">
        <button type="button" className="button secondary" onClick={close}>
          Cancel
        </button>
        <button
          className="button primary"
          disabled={!canAct || busy || !recipes.length}
        >
          {busy ? "Creating…" : "Create order"}
        </button>
      </div>
    </form>
  );
}

/** Edit a detached recipe copy and publish a new immutable version without changing active batches. */
export function RecipeForm({
  recipe,
  perform,
  canAct,
  close,
}: {
  recipe?: Recipe;
  perform: Perform;
  canAct: boolean;
  close: () => void;
}) {
  const [draft, setDraft] = useState<Recipe>(() =>
    structuredClone(recipe ?? newRecipe()),
  );
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  // Replace only the edited phase in this draft, never mutate a polled recipe.
  const updatePhase = (
    stageIndex: number,
    phaseIndex: number,
    key: "duration_h" | "target_temp_c",
    value: number,
  ) =>
    setDraft((previous) => ({
      ...previous,
      stages: previous.stages.map((stage, i) =>
        i !== stageIndex
          ? stage
          : {
              ...stage,
              phases: stage.phases.map((phase, j) =>
                j !== phaseIndex ? phase : { ...phase, [key]: value },
              ),
            },
      ),
    }));
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    const validation = validateRecipe(draft);
    if (validation) {
      setError(validation);
      return;
    }
    setError("");
    setBusy(true);
    const { recipe_version_id: _versionId, ...body } = draft;
    try {
      if (
        await perform(
          recipe
            ? `/recipes/${encodeURIComponent(recipe.id)}/versions`
            : "/recipes",
          body,
          recipe ? "New recipe version published." : "Recipe created.",
        )
      )
        close();
    } finally {
      setBusy(false);
    }
  };
  return (
    <form className="dialog-form" onSubmit={submit}>
      <div className="form-grid">
        <label className="field">
          <span>Recipe name</span>
          <input
            required
            maxLength={120}
            value={draft.name}
            onChange={(event) =>
              setDraft((previous) => ({
                ...previous,
                name: event.target.value,
              }))
            }
          />
        </label>
        <label className="field">
          <span>Recipe identifier</span>
          <input
            required
            pattern="[A-Za-z0-9_\-]{1,80}"
            maxLength={80}
            readOnly={!!recipe}
            value={draft.id}
            onChange={(event) =>
              setDraft((previous) => ({ ...previous, id: event.target.value }))
            }
          />
        </label>
      </div>
      <div className="form-section-title">
        <h3>Ingredients per 50 hL batch</h3>
        <span className="quiet-tag">Fixed batch volume</span>
      </div>
      <div className="form-grid four">
        {INGREDIENTS.map((key) => (
          <NumberField
            key={key}
            label={humanize(key.split("_")[0])}
            value={draft.ingredients[key]}
            suffix={key === "water_l" ? "L" : "kg"}
            onChange={(value) =>
              setDraft((previous) => ({
                ...previous,
                ingredients: { ...previous.ingredients, [key]: value },
              }))
            }
          />
        ))}
      </div>
      <div className="form-section-title">
        <h3>Process stages</h3>
        <span className="dim">All durations in simulated hours</span>
      </div>
      <div className="recipe-stage-form">
        {draft.stages.map((stage, stageIndex) => (
          <section key={stage.type}>
            <h4>
              <span>{String(stageIndex + 1).padStart(2, "0")}</span>
              {STAGE_LABEL[stage.type]}
            </h4>
            {stage.phases.map((phase, phaseIndex) => (
              <div className="phase-form" key={phase.name}>
                <span>{humanize(phase.name)}</span>
                <NumberField
                  label="Duration"
                  min={0.001}
                  value={phase.duration_h}
                  suffix="h"
                  onChange={(value) =>
                    updatePhase(stageIndex, phaseIndex, "duration_h", value)
                  }
                />
                <NumberField
                  label="Target temperature"
                  min={-10}
                  value={phase.target_temp_c}
                  suffix="°C"
                  onChange={(value) =>
                    updatePhase(stageIndex, phaseIndex, "target_temp_c", value)
                  }
                />
              </div>
            ))}
            <div className="form-grid holding-fields">
              {(["hold_h", "max_pause_h"] as const)
                .filter((key) => key !== "hold_h" || stage.type !== "filter")
                .map((key) => (
                  <NumberField
                    key={key}
                    label={
                      key === "hold_h"
                        ? "Maximum hold after completion"
                        : "Maximum interruption"
                    }
                    value={stage[key] ?? 0}
                    min={0.001}
                    suffix="h"
                    onChange={(value) =>
                      setDraft((previous) => ({
                        ...previous,
                        stages: previous.stages.map((item, i) =>
                          i !== stageIndex ? item : { ...item, [key]: value },
                        ),
                      }))
                    }
                  />
                ))}
            </div>
          </section>
        ))}
      </div>
      <div className="form-grid">
        <NumberField
          label="Transfer duration"
          value={draft.transfer_h}
          min={0.001}
          suffix="h"
          onChange={(value) =>
            setDraft((previous) => ({ ...previous, transfer_h: value }))
          }
        />
        <NumberField
          label="Flush duration"
          value={draft.flush_h}
          min={0.001}
          suffix="h"
          onChange={(value) =>
            setDraft((previous) => ({ ...previous, flush_h: value }))
          }
        />
      </div>
      {error && (
        <p className="inline-error" role="alert">
          {error}
        </p>
      )}
      <p className="form-note">
        Publishing creates an immutable recipe version. Already admitted batches
        keep the recipe they started with.
      </p>
      <div className="dialog-actions">
        <button type="button" className="button secondary" onClick={close}>
          Cancel
        </button>
        <button className="button primary" disabled={!canAct || busy}>
          {busy
            ? "Publishing…"
            : recipe
              ? "Publish new version"
              : "Create recipe"}
        </button>
      </div>
    </form>
  );
}
