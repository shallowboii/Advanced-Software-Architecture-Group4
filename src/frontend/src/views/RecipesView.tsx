import { duration, humanize } from "../domain";
import { Empty, Icon } from "../ui";
import type { Recipe } from "../types";
import { PageHeading } from "./PageHeading";

/** Show current recipe definitions and open creation of an immutable next version. */
export function RecipesView({
  recipes,
  edit,
  canAct,
}: {
  recipes: Recipe[];
  edit: (recipe?: Recipe) => void;
  canAct: boolean;
}) {
  return (
    <>
      <PageHeading
        eyebrow="BREWING DEFINITIONS"
        title="Recipe library"
        description="New versions apply to future admissions. Active batches keep their original recipe."
        action={
          <button
            className="button primary"
            disabled={!canAct}
            onClick={() => edit()}
          >
            <Icon name="plus" />
            Create recipe
          </button>
        }
      />
      {recipes.length ? (
        <div className="recipe-grid">
          {recipes.map((recipe) => (
            <section className="panel recipe-card" key={recipe.id}>
              <div className="recipe-card-top">
                <span className="recipe-icon">
                  <Icon name="recipe" size={27} />
                </span>
                <span className="quiet-tag">Version {recipe.version}</span>
              </div>
              <h2>{recipe.name}</h2>
              <p className="mono dim">{recipe.id}</p>
              <div className="recipe-facts">
                <div>
                  <small>Batch volume</small>
                  <strong>{recipe.volume_hl} hL</strong>
                </div>
                <div>
                  <small>Process duration</small>
                  <strong>
                    {duration(
                      recipe.stages?.reduce(
                        (sum, stage) =>
                          sum +
                          stage.phases.reduce(
                            (total, phase) => total + phase.duration_h,
                            0,
                          ),
                        0,
                      ),
                    )}
                  </strong>
                </div>
              </div>
              <div className="recipe-timeline">
                {recipe.stages?.map((stage) => (
                  <div key={stage.type}>
                    <span>{humanize(stage.type)}</span>
                    <span>
                      {duration(
                        stage.phases.reduce(
                          (sum, phase) => sum + phase.duration_h,
                          0,
                        ),
                      )}
                    </span>
                  </div>
                ))}
              </div>
              <button
                className="button secondary full"
                disabled={!canAct}
                onClick={() => edit(recipe)}
              >
                Publish a new version <Icon name="arrow" size={16} />
              </button>
            </section>
          ))}
        </div>
      ) : (
        <Empty title="No recipes loaded">
          Create a recipe or check the backend recipe configuration.
        </Empty>
      )}
    </>
  );
}
