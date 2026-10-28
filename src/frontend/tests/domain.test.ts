import test from "node:test";
import assert from "node:assert/strict";
import {
  machineLabel,
  machineTone,
  progress,
  validateRecipe,
  newRecipe,
  orderedSamples,
  duration,
  simClock,
} from "../src/domain.ts";
import type { Machine, Telemetry } from "../src/types.ts";

const machine: Machine = {
  machine_id: "unitank-1",
  type: "unitank",
  state: "PROCESSING",
  online: true,
  batch_id: "b1",
  phase: "fermentation",
  phase_elapsed_h: 50,
  phase_duration_h: 100,
  local_stop: false,
  global_stop: false,
  stopped: false,
  sim_time_h: 50,
};

test("offline and stop indicators take precedence over a retained process phase", () => {
  assert.equal(machineTone({ ...machine, online: false }), "offline");
  assert.equal(
    machineLabel({ ...machine, online: false, local_stop: true }),
    "Offline",
  );
  assert.equal(machineTone({ ...machine, global_stop: true }), "stopped");
  assert.equal(machineLabel({ ...machine, local_stop: true }), "Local STOP");
  assert.equal(
    machineTone({ ...machine, state: "FLUSH_PENDING", stopped: true }),
    "danger",
  );
  assert.equal(machineTone({ ...machine, phase: "maturation" }), "maturation");
  assert.equal(
    machineTone({ ...machine, state: "TRANSFERRING_IN" }),
    "transfer",
  );
});

test("progress is bounded and does not divide by zero while idle", () => {
  assert.equal(progress(machine), 50);
  assert.equal(progress({ ...machine, phase_duration_h: 0 }), 0);
  assert.equal(progress({ ...machine, phase_elapsed_h: 110 }), 100);
  assert.equal(progress({ ...machine, phase_elapsed_h: -5 }), 0);
  assert.equal(progress({ ...machine, phase_elapsed_h: NaN }), 0);
});

test("equipment READY is available for scheduler decisions, not an idle empty vessel", () => {
  const ready = { ...machine, state: "READY" };
  assert.equal(machineTone(ready), "ready");
  assert.equal(machineLabel(ready), "Ready");
  assert.equal(machineTone({ ...ready, stopped: true }), "stopped");
  assert.equal(machineTone({ ...ready, online: false }), "offline");
  assert.equal(machineTone({ ...ready, state: "READY_TO_TRANSFER" }), "ready");
});

test("recipe editor preserves six stages and a shared two-phase unitank", () => {
  const recipe = newRecipe();
  assert.equal(validateRecipe(recipe), null);
  recipe.stages[4].phases.reverse();
  assert.match(validateRecipe(recipe)!, /fermentation followed by maturation/);
  recipe.stages.pop();
  assert.match(validateRecipe(recipe)!, /Stages must be/);
});

test("recipe validation rejects invalid identifiers, ingredient types, duration and volume", () => {
  for (const patch of [
    { id: "../invalid" },
    { volume_hl: 100 },
    { transfer_h: 0 },
    { flush_h: Infinity },
  ]) {
    assert.notEqual(validateRecipe({ ...newRecipe(), ...patch }), null);
  }
  const recipe = newRecipe();
  assert.notEqual(
    validateRecipe({
      ...recipe,
      ingredients: { ...recipe.ingredients, hops_kg: true },
    }),
    null,
  );
  assert.notEqual(
    validateRecipe({
      ...recipe,
      ingredients: { ...recipe.ingredients, sugar_kg: 0 },
    }),
    null,
  );
  recipe.stages[0].phases[0].duration_h = -1;
  assert.notEqual(validateRecipe(recipe), null);
});

test("merged live and persisted telemetry deduplicates by event ID and sorts chronologically", () => {
  const sample: Telemetry = {
    event_id: "e1",
    machine_id: "unitank-1",
    sensor: "temp_c",
    value: 20,
    unit: "C",
    ts_utc: "2026-01-01T10:00:00Z",
  };
  const latest = {
    ...sample,
    event_id: "e2",
    value: 21,
    ts_utc: "2026-01-01T11:00:00Z",
  };
  assert.deepEqual(
    orderedSamples(
      [
        latest,
        sample,
        sample,
        { ...sample, sensor: "ph" },
        { ...sample, machine_id: "kettle-1" },
        { ...sample, event_id: "bad", value: NaN },
      ],
      "unitank-1",
      "temp_c",
    ),
    [sample, latest],
  );
});

test("simulation time and deadline descriptions handle day rollover and overdue holds", () => {
  assert.equal(simClock(25.5), "Day 2 · 01:30");
  assert.equal(duration(-0.1), "Overdue");
  assert.equal(duration(0.5), "30 min");
});
