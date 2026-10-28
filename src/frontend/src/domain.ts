import { INGREDIENTS, STAGES } from "./types.ts";
import type { Machine, Recipe, Telemetry } from "./types.ts";

/** Choose visual severity: stale/offline status and faults outrank retained process progress. */
export function machineTone(machine: Machine): string {
  if (!machine.online) return "offline";
  if (
    ["FAULT", "RECOVERING", "FLUSH_PENDING", "FLUSHING"].includes(machine.state)
  )
    return "danger";
  if (machine.stopped || machine.local_stop || machine.global_stop)
    return "stopped";
  if (machine.state.startsWith("TRANSFERRING")) return "transfer";
  if (["READY", "READY_TO_TRANSFER"].includes(machine.state)) return "ready";
  if (machine.state === "PROCESSING")
    return machine.phase === "maturation" ? "maturation" : "active";
  return "idle";
}

/** Describe inhibition independently from the machine operation that remains paused underneath it. */
export function machineLabel(machine: Machine): string {
  if (!machine.online) return "Offline";
  if (machine.local_stop) return "Local STOP";
  if (machine.global_stop) return "Global STOP";
  if (machine.stopped) return "Inhibited";
  return humanize(machine.state);
}

/** Translate protocol enum spelling into a readable label without changing the domain value. */
export function humanize(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "Unknown";
  return String(value)
    .toLowerCase()
    .replaceAll("_", " ")
    .replace(/^./, (c) => c.toUpperCase());
}

/** Clamp phase progress for rendering; empty or incomplete snapshots have no measurable progress. */
export function progress(machine: Machine): number {
  const duration = Number(machine.phase_duration_h);
  const elapsed = Number(machine.phase_elapsed_h);
  if (!Number.isFinite(duration) || duration <= 0 || !Number.isFinite(elapsed))
    return 0;
  return Math.max(0, Math.min(100, (elapsed / duration) * 100));
}

/** Format simulated hours, including deadlines that have already expired. */
export function duration(hours: number | null | undefined): string {
  if (hours === null || hours === undefined || !Number.isFinite(hours))
    return "—";
  if (hours < 0) return "Overdue";
  if (hours < 1) return `${Math.round(hours * 60)} min`;
  if (hours < 48) return `${hours.toFixed(hours < 10 ? 1 : 0)} h`;
  return `${(hours / 24).toFixed(1)} d`;
}

/** Display the simulation clock as day and time; this is not the wall clock. */
export function simClock(hours: number | undefined): string {
  if (hours === undefined || !Number.isFinite(hours)) return "Awaiting clock";
  const day = Math.floor(hours / 24) + 1;
  const hour = Math.floor(hours % 24)
    .toString()
    .padStart(2, "0");
  const minute = Math.floor((hours % 1) * 60)
    .toString()
    .padStart(2, "0");
  return `Day ${day} · ${hour}:${minute}`;
}

/** Check editor invariants before submission; the backend remains the authoritative validator. */
export function validateRecipe(recipe: unknown): string | null {
  if (!recipe || typeof recipe !== "object" || Array.isArray(recipe))
    return "Recipe must be a JSON object.";
  const r = recipe as Recipe;
  if (typeof r.id !== "string" || !/^[a-zA-Z0-9_-]{1,80}$/.test(r.id))
    return "Use up to 80 letters, digits, hyphens or underscores for the recipe ID.";
  if (typeof r.name !== "string" || !r.name.trim())
    return "Recipe name is required.";
  if (r.volume_hl !== 50) return "Every batch must have a volume of 50 hL.";
  if (
    !r.ingredients ||
    INGREDIENTS.some((key) => !finiteNonnegative(r.ingredients[key]))
  )
    return "All four ingredient quantities must be finite nonnegative numbers.";
  if (
    Object.keys(r.ingredients).some(
      (key) => !(INGREDIENTS as readonly string[]).includes(key),
    )
  )
    return "Unknown ingredient field.";
  if (
    !Array.isArray(r.stages) ||
    r.stages.length !== STAGES.length ||
    r.stages.some((stage, i) => stage.type !== STAGES[i])
  )
    return "Stages must be mash, lauter, kettle, whirlpool, unitank and filter, in that order.";
  for (const stage of r.stages) {
    if (!Array.isArray(stage.phases) || stage.phases.length === 0)
      return `Add at least one phase to ${stage.type}.`;
    if (
      stage.phases.some(
        (phase) =>
          !phase.name ||
          !Number.isFinite(phase.duration_h) ||
          phase.duration_h <= 0 ||
          !Number.isFinite(phase.target_temp_c),
      )
    )
      return `Invalid duration or temperature in ${stage.type}.`;
    if (
      !(stage.type === "filter" && stage.hold_h === null) &&
      (!finiteNonnegative(stage.hold_h) || stage.hold_h <= 0)
    )
      return `Invalid holding limit in ${stage.type}.`;
    if (!finiteNonnegative(stage.max_pause_h) || stage.max_pause_h <= 0)
      return `Invalid holding or interruption limit in ${stage.type}.`;
    if (
      stage.type === "unitank" &&
      (stage.phases.length !== 2 ||
        stage.phases[0].name !== "fermentation" ||
        stage.phases[1].name !== "maturation")
    )
      return "The uni-tank must contain fermentation followed by maturation.";
  }
  if (
    !Number.isFinite(r.transfer_h) ||
    r.transfer_h <= 0 ||
    !Number.isFinite(r.flush_h) ||
    r.flush_h <= 0
  )
    return "Transfer and flush duration must be positive.";
  return null;
}

/** Accept numeric quantities only; strings, booleans and nonfinite values are not ingredient amounts. */
function finiteNonnegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

/** Deduplicate replayed/live samples, select one sensor and return chronological chart input. */
export function orderedSamples(
  samples: Telemetry[],
  machineId: string,
  sensor: string,
): Telemetry[] {
  const unique = new Map<string, Telemetry>();
  for (const sample of samples) {
    if (
      sample.machine_id !== machineId ||
      sample.sensor !== sensor ||
      !Number.isFinite(sample.value)
    )
      continue;
    const key =
      sample.event_id ||
      `${sample.machine_id}:${sample.sensor}:${sample.ts_utc}`;
    unique.set(key, sample);
  }
  return [...unique.values()].sort(
    (a, b) => Date.parse(a.ts_utc) - Date.parse(b.ts_utc),
  );
}

/** Build an editable six-stage definition; fermentation and maturation share one unitank stage. */
export function newRecipe(): Recipe {
  return {
    id: "new-ale",
    name: "New Ale",
    version: 1,
    volume_hl: 50,
    ingredients: { malt_kg: 900, hops_kg: 20, yeast_kg: 10, water_l: 6000 },
    transfer_h: 0.1,
    flush_h: 0.1,
    stages: STAGES.map((type) => ({
      type,
      hold_h: type === "unitank" ? 168 : type === "filter" ? null : 1,
      max_pause_h: type === "unitank" ? 168 : 1,
      phases:
        type === "unitank"
          ? [
              { name: "fermentation", duration_h: 168, target_temp_c: 20 },
              { name: "maturation", duration_h: 168, target_temp_c: 3 },
            ]
          : [
              {
                name: type,
                duration_h: ["mash", "lauter", "filter"].includes(type) ? 2 : 1,
                target_temp_c:
                  type === "kettle"
                    ? 100
                    : type === "mash"
                      ? 65
                      : type === "lauter"
                        ? 76
                        : 20,
              },
            ],
    })),
  };
}

/** Accept numeric backend stage indices as well as named stage values when rendering a batch trace. */
export function stageLabel(stage: string | number): string {
  const key = typeof stage === "number" ? STAGES[stage] : stage;
  return key ? humanize(key) : "Unknown";
}
