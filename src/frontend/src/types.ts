export const STAGES = [
  "mash",
  "lauter",
  "kettle",
  "whirlpool",
  "unitank",
  "filter",
] as const;
export type MachineType = (typeof STAGES)[number];
export const STAGE_LABEL: Record<MachineType, string> = {
  mash: "Mashing",
  lauter: "Lautering",
  kettle: "Boiling",
  whirlpool: "Whirlpool",
  unitank: "Fermentation & maturation",
  filter: "Filtering & CO₂",
};
export const INGREDIENTS = [
  "malt_kg",
  "hops_kg",
  "yeast_kg",
  "water_l",
] as const;
export type Ingredients = Record<(typeof INGREDIENTS)[number], number>;
export interface Machine {
  machine_id: string;
  type: MachineType;
  state: string;
  online: boolean;
  batch_id: string | null;
  execution_id?: string | null;
  transfer_id?: string | null;
  recipe_version_id?: string | null;
  phase: string | null;
  phase_index?: number;
  phase_elapsed_h: number;
  phase_duration_h: number;
  hold_deadline_h?: number | null;
  local_stop: boolean;
  global_stop: boolean;
  stopped: boolean;
  permit_valid?: boolean;
  sim_time_h: number;
  last_seen?: string;
  epoch?: number;
  state_version?: number;
}
export interface Phase {
  name: string;
  duration_h: number;
  target_temp_c: number;
}
export interface Stage {
  type: MachineType;
  phases: Phase[];
  hold_h: number | null;
  max_pause_h: number;
}
export interface Recipe {
  id: string;
  name: string;
  version: number;
  recipe_version_id?: string;
  volume_hl: number;
  ingredients: Ingredients;
  stages: Stage[];
  transfer_h: number;
  flush_h: number;
}
export interface Order {
  id: string;
  recipe_id: string;
  quantity: number;
  started: number;
  completed: number;
  discarded: number;
  priority: number;
  status: string;
}
export interface Batch {
  id: string;
  order_id: string;
  recipe_id: string;
  recipe_version_id: string;
  status: string;
  machine_id: string | null;
  stage: string | number;
  volume_hl: number;
  created_at: string;
  completed_at?: string | null;
  run_id?: string;
}
export interface ProductionEvent {
  event_id: string;
  event: string;
  batch_id?: string | null;
  machine_id?: string | null;
  run_id?: string;
  sim_time_h?: number;
  ts_utc: string;
  reason?: string;
  snapshot?: Partial<Machine>;
}
export interface Telemetry {
  event_id?: string;
  machine_id: string;
  batch_id?: string;
  run_id?: string;
  sensor: string;
  value: number;
  unit: string;
  ts_utc: string;
  sim_time_h?: number;
}
export interface Line {
  stopped: boolean;
  policy: string;
  wip_limit: number;
  active_wip: number;
  sim_time_h: number;
  machines_pending_stop: string[] | number;
}
export interface Experiment {
  run_id: string | null;
  policy: string;
  started: number;
  completed: number;
  spoiled: number;
  operator_discarded: number;
  hl_out: number;
  hl_wasted: number;
  elapsed_h?: number;
  sim_clock_h: number;
  done: boolean;
}
export interface FactorySnapshot {
  machines: Machine[];
  recipes: Recipe[];
  orders: Order[];
  batches: Batch[];
  inventory: { stock: Ingredients } | null;
  line: Line | null;
  events: ProductionEvent[];
  experiment: Experiment | null;
}
