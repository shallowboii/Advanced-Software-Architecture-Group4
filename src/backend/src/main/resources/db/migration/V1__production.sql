CREATE TABLE factory_state (
  id integer PRIMARY KEY CHECK(id=1), epoch bigint NOT NULL DEFAULT 0,
  sim_origin timestamptz NOT NULL DEFAULT now(), time_scale double precision NOT NULL CHECK(time_scale>0),
  stopped boolean NOT NULL DEFAULT false, control_version bigint NOT NULL DEFAULT 0, policy text NOT NULL CHECK(policy IN ('PUSH','CONWIP')),
  wip_limit integer NOT NULL CHECK(wip_limit>0), run_id text NOT NULL,
  horizon_h double precision, experiment_start_h double precision NOT NULL DEFAULT 0,
  experiment_done boolean NOT NULL DEFAULT false
);
CREATE TABLE recipe_version (
  recipe_id text NOT NULL, version integer NOT NULL CHECK(version>0),
  definition jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(recipe_id,version)
);
CREATE TABLE production_order (
  id text PRIMARY KEY, recipe_id text NOT NULL, pinned_version integer,
  quantity integer NOT NULL CHECK(quantity>0), issued integer NOT NULL DEFAULT 0,
  priority integer NOT NULL DEFAULT 0, status text NOT NULL DEFAULT 'QUEUED',
  operator_label text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  CHECK(issued>=0 AND issued<=quantity)
);
CREATE TABLE machine (
  id text PRIMARY KEY, type text NOT NULL, snapshot jsonb NOT NULL,
  state_version bigint NOT NULL DEFAULT 0, last_seen timestamptz NOT NULL DEFAULT now(),
  assigned_batch text, assigned_role text
);
CREATE TABLE batch (
  id text PRIMARY KEY, order_id text NOT NULL REFERENCES production_order(id),
  recipe_id text NOT NULL, recipe_version_id text NOT NULL, recipe jsonb NOT NULL,
  status text NOT NULL, stage integer NOT NULL DEFAULT 0 CHECK(stage BETWEEN 0 AND 5),
  machine_id text NOT NULL REFERENCES machine(id), execution_id text NOT NULL,
  run_id text NOT NULL, volume_hl integer NOT NULL DEFAULT 50 CHECK(volume_hl=50),
  loss_reason text, created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
  admitted_sim_h double precision NOT NULL, completed_sim_h double precision
);
CREATE UNIQUE INDEX one_active_batch_per_owner ON batch(machine_id) WHERE status NOT IN ('COMPLETED','DISCARDED');
CREATE TABLE transfer (
  id text PRIMARY KEY, batch_id text NOT NULL REFERENCES batch(id),
  source_id text NOT NULL REFERENCES machine(id), target_id text NOT NULL REFERENCES machine(id),
  source_execution_id text NOT NULL, next_execution_id text NOT NULL, status text NOT NULL DEFAULT 'PREPARING',
  source_prepared boolean NOT NULL DEFAULT false, target_prepared boolean NOT NULL DEFAULT false,
  source_ready boolean NOT NULL DEFAULT false, target_ready boolean NOT NULL DEFAULT false,
  source_done boolean NOT NULL DEFAULT false, target_done boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(), CHECK(source_id<>target_id)
);
CREATE UNIQUE INDEX one_active_transfer_per_batch ON transfer(batch_id) WHERE status NOT IN ('DONE','ABORTED');
CREATE TABLE event_inbox (event_id text PRIMARY KEY, received_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE command (
  id text PRIMARY KEY, machine_id text NOT NULL, cmd text NOT NULL, payload jsonb NOT NULL,
  acknowledged boolean NOT NULL DEFAULT false, attempts integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(), last_sent timestamptz
);
CREATE TABLE production_event (
  recorded_seq bigserial UNIQUE NOT NULL,
  event_id text PRIMARY KEY, event text NOT NULL, batch_id text, machine_id text, run_id text NOT NULL,
  sim_time_h double precision NOT NULL, ts_utc timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL
);
CREATE INDEX production_event_run ON production_event(run_id,recorded_seq);
CREATE TABLE outbox (
  id bigserial PRIMARY KEY, topic text NOT NULL, message_key text NOT NULL, payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), sent_at timestamptz
);
CREATE TABLE api_request (
  scope text NOT NULL, request_key text NOT NULL, fingerprint text NOT NULL,
  response jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(scope,request_key)
);
CREATE TABLE telemetry (
  event_id text PRIMARY KEY, machine_id text NOT NULL, batch_id text, run_id text,
  recipe_version_id text, phase text, sensor text NOT NULL, value double precision NOT NULL,
  unit text, sim_time_h double precision, ts_utc timestamptz NOT NULL, payload jsonb NOT NULL
);
CREATE INDEX telemetry_machine_time ON telemetry(machine_id,ts_utc DESC);
CREATE INDEX telemetry_run_time ON telemetry(run_id,ts_utc);
CREATE TABLE alarm (
  id bigserial PRIMARY KEY, machine_id text, reason text NOT NULL,
  details jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
