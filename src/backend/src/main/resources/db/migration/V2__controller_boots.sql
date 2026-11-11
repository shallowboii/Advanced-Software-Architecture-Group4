-- Controller execution is deliberately volatile. PostgreSQL records discontinuities,
-- but does not reconstruct the physical contents of a restarted controller.
ALTER TABLE factory_state ADD COLUMN restart_blocked boolean NOT NULL DEFAULT false;
ALTER TABLE factory_state ADD COLUMN failure_reason text;
CREATE TABLE controller_boot (
    machine_id text NOT NULL REFERENCES machine(id),
    boot_id text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    retired_at timestamptz,
    PRIMARY KEY (machine_id, boot_id)
);

-- Recipe sequencing and all quality deadlines belong to the central scheduler.
ALTER TABLE batch ADD COLUMN phase_index integer NOT NULL DEFAULT 0;
ALTER TABLE batch ADD COLUMN hold_deadline_h double precision;
ALTER TABLE batch ADD COLUMN pause_started_h double precision;
ALTER TABLE transfer ADD COLUMN source_hold_deadline_h double precision;
ALTER TABLE batch ADD COLUMN pause_accumulated_h double precision NOT NULL DEFAULT 0;
ALTER TABLE command ADD COLUMN rejection_reason text;
