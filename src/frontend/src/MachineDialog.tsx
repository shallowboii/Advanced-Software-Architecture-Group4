import {
  duration,
  humanize,
  machineLabel,
  machineTone,
  progress,
} from "./domain";
import { Dialog, Icon, timestamp } from "./ui";
import type { Machine } from "./types";
import type { Perform } from "./actions";

/**
 * Inspect one live machine and request local stop/resume or batch-scoped flush.
 * Business labels come from the backend projection, not the physical controller.
 * The parent captures the selected batch before asking for flush confirmation.
 * Displaying a stopped machine never implies that its material was removed.
 */
export function MachineDialog({
  machine,
  canAct,
  perform,
  onClose,
  onFlush,
}: {
  machine: Machine;
  canAct: boolean;
  perform: Perform;
  onClose: () => void;
  onFlush: () => void;
}) {
  return (
    <Dialog
      title={machine.machine_id}
      description="Machine status and operator controls"
      onClose={onClose}
      sheet
    >
      <div className="machine-detail-header">
        <span className={`quiet-tag ${machineTone(machine)}`}>
          <i className="status-dot" />
          {machineLabel(machine)}
        </span>
        <span>{humanize(machine.type)} · 50 hL</span>
      </div>
      <div className="machine-phase">
        <div>
          <span>Current phase</span>
          <strong>
            {machine.phase ? humanize(machine.phase) : "No active phase"}
          </strong>
        </div>
        <strong>{Math.round(progress(machine))}%</strong>
      </div>
      <div className="progress-track large">
        <span style={{ width: `${progress(machine)}%` }} />
      </div>
      <dl className="detail-list">
        <div>
          <dt>Batch</dt>
          <dd className="mono">{machine.batch_id ?? "Empty"}</dd>
        </div>
        <div>
          <dt>State</dt>
          <dd>{humanize(machine.state)}</dd>
        </div>
        <div>
          <dt>Phase time remaining</dt>
          <dd>
            {duration(
              Math.max(0, machine.phase_duration_h - machine.phase_elapsed_h),
            )}
          </dd>
        </div>
        <div>
          <dt>Holding deadline</dt>
          <dd>
            {machine.hold_deadline_h
              ? duration(machine.hold_deadline_h - machine.sim_time_h)
              : "Not holding"}
          </dd>
        </div>
        <div>
          <dt>Recipe version</dt>
          <dd className="mono">{machine.recipe_version_id ?? "None"}</dd>
        </div>
        <div>
          <dt>Transfer</dt>
          <dd className="mono">{machine.transfer_id ?? "No transfer"}</dd>
        </div>
        <div>
          <dt>Last seen</dt>
          <dd>{timestamp(machine.last_seen)}</dd>
        </div>
      </dl>
      <p className="form-note">
        STOP retains the batch. Resume clears the local STOP; global STOP must
        be released separately.
      </p>
      <div className="machine-actions">
        <button
          className="button danger-outline"
          disabled={!canAct || !machine.online || machine.local_stop}
          onClick={() =>
            void perform(
              `/machines/${encodeURIComponent(machine.machine_id)}/stop`,
              {},
              `${machine.machine_id}: local STOP requested.`,
            )
          }
        >
          <Icon name="stop" />
          Stop machine
        </button>
        <button
          className="button secondary"
          disabled={!canAct || !machine.online || !machine.local_stop}
          onClick={() =>
            void perform(
              `/machines/${encodeURIComponent(machine.machine_id)}/resume`,
              {},
              `${machine.machine_id}: resume requested.`,
            )
          }
        >
          <Icon name="play" />
          Resume machine
        </button>
        <button
          className="button danger-outline full"
          disabled={!canAct || !machine.batch_id || !machine.online}
          onClick={onFlush}
        >
          <Icon name="drop" />
          Flush selected batch
        </button>
      </div>
    </Dialog>
  );
}
