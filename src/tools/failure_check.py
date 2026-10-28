#!/usr/bin/env python3
"""Check outages, then deliberately leave a restarted active machine unresolved.

Run only against an isolated smoke factory. The final scenario restarts an active
controller, whose RAM state is lost. A passing run leaves the line stopped and
backend evidence intact; it does not attempt to recover or discard that batch.
"""

import argparse
import copy
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode

from http_client import Api
from smoke import until

ROOT = Path(__file__).resolve().parents[2]
MACHINE_ID = "mash-0"
BUSINESS_FIELDS = {
    "batch_id",
    "recipe_id",
    "recipe_version_id",
    "order_id",
    "execution_id",
    "transfer_id",
    "run_id",
    "phase",
    "phases",
}


def require(condition, message):
    """Fail a scenario with an explanation retained in the evidence report."""
    if not condition:
        raise AssertionError(message)


def utc_now():
    """Use wall-clock timestamps for API history filters and report entries."""
    return datetime.now(timezone.utc).isoformat()


class FailureChecks:
    """Keep physical observations separate from backend-owned business mappings."""

    def __init__(self, base_url, output):
        self.api = Api(base_url)
        self.output = output
        self.key = "failure-" + str(uuid.uuid4())
        self.report = {"passed": False, "run_key": self.key, "steps": []}
        self.stopped_services = set()
        self.started = False
        self.compose = [
            "docker",
            "compose",
            "--project-directory",
            str(ROOT),
            "-f",
            str(ROOT / "src/deploy/compose/smoke.json"),
        ]

    def docker(self, *command):
        """Execute a bounded command against this repository's smoke deployment."""
        return subprocess.run(
            self.compose + list(command),
            check=True,
            capture_output=True,
            text=True,
            timeout=90,
        ).stdout

    def physical(self):
        """Read equipment state directly; business IDs must never appear here."""
        snapshot = json.loads(
            self.docker("exec", "-T", MACHINE_ID, "machinectl", "snapshot")
        )
        unexpected = BUSINESS_FIELDS.intersection(snapshot)
        require(
            not unexpected, f"Controller exposes business fields: {sorted(unexpected)}"
        )
        require(snapshot["machine_id"] == MACHINE_ID, "Unexpected controller identity")
        return snapshot

    def local(self, command):
        """Simulate a physical operator action on the current controller process."""
        self.docker("exec", "-T", MACHINE_ID, "machinectl", command)

    def wait_physical(self, predicate, timeout, label):
        """Return the actual physical snapshot that satisfied a scenario condition."""

        def observe():
            snapshot = self.physical()
            return snapshot if predicate(snapshot) else None

        return until(observe, timeout, label)

    def write_report(self):
        """Save partial evidence after each scenario and again during cleanup."""
        self.output.parent.mkdir(parents=True, exist_ok=True)
        self.output.write_text(json.dumps(self.report, indent=2) + "\n")

    def record(self, name, details):
        """Record observations, rather than only reporting a Boolean result."""
        self.report["steps"].append(
            {
                "name": name,
                "ts_utc": utc_now(),
                "details": details,
            }
        )
        self.write_report()

    def stop_service(self, service):
        """Track stopped dependencies so cleanup can restore only this test's outages."""
        self.stopped_services.add(service)
        self.docker("stop", service)

    def start_service(self, service):
        """Restart a dependency without restarting the physical controller."""
        self.docker("start", service)
        self.stopped_services.discard(service)

    def batch(self, batch_id):
        """Resolve batch identity from the backend, never from the controller."""
        return next(
            (batch for batch in self.api.call("/batches") if batch["id"] == batch_id),
            None,
        )

    def events(self, batch_id):
        """Fetch recent canonical backend evidence for one centrally tracked batch."""
        return self.api.call(
            "/events?"
            + urlencode(
                {
                    "batch_id": batch_id,
                    "limit": 1000,
                    "order": "desc",
                }
            )
        )

    def same_operation(self, snapshot, original):
        """Assert continuity using technical boot/command correlation, not batch IDs."""
        require(
            snapshot["controller_boot_id"] == original["controller_boot_id"],
            "Controller restarted during an outage-continuity scenario",
        )
        require(snapshot["occupied"], "Active controller unexpectedly became empty")
        require(snapshot["state"] == "PROCESSING", "Long operation unexpectedly ended")
        require(
            snapshot["current_command_id"] == original["current_command_id"],
            "Initiating operation command changed during an outage",
        )

    def verify_pause(self, paused):
        """A stopped actuator retains progress while simulated time continues."""
        time.sleep(1)
        later = self.physical()
        self.same_operation(later, paused)
        require(later["stopped"], "Actuator resumed while it should be inhibited")
        require(
            abs(later["elapsed_h"] - paused["elapsed_h"]) < 1e-6,
            "Actuator progress advanced while stopped",
        )
        require(
            later["sim_time_h"] > paused["sim_time_h"],
            "Simulation clock stopped ageing",
        )
        return later

    def prepare(self):
        """Require an empty running factory and define a deliberately long operation."""
        line = self.api.call("/line")
        require(
            not line.get("active_wip")
            and not line.get("stopped")
            and not line.get("recovery_required"),
            "Failure checks require an empty, resumed, isolated smoke factory",
        )
        require(
            all(
                order["status"] in ("FINISHED", "CANCELLED")
                for order in self.api.call("/orders")
            ),
            "Cancel pending orders before running failure checks",
        )
        initial = self.physical()
        require(
            initial["state"] == "EMPTY", "The smoke mash controller must start empty"
        )
        self.started = True
        recipe = copy.deepcopy(
            next(item for item in self.api.call("/recipes") if item["id"] == "ale")
        )
        recipe.update(id=self.key, name="Equipment outage check", version=1)
        recipe.pop("recipe_version_id", None)
        # The scheduler translates this recipe into physical operations. Long
        # durations keep each tested operation active throughout real-time outages.
        for stage in recipe["stages"]:
            stage.update(
                max_pause_h=1000,
                hold_h=None if stage["type"] == "filter" else 1000,
            )
            for phase in stage["phases"]:
                phase["duration_h"] = 1000
        self.api.call("/recipes", recipe, self.key + "-recipe")
        self.record("isolated equipment-only factory ready", initial)

    def admit_batch(self, suffix):
        """Admit one batch centrally, then observe its physical START independently."""
        order = self.api.call(
            "/orders",
            {"recipe_id": self.key, "quantity": 1},
            self.key + "-order-" + suffix,
        )
        batch = until(
            lambda: next(
                (
                    item
                    for item in self.api.call("/batches")
                    if item["order_id"] == order["id"]
                    and item["status"] == "PROCESSING"
                ),
                None,
            ),
            90,
            "backend batch processing",
        )
        require(
            batch["machine_id"] == MACHINE_ID, "Batch was not admitted to smoke mash"
        )
        physical = self.wait_physical(
            lambda item: item["state"] == "PROCESSING"
            and item["occupied"]
            and not item["stopped"]
            and bool(item["current_command_id"]),
            30,
            "physical operation starts",
        )
        return order, batch, physical

    def check_backend_outage(self, original):
        """Coordinator loss must expire the permit without losing physical progress."""
        self.stop_service("backend")
        expired = self.wait_physical(
            lambda item: not item["permit_valid"] and item["stopped"],
            10,
            "permit expires without backend",
        )
        self.same_operation(expired, original)
        paused = self.verify_pause(expired)
        self.start_service("backend")
        until(
            lambda: self.api.call("/health").get("status") == "ok",
            90,
            "backend recovery",
        )
        recovered = self.wait_physical(
            lambda item: not item["stopped"],
            30,
            "permit renewal after backend restart",
        )
        self.same_operation(recovered, original)
        self.record(
            "backend outage pauses actuator while clock ages",
            {
                "paused": paused,
                "recovered": recovered,
            },
        )

    def check_mqtt_outage(self, original):
        """Control-broker loss inhibits motion; reconnect keeps the same operation."""
        self.stop_service("rabbitmq")
        expired = self.wait_physical(
            lambda item: not item["permit_valid"] and item["stopped"],
            10,
            "permit expires without MQTT",
        )
        self.same_operation(expired, original)
        paused = self.verify_pause(expired)
        self.start_service("rabbitmq")
        recovered = self.wait_physical(
            lambda item: not item["stopped"],
            90,
            "MQTT reconnect and permit renewal",
        )
        self.same_operation(recovered, original)
        self.record(
            "MQTT outage retains the same in-process operation",
            {
                "paused": paused,
                "recovered": recovered,
            },
        )

    def check_kafka_outage(self, batch_id, original):
        """Telemetry-broker availability must not control the physical actuator."""
        self.stop_service("kafka")
        time.sleep(2)
        during = self.physical()
        self.same_operation(during, original)
        require(
            not during["stopped"], "Kafka outage incorrectly inhibited the actuator"
        )
        self.start_service("kafka")
        after = utc_now()
        samples = until(
            lambda: self.api.call(
                "/telemetry?"
                + urlencode(
                    {
                        "batch_id": batch_id,
                        "from": after,
                        "limit": 20,
                        "order": "desc",
                    }
                )
            ),
            90,
            "new backend-enriched Kafka telemetry after recovery",
        )
        self.record(
            "Kafka outage leaves control running; telemetry recovers",
            {
                "physical_during_outage": during,
                "recovered_samples": len(samples),
            },
        )

    def check_local_stop(self, original):
        """Local stop/resume affect motion during this boot, without erasing contents."""
        self.local("stop")
        stopped = self.wait_physical(
            lambda item: item["local_stop"] and item["stopped"],
            10,
            "physical local STOP",
        )
        self.same_operation(stopped, original)
        paused = self.verify_pause(stopped)
        self.local("resume")
        resumed = self.wait_physical(
            lambda item: not item["local_stop"]
            and not item["stopped"]
            and item["elapsed_h"] > paused["elapsed_h"],
            30,
            "physical local resume advances progress",
        )
        self.same_operation(resumed, original)
        self.record(
            "local STOP and resume retain contents in the same boot",
            {
                "paused": paused,
                "resumed": resumed,
            },
        )

    def check_operator_flush(self, order, batch, original):
        """Press physical flush; the backend must infer the batch from command history."""
        related = [
            item
            for item in self.api.call("/batches")
            if item["order_id"] == order["id"]
        ]
        require(len(related) == 1, "Outages caused duplicate batch admission")
        self.same_operation(self.physical(), original)
        self.local("flush")
        removed = until(
            lambda: (
                item
                if (item := self.batch(batch["id"])) and item["status"] == "DISCARDED"
                else None
            ),
            30,
            "first batch operator discard",
        )
        empty = self.wait_physical(
            lambda item: item["state"] == "EMPTY" and not item["occupied"],
            30,
            "physical flush completes",
        )
        require(
            empty["controller_boot_id"] == original["controller_boot_id"],
            "Controller restarted before the operator flush completed",
        )
        losses = [
            event
            for event in self.events(batch["id"])
            if event["event"] == "LOSS_RECORDED"
        ]
        require(
            len(losses) == 1 and losses[0]["reason"] == "operator",
            "Expected exactly one operator loss for the first batch",
        )
        self.record(
            "one admission and one operator loss within one controller boot",
            {
                "batch": removed,
                "physical": empty,
                "loss": losses[0],
            },
        )

    def check_active_controller_restart(self):
        """Final destructive scenario: retain unresolved backend evidence and STOP."""
        _, batch, before = self.admit_batch("restart")
        counters_before = self.api.call("/experiment/status")
        self.record(
            "active operation before final controller restart",
            {
                "batch": batch,
                "physical": before,
                "counters": counters_before,
            },
        )
        self.docker("restart", MACHINE_ID)
        restarted = self.wait_physical(
            lambda item: item["controller_boot_id"] != before["controller_boot_id"],
            30,
            "fresh controller boot",
        )
        require(
            restarted["state"] == "EMPTY" and not restarted["occupied"],
            "A restarted RAM-only controller must report empty equipment",
        )
        require(
            not restarted["current_command_id"], "Old actuator command survived restart"
        )
        require(
            not restarted["interrupted_command_id"],
            "Old interrupted command survived restart",
        )
        require(restarted["elapsed_h"] == 0, "Old physical progress survived restart")
        line = until(
            lambda: (
                item
                if (item := self.api.call("/line")).get("stopped")
                and item.get("recovery_required")
                else None
            ),
            30,
            "backend detects lost active equipment state",
        )
        require(
            line.get("restart_blocked"),
            "Controller restart must block automatic resume",
        )
        require(
            line.get("failure_reason"), "Restart STOP must retain an explanatory reason"
        )
        unresolved = until(
            lambda: (
                item
                if (item := self.batch(batch["id"])) and item["status"] == "UNRESOLVED"
                else None
            ),
            30,
            "backend retains unresolved batch",
        )
        for field in (
            "id",
            "order_id",
            "recipe_id",
            "recipe_version_id",
            "machine_id",
            "volume_hl",
        ):
            require(
                unresolved[field] == batch[field],
                f"Restart erased central batch field: {field}",
            )
        try:
            self.api.call("/line/resume", {}, self.key + "-blocked-resume")
        except RuntimeError as error:
            require(
                isinstance(error.__cause__, HTTPError) and error.__cause__.code == 409,
                f"Expected resume conflict 409, received: {error}",
            )
            resume_error = str(error)
        else:
            raise AssertionError(
                "Line resume unexpectedly succeeded with unresolved contents"
            )
        counters_after = self.api.call("/experiment/status")
        require(
            counters_after.get("failed")
            and counters_after.get("done")
            and counters_after.get("requires_clean_factory"),
            "Restart must invalidate the experiment and require a clean factory",
        )
        for field in (
            "completed",
            "spoiled",
            "operator_discarded",
            "hl_out",
            "hl_wasted",
        ):
            require(
                counters_after[field] == counters_before[field],
                f"Restart invented a material outcome: {field}",
            )
        retained = self.batch(batch["id"])
        require(
            retained["status"] == "UNRESOLVED", "Rejected resume changed batch evidence"
        )
        line_after_rejection = self.api.call("/line")
        require(
            line_after_rejection["stopped"]
            and line_after_rejection["recovery_required"],
            "Rejected resume cleared the recovery STOP",
        )
        final_physical = self.physical()
        require(
            final_physical["state"] == "EMPTY" and not final_physical["occupied"],
            "An old operation was replayed onto the fresh controller",
        )

        def state_loss_evidence():
            events = self.events(batch["id"])
            return (
                events
                if any(event["event"] == "CONTROLLER_STATE_LOST" for event in events)
                else None
            )

        evidence = until(
            state_loss_evidence, 10, "controller-state-loss evidence recorded"
        )
        self.record(
            "controller restart loses RAM; backend blocks resume and retains evidence",
            {
                "physical_after_restart": restarted,
                "physical_after_rejection": final_physical,
                "line": line_after_rejection,
                "batch": retained,
                "experiment": counters_after,
                "resume_rejection": resume_error,
                "events": evidence,
            },
        )
        self.report["final_state"] = {
            "line_stopped": True,
            "recovery_required": True,
            "unresolved_batch_id": batch["id"],
            "requires_clean_factory": True,
        }

    def cleanup(self):
        """Restore dependencies only; never clear STOP or discard unresolved evidence."""
        if not self.started:
            return
        for service in sorted(self.stopped_services.copy()):
            self.start_service(service)
        if not self.report["passed"]:
            until(
                lambda: self.api.call("/health").get("status") == "ok",
                60,
                "backend before failure STOP",
            )
            self.api.call("/line/stop", {}, self.key + "-failure-stop")

    def run(self):
        """Run continuity scenarios first; an active-controller restart must be last."""
        try:
            self.prepare()
            order, batch, original = self.admit_batch("outages")
            self.check_backend_outage(original)
            self.check_mqtt_outage(original)
            self.check_kafka_outage(batch["id"], original)
            self.check_local_stop(original)
            self.check_operator_flush(order, batch, original)
            self.check_active_controller_restart()
            self.report["passed"] = True
        except Exception as error:
            self.report["error"] = str(error)
            if isinstance(error, subprocess.CalledProcessError):
                self.report["command_error"] = {
                    "stdout": error.stdout,
                    "stderr": error.stderr,
                }
        finally:
            try:
                self.cleanup()
            except Exception as error:
                self.report["cleanup_error"] = str(error)
                self.report["passed"] = False
            self.write_report()
        return self.report["passed"]


def main():
    """Parse the target/output location and report the deliberately blocked final state."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--output", type=Path, default=Path("failure-result.json"))
    args = parser.parse_args()
    checks = FailureChecks(args.base_url, args.output)
    passed = checks.run()
    print(f'Failure checks: {"PASS" if passed else "FAIL"}; {args.output}')
    if passed:
        print(
            "Expected final state: line STOP, recovery required, unresolved batch retained."
        )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
