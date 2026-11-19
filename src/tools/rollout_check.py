#!/usr/bin/env python3
"""Observe a backend rollout while an isolated factory has a batch in progress."""

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from http_client import Api


def main():
    """Restart backend replicas and record availability and production interruptions."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--namespace", default="beerfactory")
    parser.add_argument("--output", type=Path, default=Path("rollout-result.json"))
    args = parser.parse_args()
    api = Api(args.base_url)
    baseline = api.call("/machines")
    if not any(m.get("batch_id") and not m.get("stopped") for m in baseline):
        raise SystemExit("Start an active batch before testing continuous deployment")
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "before": baseline,
        "samples": [],
        "errors": [],
    }
    subprocess.run(
        ["kubectl", "-n", args.namespace, "rollout", "restart", "deployment/backend"],
        check=True,
    )
    process = subprocess.Popen(
        [
            "kubectl",
            "-n",
            args.namespace,
            "rollout",
            "status",
            "deployment/backend",
            "--timeout=120s",
        ]
    )
    started = time.monotonic()
    while process.poll() is None and time.monotonic() - started < 125:
        try:
            report["samples"].append(
                {
                    "elapsed_s": time.monotonic() - started,
                    "machines": api.call("/machines"),
                }
            )
        except Exception as error:
            report["errors"].append(
                {"elapsed_s": time.monotonic() - started, "error": str(error)}
            )
        time.sleep(0.5)
    if process.poll() is None:
        process.terminate()
    report["rollout_exit_code"] = process.wait(timeout=5)
    baseline_stops = {m["machine_id"]: bool(m.get("stopped")) for m in baseline}
    interruptions = [
        m["machine_id"]
        for sample in report["samples"]
        for m in sample["machines"]
        if m.get("batch_id")
        and m.get("stopped")
        and not baseline_stops.get(m["machine_id"], False)
    ]
    report["production_interruptions"] = sorted(set(interruptions))
    expected = {m["machine_id"] for m in baseline if m.get("online")}
    missing = [
        sorted(
            expected - {m["machine_id"] for m in sample["machines"] if m.get("online")}
        )
        for sample in report["samples"]
    ]
    report["missing_or_offline"] = [group for group in missing if group]
    report["passed"] = (
        report["rollout_exit_code"] == 0
        and not interruptions
        and bool(report["samples"])
        and not report["errors"]
        and not report["missing_or_offline"]
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
