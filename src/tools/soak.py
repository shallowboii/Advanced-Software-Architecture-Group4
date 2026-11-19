#!/usr/bin/env python3
"""Record real-time service availability without conflating it with simulated days."""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from http_client import Api


def main():
    """Sample a live factory for the requested real duration and save availability evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--interval", type=float, default=5)
    parser.add_argument("--tanks", type=int, choices=[1, 20, 40], default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.hours <= 0 or args.interval <= 0:
        parser.error("Duration and interval must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    api = Api(args.base_url)
    start, samples, failures = time.monotonic(), 0, 0
    try:
        while time.monotonic() - start < args.hours * 3600:
            record = {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "elapsed_s": time.monotonic() - start,
            }
            try:
                record.update(
                    health=api.call("/health"),
                    line=api.call("/line"),
                    machines=api.call("/machines"),
                )
                expected = {
                    f"{kind}-0"
                    for kind in ("mash", "lauter", "kettle", "whirlpool", "filter")
                } | {f"unitank-{i}" for i in range(args.tanks)}
                online = {
                    m["machine_id"] for m in record["machines"] if m.get("online")
                }
                record["healthy"] = (
                    record["health"].get("status") == "ok" and expected <= online
                )
                record["active_wip"] = record["line"].get("active_wip", 0)
                record["unexpected_stop"] = not record["line"].get("stopped") and any(
                    m.get("batch_id") and m.get("stopped") for m in record["machines"]
                )
            except Exception as error:
                record.update(healthy=False, error=str(error))
            samples += 1
            failures += not record["healthy"] or record.get("unexpected_stop", False)
            with (args.output / "samples.jsonl").open("a") as stream:
                stream.write(json.dumps(record) + "\n")
            time.sleep(
                min(
                    args.interval,
                    max(0, args.hours * 3600 - (time.monotonic() - start)),
                )
            )
    finally:
        elapsed = time.monotonic() - start
        summary = {
            "requested_real_hours": args.hours,
            "observed_real_seconds": elapsed,
            "samples": samples,
            "failed_samples": failures,
            "healthy_sample_fraction": (
                (samples - failures) / samples if samples else None
            ),
            "completed": elapsed >= args.hours * 3600,
            "expected_machines": args.tanks + 5,
            "method": "periodic availability samples; production demand must be provided separately; not a proof of indefinite uptime",
        }
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    raise SystemExit(0 if summary["completed"] and not failures else 1)


if __name__ == "__main__":
    main()
