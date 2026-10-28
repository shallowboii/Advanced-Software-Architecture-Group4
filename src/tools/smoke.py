#!/usr/bin/env python3
"""Check real HTTP, controller heartbeats, production, telemetry and line STOP."""

import argparse
import json
import subprocess
import time
import uuid
from http_client import Api


def until(function, timeout, label):
    """Poll until a predicate returns a value, retaining the last transient error."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = function()
            if result:
                return result
        except (
            OSError,
            RuntimeError,
            subprocess.SubprocessError,
            json.JSONDecodeError,
        ) as error:
            last = str(error)
        time.sleep(0.5)
    raise RuntimeError(f"Timed out: {label}; last error: {last}")


def main():
    """Exercise one real batch across six machines, STOP/resume and telemetry."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args()
    api = Api(args.base_url)
    until(
        lambda: api.call("/health").get("status") == "ok",
        args.timeout,
        "backend readiness",
    )
    expected = {
        kind + "-0"
        for kind in ["mash", "lauter", "kettle", "whirlpool", "unitank", "filter"]
    }
    until(
        lambda: expected
        <= {m["machine_id"] for m in api.call("/machines") if m.get("online")},
        args.timeout,
        "all six controller types online",
    )
    key = "smoke-" + str(uuid.uuid4())
    order = api.call("/orders", {"recipe_id": "ale", "quantity": 1, "priority": 0}, key)
    repeated = api.call(
        "/orders", {"recipe_id": "ale", "quantity": 1, "priority": 0}, key
    )
    assert order["id"] == repeated["id"], "Order idempotency failed"
    batches = until(
        lambda: [
            b
            for b in api.call("/batches")
            if b["order_id"] == order["id"] and b["status"] == "PROCESSING"
        ],
        args.timeout,
        "batch processing",
    )
    batch_id = batches[0]["id"]
    api.call("/line/stop", {}, key + "-stop")
    until(lambda: api.call("/line").get("stopped"), 10, "line stop latch")
    until(
        lambda: expected
        <= {
            m["machine_id"]
            for m in api.call("/machines")
            if m.get("online") and m.get("global_stop")
        },
        10,
        "all six global stop acknowledgements",
    )
    api.call("/line/resume", {}, key + "-resume")
    completed = until(
        lambda: next(
            (
                b
                for b in api.call("/batches")
                if b["id"] == batch_id and b["status"] == "COMPLETED"
            ),
            None,
        ),
        args.timeout,
        "full six-machine batch",
    )
    assert completed["volume_hl"] == 50
    until(
        lambda: api.call("/telemetry?batch_id=" + batch_id),
        30,
        "real Kafka telemetry persisted",
    )
    events = api.call("/events?batch_id=" + batch_id + "&limit=1000")
    terminal = [
        e
        for e in events
        if e.get("batch_id") == batch_id and e.get("event") == "COMPLETED"
    ]
    assert len(terminal) == 1, f"Expected one terminal outcome, got {len(terminal)}"
    print(
        f"PASS: six controller types, idempotent order, STOP/resume, batch {batch_id}, telemetry and unique completion"
    )


if __name__ == "__main__":
    main()
