#!/usr/bin/env python3
"""Save actual verifier output; report missing tool/licence as unverified, never passed."""

import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    """Run the licensed verifier and compare actual answers with each variant’s expectations."""
    output = HERE / "results"
    output.mkdir(exist_ok=True)
    verifier = shutil.which("verifyta")
    report = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "tool": verifier,
        "verified": False,
        "variants": {},
    }
    if not verifier:
        report["reason"] = "verifyta is unavailable"
    else:
        model = json.loads((HERE / "behaviour.json").read_text())
        for variant in ["naive", "reserved"]:
            try:
                result = subprocess.run(
                    [
                        verifier,
                        "-q",
                        "-t",
                        "1",
                        "-f",
                        str(output / variant),
                        str(HERE / "generated" / f"{variant}.xml"),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                text = result.stdout + result.stderr
                (output / f"{variant}.txt").write_text(text)
                answers = [
                    m.group(1) is None
                    for m in re.finditer(r"Formula is (NOT )?satisfied", text)
                ]
                expected = [q["expected_" + variant] for q in model["queries"]]
                report["variants"][variant] = {
                    "exit_code": result.returncode,
                    "answers": answers,
                    "expected": expected,
                    "matched": result.returncode == 0 and answers == expected,
                }
            except subprocess.TimeoutExpired:
                report["variants"][variant] = {
                    "matched": False,
                    "reason": "verifier timed out",
                }
        report["verified"] = all(v["matched"] for v in report["variants"].values())
    (output / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if report["verified"] else 2


if __name__ == "__main__":
    sys.exit(main())
