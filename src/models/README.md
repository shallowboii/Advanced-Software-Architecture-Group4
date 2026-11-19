# Formal model and transformation

`behaviour.json` is a small timed-statechart architectural description: templates, states, invariants, transitions, guards, synchronisation and effects. `generate.py` preserves those elements in UPPAAL XML and renders the same structure in PlantUML. `generated/traceability.json` links generated locations/transitions back to source elements and requirement IDs. Unsupported state/transition constructs fail validation.

The model abstracts two upstream batches and one downstream destination. The `naive` variant admits conflicting reservations; `reserved` checks exclusive reservation before starting transfer. This models the ownership mechanism, not the full six-stage line or every distributed failure. Both variants include a bounded STOP interval and ageing during STOP. The guard/state difference is explicit in the generated files.

Verified on 27 October 2026 with UPPAAL 5.0.0: all five queries hold for `reserved`; `naive` violates both exclusivity properties as intended. The other three queries hold in both variants. Actual counterexample traces are `results/naive-1` and `results/naive-2`, alongside reachability witnesses and raw verifier output. The process counter is named `process_ticks` because `progress` is a reserved UPPAAL keyword.

Run `python3 src/models/generate.py`, then `python3 src/models/verify.py`. The verifier writes raw output, shortest diagnostic traces where available, and a result manifest. A missing licence or tool yields exit 2 and `verified:false`; it is not a successful proof. Inspect the failed naive reservation query's trace in UPPAAL and retain it with the result artifacts. Do not invent a trace or reuse the old CIP model's results.

The discrete progress counter deliberately abstracts continuous pumping. Timed ticks advance age during STOP but process/transfer counters advance only while enabled. This represents a process model in simulated units, not a physical <=1-second stop proof. Separate runtime tests measure local and global stop latency in real seconds.

Current generated queries check reservation exclusivity, destination ownership, deadlock freedom and reachability. Further properties from the implementation plan (coordinator fencing, network reconnection, end-to-end command idempotency and bounded stop response) need additional abstractions or runtime evidence; this model does not claim to prove them. Validating the correspondence of controller traces to this abstraction remains part of the acceptance work.
