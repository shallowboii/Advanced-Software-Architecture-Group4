# Production experiments

Use an isolated factory with fresh volumes for each run. The runner refuses an active factory and preserves failed runs. Do not reuse demonstration state for scientific comparisons. Start the short-fixture smoke factory with `make up PROFILE=smoke`, then:

```sh
python3 src/experiment/run.py --pilot --tanks 1 --run-id pilot-conwip
python3 src/experiment/analyse.py src/experiment/results/pilot-conwip
```

For full profiles use the real-duration recipes, `--tanks 20` or `40`, and `--policy PUSH` or `CONWIP`. Each policy/count/seed/scenario combination gets an isolated Compose project or namespace and fresh data volumes. Pass its frontend URL with `--base-url`. The runner captures recipes, machine identities, Git SHA, scenario, submitted actions, input schedule, raw canonical events, sampled machine states and final orders/status. Supply image digests and host/resource information alongside the manifest before publishing results.

Scenarios in `scenarios/` cover steady Ale, a switch to Lager, and reconfiguration: adding a recipe, changing a version, urgent demand, an explicit batch flush and STOP/resume. Select one with `--scenario`. No process code changes are needed to publish a recipe. The reconfiguration scenario flushes the oldest viable batch currently assigned to a unitank, using the same selection rule under both policies and recording the selected ID. Custom `flush_order` actions can target a particular scheduled order. If there is no eligible target, the artifact records an unapplied injection and the run fails instead of silently changing the intervention.

`--seed N` adds reproducible arrival jitter. Pair the same seed across PUSH and CONWIP with matching scenario and tank count. Repeating unseeded deterministic inputs checks reproducibility but does not create independent statistical samples. Pilot-test acceleration and scheduling lag before choosing repetitions. The default experiment has 42 simulated days of warm-up and 90 measured days. A full run at two real seconds per simulated hour takes about 106 minutes.

The runner stops admission and latches STOP on exit, then exports history in stable append order. Ageing continues under STOP. Measurement uses recorded simulated event time and excludes outcomes after the declared horizon. Resume only after preserving the run's data. The runner never deletes state or creates results for a failed endpoint.

`analyse.py` deduplicates event IDs, counts each loss once, separates operator discard from spoilage and rejects contradictory terminal outcomes. It exports throughput, waste, WIP, sampled machine intervals and utilisation fractions. Sampling resolution is recorded and does not establish exact sub-sample STOP latency.

```sh
python3 src/experiment/compare.py results/push-seed1 results/conwip-seed1 results/push-seed2 results/conwip-seed2 --output comparison
```

Comparison exports paired differences, bootstrap intervals where at least two seed pairs exist, and a standalone SVG chart. It rejects failed runs. A small number of repetitions does not support strong statistical conclusions; choose the experiment size before interpreting the results.

Availability and live deployment require separate real-time observations:

```sh
python3 src/tools/soak.py --hours 24 --tanks 20 --output soak-results
python3 src/tools/rollout_check.py --output rollout-result.json
```

Provide sustained production demand during the soak. The script checks all expected machine identities and reports sampled availability; it cannot prove indefinite uptime. The rollout script requires an active batch and a Kubernetes context with the intended factory. Neither script's existence constitutes a completed measurement. Actual raw production/soak/rollout results have not been collected in this environment.
