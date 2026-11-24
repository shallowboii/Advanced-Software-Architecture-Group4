# Analyzers

Python scripts that analyze the data from the scheduler experiments.

## machine_utilization.py

Measures how busy every machine was in each scheduler run. It reads the scheduler console output that was pasted into [`src/docs/Batches .docx`](../../docs/) (the file name has a space before `.docx`).

### What it calculates

A machine counts as **busy** from the moment the scheduler sends `START` for a batch until the machine reports `done` for that batch:

```text
busy hours = sum over its batches of (hour of "done" - hour of "START sent")
usage %    = busy hours / run length * 100
```

- **Run length** is the `actual: N hours` from the run's summary line, which is also the hour of the last `done`.
- Hours are simulated hours, as printed in the log (`h  12: ...`).
- For a step with several machines, the step's usage is the average of its machines. Today that's `fermentation_maturation`, with 24 tanks. Every other step has 1 machine.

### Run it

Python 3.10 or newer. From the repository root:

```bash
python src/helper/analyzers/machine_utilization.py
```

Options:

```bash
python src/helper/analyzers/machine_utilization.py other_log.docx   # another input file
python src/helper/analyzers/machine_utilization.py --out somewhere/ # another output folder
```

The chart needs matplotlib. Without it the script still prints the tables and writes the CSV, and it says that the chart was skipped.

```bash
pip install -r src/helper/analyzers/requirements.txt
```

### Output

- **Console:** one table per run (step, machines, busy hours, usage %), then all runs side by side.
- **`results/machine_utilization.csv`:** one row per machine per run, with the columns `scheduler, batches, run_hours, step, machine, batches_done, busy_hours, utilization_pct`.
- **`results/machine_utilization.png`:** a bar chart of usage % per step. Greedy runs are blue and Star runs orange, darker for more batches.

`results/` is created next to the script.

### Input format

The script reads every paragraph of the docx as one line and uses only these:

| Line | Meaning |
| --- | --- |
| `Greedy` / `Star` | the scheduler of the runs that follow |
| `24 batches` | starts a new run |
| `h  12: mashing          machine 1 START sent (batch 3)` | the scheduler sent the start |
| `h  14: mashing          machine 1 done (batch 3)` | the machine reported done |
| `... actual: 529 hours ...` | the run's length (Greedy: `Planned makespan: ..., actual: N hours`, Star: `Actual: N hours, ...`) |

Everything else (`running`, tank lines, `export`/`mvn`, SLF4J warnings, ...) is skipped.

A `START sent` is paired with the `done` of the **same step and batch**. The machine number is taken from the `done` line, because the Star scheduler prints `START` with 0-based machine numbers (`machine 0`) and `done` with 1-based ones (`machine 1`).

The script warns, instead of stopping, about:
- a `START` without a `done`,
- a `done` without a `START`,
- a run without a summary line (its usage is shown as `-`),
- a machine that is busy for longer than the run.

A machine only shows up if it finished at least one batch, so a machine that was never used doesn't count as 0% in a step's average.
