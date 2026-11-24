# Analyzers

Python scripts that analyze the data from the scheduler experiments.

## scheduler_metrics.py

Compares the schedulers using the scheduler console output. The output can be pasted into a Word document, like [`src/docs/Batches .docx`](../../docs/) (the file name has a space before `.docx`), or simply saved as a text file. For every run (Greedy and Star, 24 / 50 / 100 batches) it calculates:

| Metric | Question it answers |
| --- | --- |
| **Throughput** | How many batches does the plant finish per day? |
| **Machine utilization** | How much of the run was each machine actually working? |
| **Batch lead time** | How long does one batch take from start to finish? |
| **Waiting time per step** | Where do batches sit and wait? |

### Run it

Python 3.10 or newer. From the repository root:

```bash
py -3.12 -m pip install -r src/helper/analyzers/requirements.txt   # once, for the charts
py -3.12 src/helper/analyzers/scheduler_metrics.py
```

Without matplotlib (`python src/helper/analyzers/scheduler_metrics.py`) it still prints the tables and writes the CSV files; it only skips the charts.

With no file given, it reads `src/docs/Batches .docx`. To analyze another log, pass it as the first argument:

```bash
py -3.12 src/helper/analyzers/scheduler_metrics.py runs.txt             # a text file
py -3.12 src/helper/analyzers/scheduler_metrics.py other_log.docx       # another Word document
py -3.12 src/helper/analyzers/scheduler_metrics.py runs.txt --out out/  # another output folder
```

### Using a text file instead of Word

A `.docx` file is read as a Word document. Any other file (`.txt`, `.log`, ...) is read as plain text, one line per log line. Both give exactly the same results.

The easiest way to get a text file is to save the scheduler output directly while it runs. Write the scheduler name and the number of batches on top, so the script knows which run it is:

```bash
echo "Greedy"     >  runs.txt
echo "24 batches" >> runs.txt
mvn -q compile exec:java -Dexec.args="24" >> runs.txt 2>&1

echo "Star"       >> runs.txt
echo "24 batches" >> runs.txt
mvn -q compile exec:java -Dexec.mainClass=StarScheduler -Dexec.args="24" >> runs.txt 2>&1
```

You can also copy the console output into Notepad and save it as `runs.txt`. A text file looks like this:

```text
Greedy
24 batches
h   0: mashing          machine 1 START sent (batch 1)
h   1: mashing          machine 1 running (batch 1)
h   2: mashing          machine 1 done (batch 1)
...
Planned makespan: 484 hours, actual: 529 hours, failed batches: 0
50 batches
...
Star
24 batches
...
Actual: 492 hours, batches started: 24, failed batches: 0
```

One file can hold any number of runs. Extra lines (warnings, `export`, `mvn`, the tank plan) can stay in; they are skipped. Each run needs:
- its `Greedy` / `Star` line (once, before the scheduler's first run),
- its `N batches` line,
- its summary line with `actual: N hours`, or the throughput and utilization can't be calculated.

The encoding doesn't matter: characters that aren't UTF-8 (e.g. Polish letters saved by Notepad in an older encoding) are replaced and don't stop the script.

## How the values are calculated

For every step of every batch the log has three times, in simulated hours:

| Time | Log line |
| --- | --- |
| **START** | `h  2: lautering machine 1 START sent (batch 1)`: the scheduler sends the start |
| **RUNNING** | `h  3: lautering machine 1 running (batch 1)`: the machine starts working |
| **DONE** | `h  5: lautering machine 1 done (batch 1)`: the machine has finished |

From these:

```text
processing time  = DONE - RUNNING                    the machine is working on the batch
waiting time     = RUNNING - DONE of the step before  the batch is ready but nothing happens yet
                   (mashing has no step before: RUNNING - START)

lead time        = DONE of filtering - START of mashing
                 = all waiting times + all processing times

utilization      = all processing times of one machine / run length x 100 %
throughput       = finished batches / run length x 24  (batches per day)
run length       = the "actual: N hours" of the run's summary line
```

### Example: batch 1 of Greedy 24

| Step | START | RUNNING | DONE | waiting | processing |
| --- | --- | --- | --- | --- | --- |
| mashing | 0 | 1 | 2 | 1 - 0 = **1** | 2 - 1 = **1** |
| lautering | 2 | 3 | 5 | 3 - 2 = **1** | 5 - 3 = **2** |
| boiling | 5 | 5 | 6 | 5 - 5 = **0** | 6 - 5 = **1** |
| whirlpooling | 6 | 6 | 7 | 6 - 6 = **0** | 7 - 6 = **1** |
| fermentation_maturation | 7 | 7 | 343 | 7 - 7 = **0** | 343 - 7 = **336** |
| filtering | 343 | 344 | 348 | 344 - 343 = **1** | 348 - 344 = **4** |
| **sum** | | | | **3** | **345** |

Lead time = 348 - 0 = **348 h** = 3 h waiting + 345 h processing.

### Details

- **Utilization counts only processing time.** The time a batch waits in front of a machine (START to RUNNING) is not counted as work, but as waiting time for that step. This matters for Star: it sends START early, so its batches queue in front of the machines.
- **Steps with several machines:** `fermentation_maturation` has 24 tanks. The tables show their average and the lowest-highest tank. The heatmap and `machines.csv` show every tank.
- **Throughput** includes the start of the run, when the plant is still filling up, and the end, when the last batches are fermenting. That's why it grows with the number of batches.
- **Machine numbers** come from the DONE line. Star prints START with 0-based numbers (`machine 0`) and RUNNING/DONE with 1-based ones (`machine 1`), so the three lines are matched by **step and batch**, not by machine number.
- The script checks that waiting + processing adds up to the lead time for every batch.

## Output

Everything goes into `results/` next to the script, or into the folder given with `--out`.

| File | Content |
| --- | --- |
| `runs.csv` | one row per run: run length, throughput, lead time avg / min / max |
| `machines.csv` | one row per machine per run: batches done, processing hours, utilization % |
| `batches.csv` | one row per batch per run: lead time and the waiting time of every step |
| `throughput.png` | line chart: batches per day against the number of batches, Greedy vs Star |
| `utilization_heatmap.png` | heatmap: utilization % of every machine (rows) in every run (columns) |
| `lead_time_boxplot.png` | box plot: spread of the batch lead times per run (triangle = average) |
| `waiting_time.png` | stacked bars: average waiting time per batch, split by step |

The console also prints three tables:
- throughput and lead time,
- utilization per step,
- waiting time per step.

## Input format

Every paragraph of a `.docx`, or every line of a text file, is read as one line. Only these lines are used:

| Line | Meaning |
| --- | --- |
| `Greedy` / `Star` | the scheduler of the runs that follow |
| `24 batches` | starts a new run |
| `h  12: mashing machine 1 START sent (batch 3)` | START |
| `h  13: mashing machine 1 running (batch 3)` | RUNNING |
| `h  14: mashing machine 1 done (batch 3)` | DONE |
| `... actual: 529 hours ...` | run length (Greedy: `Planned makespan: ..., actual: N hours`, Star: `Actual: N hours, ...`) |

Everything else is skipped: tank lines, `export`/`mvn`, SLF4J warnings, and so on.

The script prints a warning instead of stopping if:
- **A step of a batch misses START, RUNNING or DONE.** That step is left out, and the batch is left out of lead time and waiting time.
- **A run has no summary line.** Its throughput and utilization are shown as `-`.
- **A machine processes for longer than the run.**
