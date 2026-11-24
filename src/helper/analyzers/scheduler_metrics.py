"""
Scheduler metrics, calculated from the scheduler log in "Batches .docx".

For every run (e.g. Greedy with 24 batches) it calculates:
  - throughput           batches finished per day
  - machine utilization  how much of the run each machine was processing
  - batch lead time      how long one batch takes, from mashing to filtering
  - waiting time         how long a batch waits before each step starts
The formulas are explained in README.md.

Run (from the repository root):
    python src/helper/analyzers/scheduler_metrics.py
    python src/helper/analyzers/scheduler_metrics.py other.docx --out somewhere/

Writes three CSV files and, if matplotlib is installed, four charts into the
output folder (default: results/ next to this script).
"""

import argparse
import csv
import re
import sys
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from xml.etree import ElementTree

HERE = Path(__file__).resolve().parent
DEFAULT_DOCX = HERE.parent.parent / "docs" / "Batches .docx"
DEFAULT_OUT = HERE / "results"

# The lines of the log we use; everything else is skipped
SCHEDULER_LINE = re.compile(r"^(Greedy|Star)$")
BATCHES_LINE = re.compile(r"^(\d+) batches$")
EVENT_LINE = re.compile(
    r"^h\s*(\d+):\s+(\w+)\s+machine (\d+) (START sent|running|done) \(batch (\d+)\)$")
SUMMARY_LINE = re.compile(r"actual:? (\d+) hours", re.IGNORECASE)

START, RUNNING, DONE = "START sent", "running", "done"

# Production order: a batch goes through the steps in this order
STEP_ORDER = ["mashing", "lautering", "boiling", "whirlpooling",
              "fermentation_maturation", "filtering"]

HOURS_PER_DAY = 24

# Console table layout
NAME_WIDTH = 24     # widest step name: fermentation_maturation
CELL_WIDTH = 12     # one column per run, e.g. "Greedy 100"

# Charts
CHART_DPI = 150
SCHEDULER_COLORS = {"Greedy": ["#9ecae1", "#4292c6", "#08519c"],   # blues, light to dark
                    "Star": ["#fdae6b", "#f16913", "#a63603"]}     # oranges
OTHER_COLOR = "#999999"


@dataclass
class Run:
    """One scheduler run, e.g. Greedy with 24 batches."""
    scheduler: str
    batches: int
    hours: int | None = None   # run length, from the summary line
    # (step, batch) -> {"START sent": hour, "running": hour, "done": hour, "machine": n}
    events: dict = field(default_factory=lambda: defaultdict(dict))
    warnings: list = field(default_factory=list)

    @property
    def name(self):
        return f"{self.scheduler} {self.batches}"


# ---- 1. Reading the log -------------------------------------------------------

def read_docx_lines(path):
    """Return the text of every paragraph in a .docx file, one string each.

    A .docx is a zip file; the text is in word/document.xml. Every paragraph
    (<w:p>) holds its text in one or more pieces (<w:t>), which we join."""
    word = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    with zipfile.ZipFile(path) as docx:
        document = ElementTree.fromstring(docx.read("word/document.xml"))
    return ["".join(piece.text or "" for piece in paragraph.iter(word + "t")).strip()
            for paragraph in document.iter(word + "p")]


def parse_runs(lines):
    """Split the log into runs and collect the event times of every step."""
    runs = []
    scheduler = None
    run = None

    for line in lines:
        # 1. A scheduler name ("Greedy", "Star") comes before its runs
        if match := SCHEDULER_LINE.match(line):
            scheduler = match[1]

        # 2. "24 batches" starts a new run
        elif match := BATCHES_LINE.match(line):
            run = Run(scheduler or "unknown", int(match[1]))
            runs.append(run)

        # 3. Machine events, stored per (step, batch). The machine number is
        #    taken from "done": Star prints START with 0-based numbers and
        #    done with 1-based ones.
        elif match := EVENT_LINE.match(line):
            if run is None:
                print(f"warning: event before the first 'N batches' line: {line}")
                continue
            hour, step, machine, event, batch = (int(match[1]), match[2], int(match[3]),
                                                 match[4], int(match[5]))
            times = run.events[(step, batch)]
            if event in times:
                run.warnings.append(f"{step} batch {batch}: '{event}' twice, kept the first")
                continue
            times[event] = hour
            if event == DONE:
                times["machine"] = machine

        # 4. The summary line gives the length of the run
        elif run is not None and (match := SUMMARY_LINE.search(line)):
            run.hours = int(match[1])

    return runs


def steps_of(runs):
    """All step names in production order (unknown ones at the end)."""
    seen = {step for run in runs for (step, _) in run.events}
    return [s for s in STEP_ORDER if s in seen] + sorted(seen - set(STEP_ORDER))


def is_complete(times):
    return START in times and RUNNING in times and DONE in times


def check(run):
    """Add warnings for anything that makes the numbers of a run unreliable."""
    for (step, batch), times in sorted(run.events.items()):
        missing = [e for e in (START, RUNNING, DONE) if e not in times]
        if missing:
            run.warnings.append(f"{step} batch {batch}: no {' / '.join(missing)}, step skipped")
    if not run.hours:
        run.warnings.append("no 'actual: N hours' summary line: "
                            "throughput and utilization can't be calculated")


# ---- 2. The metrics --------------------------------------------------------

def throughput(run):
    """Batches finished per day: batches / run length * 24."""
    if not run.hours:
        return None
    return len(batch_times(run)) / run.hours * HOURS_PER_DAY


def machine_utilization(run):
    """Per machine: (step, machine) -> (batches done, processing hours, utilization %).

    processing hours = sum of (done - running) over the batches of that machine
    utilization %    = processing hours / run length * 100"""
    processing = defaultdict(int)
    batches_done = defaultdict(int)
    for (step, _), times in run.events.items():
        if is_complete(times):
            key = (step, times["machine"])
            processing[key] += times[DONE] - times[RUNNING]
            batches_done[key] += 1

    result = {}
    for key, hours in processing.items():
        percent = hours / run.hours * 100 if run.hours else None
        if percent is not None and percent > 100:
            run.warnings.append(f"{key[0]} machine {key[1]}: processing longer than the run")
        result[key] = (batches_done[key], hours, percent)
    return result


def step_utilization(utilization, step):
    """Average, lowest and highest utilization % of the machines of one step."""
    values = [pct for (s, _), (_, _, pct) in utilization.items() if s == step and pct is not None]
    if not values:
        return None, None, None
    return sum(values) / len(values), min(values), max(values)


def batch_times(run):
    """Per batch: {"lead": hours, "wait": {step: hours}}, for complete batches only.

    waiting time of a step = running of that step - done of the step before
                             (for the first step: running - START sent)
    lead time              = done of the last step - START sent of the first step
                           = all waiting times + all processing times"""
    steps = [s for s in steps_of([run])]
    result = {}
    for batch in sorted({b for (_, b) in run.events}):
        times = [run.events.get((step, batch), {}) for step in steps]
        if not all(is_complete(t) for t in times):
            continue  # check() already warned about the missing events

        wait = {}
        previous_done = None
        for step, t in zip(steps, times):
            ready = t[START] if previous_done is None else previous_done
            wait[step] = t[RUNNING] - ready
            previous_done = t[DONE]
        lead = times[-1][DONE] - times[0][START]

        # Waiting + processing must add up to the lead time
        processing = sum(t[DONE] - t[RUNNING] for t in times)
        if sum(wait.values()) + processing != lead:
            run.warnings.append(f"batch {batch}: waiting + processing != lead time")
        result[batch] = {"lead": lead, "wait": wait}
    return result


def average(values):
    values = list(values)
    return sum(values) / len(values) if values else None


# ---- 3. Console tables -------------------------------------------------------

def cell(value, unit="", decimals=1):
    """A number right-aligned in one column, or '-' if it is unknown."""
    text = "-" if value is None else f"{value:.{decimals}f}{unit}"
    return f"{text:>{CELL_WIDTH}}"


def table(title, runs, rows):
    """Print rows of (label, [one value per run]) with a header line of run names."""
    print(f"\n{title}")
    print(f"  {'':<{NAME_WIDTH}}" + "".join(f"{run.name:>{CELL_WIDTH}}" for run in runs))
    for label, cells in rows:
        print(f"  {label:<{NAME_WIDTH}}" + "".join(cells))


def print_report(runs, metrics):
    steps = steps_of(runs)

    # 1. Throughput and lead time
    rows = [("run length (h)", [cell(r.hours, decimals=0) for r in runs]),
            ("throughput (batches/day)", [cell(metrics[r.name]["throughput"], decimals=2)
                                          for r in runs])]
    for label, pick in (("lead time avg (h)", average), ("lead time min (h)", min),
                        ("lead time max (h)", max)):
        rows.append((label, [cell(pick([b["lead"] for b in metrics[r.name]["batches"].values()])
                                  if metrics[r.name]["batches"] else None) for r in runs]))
    table("Throughput and batch lead time", runs, rows)

    # 2. Utilization per step (steps with several machines: average, then range)
    rows = []
    for step in steps:
        rows.append((step, [cell(step_utilization(metrics[r.name]["utilization"], step)[0], "%")
                            for r in runs]))
        if any(sum(1 for (s, _) in metrics[r.name]["utilization"] if s == step) > 1 for r in runs):
            ranges = []
            for r in runs:
                _, low, high = step_utilization(metrics[r.name]["utilization"], step)
                ranges.append(f"{'-' if low is None else f'{low:.0f}-{high:.0f}%':>{CELL_WIDTH}}")
            rows.append(("  (lowest-highest machine)", ranges))
    table("Machine utilization: processing time / run length "
          "(steps with several machines: average)", runs, rows)

    # 3. Waiting time per step, average per batch
    rows = [(step, [cell(average(b["wait"][step] for b in metrics[r.name]["batches"].values()), " h")
                    for r in runs]) for step in steps]
    rows.append(("total", [cell(average(sum(b["wait"].values())
                                        for b in metrics[r.name]["batches"].values()), " h")
                           for r in runs]))
    table("Waiting time per step: average hours a batch waits before the step runs", runs, rows)

    for run in runs:
        for warning in run.warnings:
            print(f"warning ({run.name}): {warning}")


# ---- 4. CSV files ------------------------------------------------------------

def write_csvs(runs, metrics, folder):
    steps = steps_of(runs)
    written = []

    # 1. One row per run
    path = folder / "runs.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["scheduler", "batches", "run_hours", "throughput_per_day",
                         "lead_time_avg_h", "lead_time_min_h", "lead_time_max_h"])
        for run in runs:
            m = metrics[run.name]
            leads = [b["lead"] for b in m["batches"].values()]
            writer.writerow([run.scheduler, run.batches, run.hours,
                             "" if m["throughput"] is None else f"{m['throughput']:.3f}",
                             f"{average(leads):.1f}" if leads else "",
                             min(leads, default=""), max(leads, default="")])
    written.append(path)

    # 2. One row per machine per run
    path = folder / "machines.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["scheduler", "batches", "run_hours", "step", "machine",
                         "batches_done", "processing_hours", "utilization_pct"])
        for run in runs:
            utilization = metrics[run.name]["utilization"]
            for (step, machine) in sorted(utilization, key=lambda k: (steps.index(k[0]), k[1])):
                done, hours, percent = utilization[(step, machine)]
                writer.writerow([run.scheduler, run.batches, run.hours, step, machine, done,
                                 hours, "" if percent is None else f"{percent:.1f}"])
    written.append(path)

    # 3. One row per batch per run
    path = folder / "batches.csv"
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["scheduler", "batches", "batch", "lead_time_h"]
                        + [f"wait_{step}_h" for step in steps])
        for run in runs:
            for batch, b in metrics[run.name]["batches"].items():
                writer.writerow([run.scheduler, run.batches, batch, b["lead"]]
                                + [b["wait"].get(step, "") for step in steps])
    written.append(path)
    return written


# ---- 5. Charts ---------------------------------------------------------------

def run_colors(runs):
    """One color per run: blues for Greedy, oranges for Star, darker for more batches."""
    colors, used = {}, defaultdict(int)
    for run in sorted(runs, key=lambda r: r.batches):
        family = SCHEDULER_COLORS.get(run.scheduler, [])
        i = used[run.scheduler]
        colors[run.name] = family[i] if i < len(family) else OTHER_COLOR
        used[run.scheduler] += 1
    return colors


def chart_throughput(plt, runs, metrics, path):
    """Line chart: batches per day against the number of batches, one line per scheduler."""
    figure, axes = plt.subplots(figsize=(7, 4.5))
    for index, scheduler in enumerate(dict.fromkeys(r.scheduler for r in runs)):
        label_offset = (0, -14) if index == 0 else (0, 8)  # first scheduler's labels below
        points = sorted((r.batches, metrics[r.name]["throughput"]) for r in runs
                        if r.scheduler == scheduler and metrics[r.name]["throughput"])
        x, y = [p[0] for p in points], [p[1] for p in points]
        color = SCHEDULER_COLORS.get(scheduler, [OTHER_COLOR])[-1]
        axes.plot(x, y, marker="o", color=color, label=scheduler)
        for xi, yi in zip(x, y):
            axes.annotate(f"{yi:.2f}", (xi, yi), textcoords="offset points", xytext=label_offset,
                          ha="center", fontsize=8, color=color)
    axes.set_xticks(sorted({r.batches for r in runs}))
    axes.set_xlabel("batches in the run")
    axes.set_ylabel("throughput (batches per day)")
    axes.set_title("Throughput")
    axes.grid(alpha=0.3)
    axes.legend()
    figure.tight_layout()
    figure.savefig(path, dpi=CHART_DPI)


def chart_utilization(plt, runs, metrics, path):
    """Heatmap: one row per machine, one column per run, color = utilization %."""
    steps = steps_of(runs)
    machines = sorted({key for r in runs for key in metrics[r.name]["utilization"]},
                      key=lambda k: (steps.index(k[0]), k[1]))
    multi = {s for s in steps if sum(1 for (st, _) in machines if st == s) > 1}
    # Steps with several machines get a number: "fermentation 3"
    labels = [f"{step.split('_')[0]} {machine}" if step in multi else step
              for step, machine in machines]

    values = [[metrics[r.name]["utilization"].get(key, (None, None, None))[2] for r in runs]
              for key in machines]
    grid = [[float("nan") if v is None else v for v in row] for row in values]

    figure, axes = plt.subplots(figsize=(8, 0.32 * len(machines) + 1.5))
    image = axes.imshow(grid, cmap="Blues", vmin=0, vmax=100, aspect="auto")
    for i, row in enumerate(values):
        for j, v in enumerate(row):
            if v is not None:
                axes.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=7,
                          color="white" if v > 60 else "black")
    axes.set_xticks(range(len(runs)), [r.name for r in runs])
    axes.set_yticks(range(len(machines)), labels, fontsize=8)
    axes.set_title("Machine utilization (% of the run spent processing)")
    figure.colorbar(image, ax=axes, label="utilization %")
    figure.tight_layout()
    figure.savefig(path, dpi=CHART_DPI)


def chart_lead_time(plt, runs, metrics, path):
    """Box plot: the spread of batch lead times in each run (triangle = average)."""
    shown = [r for r in runs if metrics[r.name]["batches"]]
    data = [[b["lead"] for b in metrics[r.name]["batches"].values()] for r in shown]
    colors = run_colors(shown)
    figure, axes = plt.subplots(figsize=(8, 4.5))
    boxes = axes.boxplot(data, patch_artist=True, showmeans=True,
                         medianprops={"color": "black"})
    for box, run in zip(boxes["boxes"], shown):
        box.set_facecolor(colors[run.name])
    axes.set_xticks(range(1, len(shown) + 1), [r.name for r in shown])
    axes.set_ylabel("lead time per batch (hours)")
    axes.set_title("Batch lead time: from mashing START to filtering done")
    axes.grid(axis="y", alpha=0.3)
    figure.tight_layout()
    figure.savefig(path, dpi=CHART_DPI)


def chart_waiting(plt, runs, metrics, path):
    """Stacked bars: average waiting time per batch, split by the step it waits for."""
    steps = steps_of(runs)
    shown = [r for r in runs if metrics[r.name]["batches"]]
    figure, axes = plt.subplots(figsize=(8, 4.5))
    bottom = [0.0] * len(shown)
    for step in steps:
        heights = [average(b["wait"][step] for b in metrics[r.name]["batches"].values())
                   for r in shown]
        axes.bar([r.name for r in shown], heights, bottom=bottom, label=step)
        bottom = [b + h for b, h in zip(bottom, heights)]
    for x, total in enumerate(bottom):
        axes.annotate(f"{total:.1f} h", (x, total), textcoords="offset points", xytext=(0, 3),
                      ha="center", fontsize=8)
    axes.set_ylabel("average waiting time per batch (hours)")
    axes.set_title("Waiting time per step")
    axes.legend(fontsize=8)
    axes.grid(axis="y", alpha=0.3)
    figure.tight_layout()
    figure.savefig(path, dpi=CHART_DPI)


def draw_charts(runs, metrics, folder):
    """Draws all charts. Returns the files written, or None without matplotlib."""
    try:
        import matplotlib
        matplotlib.use("Agg")  # write files, no window
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    charts = {"throughput.png": chart_throughput,
              "utilization_heatmap.png": chart_utilization,
              "lead_time_boxplot.png": chart_lead_time,
              "waiting_time.png": chart_waiting}
    written = []
    for name, draw in charts.items():
        draw(plt, runs, metrics, folder / name)
        plt.close("all")
        written.append(folder / name)
    return written


# ---- main --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scheduler metrics from the scheduler log.")
    parser.add_argument("docx", nargs="?", type=Path, default=DEFAULT_DOCX,
                        help=f"the log as .docx (default: {DEFAULT_DOCX})")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help=f"folder for the CSV files and charts (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    # 1. Read the log
    if not args.docx.is_file():
        sys.exit(f"error: {args.docx} not found")
    runs = parse_runs(read_docx_lines(args.docx))
    if not runs:
        sys.exit(f"error: no '<N> batches' runs found in {args.docx}")

    # 2. Check it and calculate everything once
    metrics = {}
    for run in runs:
        check(run)
        metrics[run.name] = {"utilization": machine_utilization(run),
                             "batches": batch_times(run)}
        metrics[run.name]["throughput"] = throughput(run)

    # 3. Print, then save the CSV files and the charts
    print_report(runs, metrics)
    args.out.mkdir(parents=True, exist_ok=True)
    print()
    for path in write_csvs(runs, metrics, args.out):
        print(f"wrote {path}")
    charts = draw_charts(runs, metrics, args.out)
    if charts is None:
        print("charts skipped: matplotlib is not installed (pip install matplotlib)")
    for path in charts or []:
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
