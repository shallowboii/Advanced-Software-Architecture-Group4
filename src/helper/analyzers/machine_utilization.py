"""
Machine utilization: how busy each machine was during a scheduler run.

Reads the scheduler console output that was pasted into "Batches .docx".
For every machine it adds up the time from the scheduler's "START sent"
to the machine's "done", and divides that by the length of the run:

    usage % = busy hours / run hours * 100

Run (from the repository root):
    python src/helper/analyzers/machine_utilization.py
    python src/helper/analyzers/machine_utilization.py other.docx --out somewhere/

Writes machine_utilization.csv and, if matplotlib is installed,
machine_utilization.png into the output folder (default: results/ next to
this script).
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
CSV_NAME = "machine_utilization.csv"
CHART_NAME = "machine_utilization.png"

# The lines of the log we use; everything else is skipped
SCHEDULER_LINE = re.compile(r"^(Greedy|Star)$")
BATCHES_LINE = re.compile(r"^(\d+) batches$")
EVENT_LINE = re.compile(
    r"^h\s*(\d+):\s+(\w+)\s+machine (\d+) (START sent|running|done) \(batch (\d+)\)$")
SUMMARY_LINE = re.compile(r"actual:? (\d+) hours", re.IGNORECASE)

# Production order, used for sorting the tables and the chart
STEP_ORDER = ["mashing", "lautering", "boiling", "whirlpooling",
              "fermentation_maturation", "filtering"]

STEP_COLUMN_WIDTH = 24    # widest step name: fermentation_maturation
RUN_COLUMN_WIDTH = 12     # e.g. "Greedy 100"
CHART_SIZE_INCHES = (12, 5)
CHART_DPI = 150
# One color family per scheduler, light to dark for more batches
CHART_COLORS = {"Greedy": ["#9ecae1", "#4292c6", "#08519c"],
                "Star": ["#fdae6b", "#f16913", "#a63603"]}
CHART_OTHER_COLOR = "#999999"


@dataclass
class Run:
    """One scheduler run, e.g. Greedy with 24 batches."""
    scheduler: str
    batches: int
    hours: int | None = None                    # from the summary line
    busy: dict = field(default_factory=lambda: defaultdict(int))        # (step, machine) -> hours
    batches_done: dict = field(default_factory=lambda: defaultdict(int))  # (step, machine) -> count
    open_starts: dict = field(default_factory=dict)  # (step, batch) -> hour of START sent
    warnings: list = field(default_factory=list)

    @property
    def name(self):
        return f"{self.scheduler} {self.batches}"

    def usage(self, step, machine):
        """Busy time of one machine as a percentage of the run."""
        return self.busy[(step, machine)] / self.hours * 100


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
    """Split the log into runs and add up the busy time of every machine."""
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

        # 3. Machine events: pair "START sent" with "done" of the same step and
        #    batch. The machine number is taken from "done": Star prints START
        #    with 0-based numbers and done with 1-based ones.
        elif match := EVENT_LINE.match(line):
            if run is None:
                print(f"warning: event before the first 'N batches' line: {line}")
                continue
            hour, step, machine, event, batch = (int(match[1]), match[2], int(match[3]),
                                                 match[4], int(match[5]))
            if event == "START sent":
                if (step, batch) in run.open_starts:
                    run.warnings.append(f"{step} batch {batch}: second START before done")
                run.open_starts[(step, batch)] = hour
            elif event == "done":
                start = run.open_starts.pop((step, batch), None)
                if start is None:
                    run.warnings.append(f"{step} batch {batch}: done without START (h{hour})")
                    continue
                run.busy[(step, machine)] += hour - start
                run.batches_done[(step, machine)] += 1

        # 4. The summary line gives the length of the run
        elif run is not None and (match := SUMMARY_LINE.search(line)):
            run.hours = int(match[1])

    return runs


def check(run):
    """Add warnings for anything that makes the numbers of a run unreliable."""
    for step, batch in run.open_starts:
        run.warnings.append(f"{step} batch {batch}: START without done")
    if not run.hours:
        run.warnings.append("no 'actual: N hours' summary line, so usage can't be calculated")
        return
    for (step, machine) in run.busy:
        if run.usage(step, machine) > 100:
            run.warnings.append(f"{step} machine {machine}: busy longer than the run")


def steps_of(runs):
    """All step names in production order (unknown ones at the end)."""
    seen = {step for run in runs for (step, _) in run.busy}
    return [s for s in STEP_ORDER if s in seen] + sorted(seen - set(STEP_ORDER))


def step_summary(run, step):
    """(number of machines, total busy hours, average usage %) of one step.
    The usage is None if the step didn't run or the run length is unknown."""
    machines = [m for (s, m) in run.busy if s == step]
    if not machines:
        return 0, 0, None
    busy = sum(run.busy[(step, m)] for m in machines)
    if not run.hours:
        return len(machines), busy, None
    average = sum(run.usage(step, m) for m in machines) / len(machines)
    return len(machines), busy, average


def percent(value, width):
    """A usage % right-aligned in width, or '-' if it is unknown."""
    return f"{'-' if value is None else f'{value:.1f}%':>{width}}"


def print_report(runs):
    steps = steps_of(runs)

    # 1. One table per run
    for run in runs:
        length = f"{run.hours} h" if run.hours else "length unknown"
        print(f"\n{run.scheduler}, {run.batches} batches ({length})")
        print(f"  {'step':<{STEP_COLUMN_WIDTH}} machines  busy h   usage")
        for step in steps:
            machines, busy, average = step_summary(run, step)
            if machines == 0:
                continue
            note = "  (average of the machines)" if machines > 1 else ""
            print(f"  {step:<{STEP_COLUMN_WIDTH}} {machines:>8} {busy:>7} "
                  f"{percent(average, 7)}{note}")
        for warning in run.warnings:
            print(f"  warning: {warning}")

    # 2. All runs side by side, usage % per step
    print("\nUsage % per step (average of the machines of that step)")
    header = "".join(f"{run.name:>{RUN_COLUMN_WIDTH}}" for run in runs)
    print(f"  {'step':<{STEP_COLUMN_WIDTH}}{header}")
    for step in steps:
        cells = "".join(percent(step_summary(run, step)[2], RUN_COLUMN_WIDTH) for run in runs)
        print(f"  {step:<{STEP_COLUMN_WIDTH}}{cells}")


def write_csv(runs, path):
    """One row per machine per run."""
    steps = steps_of(runs)
    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["scheduler", "batches", "run_hours", "step", "machine",
                         "batches_done", "busy_hours", "utilization_pct"])
        for run in runs:
            for (step, machine) in sorted(run.busy, key=lambda k: (steps.index(k[0]), k[1])):
                usage = f"{run.usage(step, machine):.1f}" if run.hours else ""
                writer.writerow([run.scheduler, run.batches, run.hours, step, machine,
                                 run.batches_done[(step, machine)],
                                 run.busy[(step, machine)], usage])


def draw_chart(runs, path):
    """Grouped bars: one group per step, one bar per run. Returns False if
    matplotlib isn't installed."""
    try:
        import matplotlib
        matplotlib.use("Agg")  # write a file, no window
        import matplotlib.pyplot as plt
    except ImportError:
        return False

    steps = steps_of(runs)
    runs = [run for run in runs if run.hours]
    bar_width = 0.8 / len(runs)
    shade = defaultdict(int)  # how many runs of each scheduler are drawn so far
    figure, axes = plt.subplots(figsize=CHART_SIZE_INCHES)
    for i, run in enumerate(runs):
        x = [s + (i - (len(runs) - 1) / 2) * bar_width for s in range(len(steps))]
        heights = [step_summary(run, step)[2] or 0 for step in steps]
        colors = CHART_COLORS.get(run.scheduler, [])
        color = colors[shade[run.scheduler]] if shade[run.scheduler] < len(colors) \
            else CHART_OTHER_COLOR
        shade[run.scheduler] += 1
        axes.bar(x, heights, bar_width, label=run.name, color=color)

    axes.set_xticks(range(len(steps)), [s.replace("_", "\n") for s in steps])
    axes.set_ylabel("usage % (START sent to done / run length)")
    axes.set_title("Machine utilization per step (average of the machines of a step)")
    axes.legend(ncols=2)
    axes.grid(axis="y", alpha=0.3)
    figure.tight_layout()
    figure.savefig(path, dpi=CHART_DPI)
    return True


def main():
    parser = argparse.ArgumentParser(description="Machine utilization from the scheduler log.")
    parser.add_argument("docx", nargs="?", type=Path, default=DEFAULT_DOCX,
                        help=f"the log as .docx (default: {DEFAULT_DOCX})")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help=f"folder for the CSV and the chart (default: {DEFAULT_OUT})")
    args = parser.parse_args()

    # 1. Read the log
    if not args.docx.is_file():
        sys.exit(f"error: {args.docx} not found")
    runs = parse_runs(read_docx_lines(args.docx))
    if not runs:
        sys.exit(f"error: no '<N> batches' runs found in {args.docx}")

    # 2. Check it and print the tables
    for run in runs:
        check(run)
    print_report(runs)

    # 3. Save the CSV and the chart
    args.out.mkdir(parents=True, exist_ok=True)
    write_csv(runs, args.out / CSV_NAME)
    print(f"\nwrote {args.out / CSV_NAME}")
    if draw_chart(runs, args.out / CHART_NAME):
        print(f"wrote {args.out / CHART_NAME}")
    else:
        print("chart skipped: matplotlib is not installed (pip install matplotlib)")


if __name__ == "__main__":
    main()
