#!/usr/bin/env python3
"""Export paired treatment differences and a standalone SVG from measured runs."""
import argparse
import html
import json
import random
import statistics
from pathlib import Path
from analyse import load_events, summarise, paired_comparison


def interval(values):
    if len(values) < 2:
        return None
    rng = random.Random(0)
    estimates = sorted(statistics.mean(rng.choices(values, k=len(values))) for _ in range(2000))
    return [estimates[49], estimates[1949]]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runs', nargs='+', type=Path)
    parser.add_argument('--output', type=Path, default=Path('comparison'))
    args = parser.parse_args()
    records = []
    for path in args.runs:
        manifest = json.loads((path / 'manifest.json').read_text())
        if manifest.get('outcome') != 'completed':
            raise ValueError(f'{path}: incomplete/failed run cannot silently enter treatment comparison')
        summary = summarise(load_events(path / 'events.jsonl'), manifest['measurement_start_h'], manifest['measurement_end_h'])
        records.append((manifest, summary))
    pairs = paired_comparison(records)
    groups = {}
    for pair in pairs:
        groups.setdefault((pair['scenario'], pair['tanks']), []).append(pair)
    report = []
    for (scenario, tanks), group in groups.items():
        entry = {'scenario': scenario, 'tanks': tanks, 'paired_runs': len(group), 'direction': 'CONWIP minus PUSH'}
        for field in ['waste_difference_hl', 'throughput_difference']:
            values = [p[field] for p in group]
            entry[field] = {'mean': statistics.mean(values), 'bootstrap_95_percent_interval': interval(values)}
        report.append(entry)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'paired-results.json').write_text(json.dumps({'groups': report, 'pairs': pairs}, indent=2) + '\n')
    # Export a self-contained deterministic chart, without a browser plotting dependency.
    height = 100 + 45 * len(records)
    maximum = max((r['completed_per_day'] or 0 for _, r in records), default=1) or 1
    rows = ['<svg xmlns="http://www.w3.org/2000/svg" width="900" height="' + str(height) + '" viewBox="0 0 900 ' + str(height) + '">',
            '<rect width="100%" height="100%" fill="white"/><text x="24" y="32" font-family="sans-serif" font-size="20">Measured completed batches per simulated day</text>']
    for i, (manifest, result) in enumerate(records):
        y = 65 + i * 45
        value = result['completed_per_day'] or 0
        label = html.escape(f"{manifest['policy']} / {manifest['tanks']} tanks / {manifest.get('seed')}")
        rows += [f'<text x="24" y="{y+18}" font-family="sans-serif" font-size="13">{label}</text>',
                 f'<rect x="300" y="{y}" height="25" width="{value/maximum*450:.2f}" fill="#147d64"/>',
                 f'<text x="{310+value/maximum*450:.2f}" y="{y+18}" font-family="sans-serif" font-size="13">{value:.3f}</text>']
    rows.append('</svg>')
    (args.output / 'throughput.svg').write_text('\n'.join(rows))
    print(f'Wrote {args.output}; {len(pairs)} seeded pairs. No inference is made for unseeded deterministic runs.')


if __name__ == '__main__':
    main()
