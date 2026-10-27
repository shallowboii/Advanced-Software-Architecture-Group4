#!/usr/bin/env python3
"""Reduce immutable production events, detecting double outcomes and invalid traces."""
import argparse
import csv
import json
from pathlib import Path


def summarise(events, start_h=0, end_h=None):
    seen, batches = {}, {}
    canonical_losses = {e.get('batch_id') for e in events if e['event'] == 'LOSS_RECORDED'}
    counts = {'started': 0, 'completed': 0, 'spoiled': 0, 'operator_discarded': 0}
    for event in sorted(events, key=lambda e: (float(e['sim_time_h']), e['ts_utc'], e['event_id'])):
        ident = event['event_id']
        if ident in seen:
            if seen[ident] != event:
                raise ValueError(f'Event {ident} has conflicting duplicates')
            continue
        seen[ident] = event
        when = float(event['sim_time_h'])
        if end_h is not None and when > end_h:
            continue
        batch = event.get('batch_id')
        if not batch:
            continue
        state = batches.setdefault(batch, {'admitted': False, 'completed': False, 'loss': None, 'removed': False})
        name = event['event']
        measured = when >= start_h
        if name == 'ADMITTED' and not state['admitted']:
            state['admitted'] = True
            if measured:
                counts['started'] += 1
        elif name == 'COMPLETED':
            if state['loss']:
                raise ValueError(f'Lost batch {batch} later completed')
            if state['completed']:
                raise ValueError(f'Batch {batch} completed twice with different event IDs')
            state['completed'] = True
            if measured:
                counts['completed'] += 1
        elif name in ['LOSS_RECORDED', 'SPOILED', 'FLUSH_REQUESTED', 'FLUSHED']:
            if state['completed']:
                raise ValueError(f'Completed batch {batch} later discarded')
            if state['loss'] is None and (name == 'LOSS_RECORDED' or batch not in canonical_losses):
                category = 'spoiled' if name == 'SPOILED' or event.get('reason') in ['spoilage', 'hold_expired', 'pause_expired', 'hold_timeout', 'interruption_limit'] else 'operator_discarded'
                state['loss'] = category
                if measured:
                    counts[category] += 1
            if name == 'FLUSHED':
                state['removed'] = True
    last = end_h if end_h is not None else max((float(e['sim_time_h']) for e in seen.values()), default=start_h)
    duration = max(0, last - start_h)
    counts.update(hl_out=counts['completed'] * 50, hl_wasted=(counts['spoiled'] + counts['operator_discarded']) * 50,
                  completed_per_day=counts['completed'] * 24 / duration if duration else None,
                  horizon_h=duration,
                  viable_wip=sum(s['admitted'] and not s['completed'] and not s['loss'] for s in batches.values()),
                  loss_awaiting_flush=sum(bool(s['loss']) and not s['removed'] for s in batches.values()),
                  unique_events=len(seen))
    return counts


def load_events(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def machine_intervals(samples, start_h, end_h):
    """Integrate sampled states; record the sampling method rather than exact event timings."""
    histories = {}
    for sample in samples:
        for machine in sample['machines']:
            ident = machine['machine_id']
            state = ('OFFLINE' if not machine.get('online') else 'STOPPED' if machine.get('stopped') else machine['state'])
            histories.setdefault(ident, []).append((float(sample['sim_time_h']), state, machine.get('type', 'unknown')))
    result = []
    for ident, history in histories.items():
        history.sort()
        for index, (when, state, kind) in enumerate(history):
            until = history[index + 1][0] if index + 1 < len(history) else end_h
            left, right = max(when, start_h), min(until, end_h)
            if right > left:
                result.append({'machine_id': ident, 'type': kind, 'state': state, 'start_h': left, 'end_h': right,
                               'duration_h': right - left, 'method': 'sample_and_hold'})
    return result


def paired_comparison(runs):
    """Pair genuine declared seeds; deterministic repeats do not get confidence intervals."""
    pairs = {}
    for manifest, summary in runs:
        seed = manifest.get('seed')
        if seed is None:
            continue
        key = (manifest['scenario']['name'], manifest['tanks'], seed)
        if manifest['policy'] in pairs.setdefault(key, {}):
            raise ValueError(f'Duplicate treatment for {key}')
        pairs[key][manifest['policy']] = summary
    differences = []
    for key, pair in pairs.items():
        if set(pair) == {'PUSH', 'CONWIP'}:
            differences.append({'scenario': key[0], 'tanks': key[1], 'seed': key[2],
                                'waste_difference_hl': pair['CONWIP']['hl_wasted'] - pair['PUSH']['hl_wasted'],
                                'throughput_difference': pair['CONWIP']['completed_per_day'] - pair['PUSH']['completed_per_day']})
    return differences


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run', type=Path)
    args = parser.parse_args()
    manifest = json.loads((args.run / 'manifest.json').read_text())
    summary = summarise(load_events(args.run / 'events.jsonl'), manifest.get('measurement_start_h', 0), manifest.get('measurement_end_h'))
    summary.update(run_id=manifest['run_id'], policy=manifest['policy'], tanks=manifest['tanks'], outcome=manifest.get('outcome', 'unknown'))
    with (args.run / 'summary.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(summary))
        writer.writeheader()
        writer.writerow(summary)
    sample_path = args.run / 'machine-samples.jsonl'
    if sample_path.exists() and manifest.get('measurement_end_h') is not None:
        intervals = machine_intervals(load_events(sample_path), manifest.get('measurement_start_h', 0), manifest['measurement_end_h'])
        fields = ['machine_id', 'type', 'state', 'start_h', 'end_h', 'duration_h', 'method']
        with (args.run / 'machine_intervals.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=fields); writer.writeheader(); writer.writerows(intervals)
        totals = {}
        for interval in intervals:
            totals.setdefault(interval['machine_id'], {}).setdefault(interval['state'], 0)
            totals[interval['machine_id']][interval['state']] += interval['duration_h']
        utilisation = {ident: {'observed_h': sum(states.values()), 'state_fractions': {s: hours / sum(states.values()) for s, hours in states.items()}} for ident, states in totals.items()}
        (args.run / 'utilisation.json').write_text(json.dumps(utilisation, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
