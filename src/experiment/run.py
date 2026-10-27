#!/usr/bin/env python3
"""Execute a declared scenario against an isolated running factory and save raw data."""
import argparse
import csv
import json
import random
import copy
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'src/tools'))
from http_client import Api
from analyse import summarise


def write(path, data):
    path.write_text(json.dumps(data, indent=2, allow_nan=False) + '\n')


def export_events(api, run_id):
    events, offset = [], 0
    while True:
        page = api.call('/events?' + urlencode({'run_id': run_id, 'offset': offset, 'limit': 1000}))
        if not isinstance(page, list):
            raise ValueError('Event export must be an array')
        events.extend(page)
        if len(page) < 1000:
            return events
        offset += len(page)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://localhost:8088')
    parser.add_argument('--scenario', type=Path, default=ROOT / 'src/experiment/scenarios/steady.json')
    parser.add_argument('--policy', choices=['PUSH', 'CONWIP'], default='CONWIP')
    parser.add_argument('--tanks', type=int, choices=[1, 20, 40], default=20)
    parser.add_argument('--wip-limit', type=int)
    parser.add_argument('--seed', type=int, help='Seeded arrival jitter; same seed gives the same input schedule across policies')
    parser.add_argument('--run-id', default='run-' + str(uuid.uuid4()))
    parser.add_argument('--output', type=Path, default=ROOT / 'src/experiment/results')
    parser.add_argument('--timeout', type=float, default=18000, help='Maximum real seconds, including warm-up')
    parser.add_argument('--pilot', action='store_true', help='Use a 48-hour smoke horizon; requires smoke recipe profile')
    parser.add_argument('--selftest', action='store_true')
    args = parser.parse_args()
    if args.selftest:
        import unittest
        result = unittest.TextTestRunner().run(unittest.defaultTestLoader.discover(str(ROOT / 'src/experiment'), pattern='test*.py'))
        return 0 if result.wasSuccessful() else 1
    scenario = json.loads(args.scenario.read_text())
    api = Api(args.base_url)
    machines = api.call('/machines')
    actual_tanks = sum(m['type'] == 'unitank' and m.get('online') for m in machines)
    if actual_tanks != args.tanks:
        raise ValueError(f'Expected {args.tanks} online tanks, got {actual_tanks}; use a separate factory profile')
    line = api.call('/line')
    if line.get('active_wip', 0):
        raise ValueError('Run requires an empty isolated factory; existing batches will not be deleted')
    if line.get('stopped') or any(m.get('local_stop') or m.get('global_stop') for m in machines):
        raise ValueError('Factory has a STOP latch; explicitly reset it before starting an experiment')
    required_types = {'mash', 'lauter', 'kettle', 'whirlpool', 'unitank', 'filter'}
    if {m['type'] for m in machines if m.get('online')} != required_types:
        raise ValueError('All six stages must be online before starting an experiment')
    if any(o.get('status') not in ('FINISHED', 'CANCELLED') and o.get('started', 0) < o.get('quantity', 0) for o in api.call('/orders')):
        raise ValueError('Run requires no outstanding orders from another run')
    out = args.output / args.run_id
    out.mkdir(parents=True, exist_ok=False)
    warmup, measured = (0, 48) if args.pilot else (scenario['warmup_h'], scenario['measurement_h'])
    horizon = warmup + measured
    interval = scenario['order_interval_h']
    schedule = [{'at_h': h, 'recipe_id': 'lager' if scenario.get('switch_at_h') is not None and h >= scenario['switch_at_h'] else 'ale'}
                for h in range(0, int(horizon), interval)]
    if args.seed is not None:
        rng = random.Random(args.seed)
        for item in schedule:
            item['at_h'] = max(0, item['at_h'] + rng.uniform(-interval * .15, interval * .15))
        schedule.sort(key=lambda item: item['at_h'])
    actions = [] if args.pilot else sorted(scenario.get('actions', []), key=lambda item: item['at_h'])
    write(out / 'schedule.json', schedule)
    write(out / 'recipes.json', api.call('/recipes'))
    write(out / 'machines.json', machines)
    git = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True, capture_output=True)
    manifest = {'run_id': args.run_id, 'policy': args.policy, 'tanks': args.tanks, 'wip_limit': args.wip_limit or args.tanks,
                'git_sha': git.stdout.strip(), 'scenario': scenario, 'pilot': args.pilot, 'base_url': args.base_url,
                'seed': args.seed, 'started_utc': datetime.now(timezone.utc).isoformat(), 'outcome': 'running', 'actions': []}
    write(out / 'manifest.json', manifest)
    started = False
    failed = None
    try:
        initial = api.call('/experiment/start', {'run_id': args.run_id, 'policy': args.policy, 'wip_limit': manifest['wip_limit'], 'horizon_h': horizon}, args.run_id + '-start')
        started = True
        origin = initial['start_sim_h']
        manifest.update(measurement_start_h=origin + warmup, measurement_end_h=origin + horizon)
        write(out / 'manifest.json', manifest)
        next_order, next_action, real_start = 0, 0, time.monotonic()
        order_ids = []
        while time.monotonic() - real_start < args.timeout:
            status = api.call('/experiment/status')
            elapsed = status['sim_clock_h'] - origin
            while next_order < len(schedule) and schedule[next_order]['at_h'] <= elapsed and elapsed < horizon:
                item = schedule[next_order]
                created = api.call('/orders', {'recipe_id': item['recipe_id'], 'quantity': 1, 'priority': 0}, f'{args.run_id}-order-{next_order}')
                order_ids.append(created['id'])
                next_order += 1
            while next_action < len(actions) and actions[next_action]['at_h'] <= elapsed:
                action = actions[next_action]
                key = f'{args.run_id}-action-{next_action}'
                record = {'action': action, 'actual_h': elapsed}
                if action['kind'] == 'stop':
                    record['response'] = api.call('/line/stop', {}, key)
                elif action['kind'] == 'resume':
                    record['response'] = api.call('/line/resume', {}, key)
                elif action['kind'] == 'order':
                    record['response'] = api.call('/orders', {'recipe_id': action['recipe_id'], 'quantity': action.get('quantity', 1), 'priority': action.get('priority', 100)}, key)
                elif action['kind'] in ('flush_order', 'flush_active'):
                    candidates = [b for b in api.call('/batches') if b.get('run_id') == args.run_id
                                  and b['status'] not in ('RESERVING', 'COMPLETED', 'DISCARDED', 'FLUSH_PENDING')
                                  and not b.get('loss_reason')]
                    if action['kind'] == 'flush_order':
                        index = action['order_index']
                        matches = [b for b in candidates if index < len(order_ids) and b.get('order_id') == order_ids[index]]
                    else:
                        # The same intervention rule under both policies, with actual selected IDs saved.
                        matches = sorted((b for b in candidates if b.get('stage') == action.get('stage', 4)),
                                         key=lambda b: (b['created_at'], b['id']))[:1]
                    record['targets'] = [b['id'] for b in matches]
                    if not matches:
                        record['injection_applied'] = False
                        manifest['actions'].append(record)
                        raise ValueError(f'Flush action {next_action} has no admitted active target; this scenario was not fully applied')
                    record['injection_applied'] = True
                    for batch in matches:
                        api.call('/batches/' + batch['id'] + '/flush', {}, key + '-' + batch['id'])
                elif action['kind'] == 'publish_recipe':
                    recipe = copy.deepcopy(json.loads((ROOT / 'src/config/recipes.json').read_text())['recipes'][0])
                    recipe.update(id=action['recipe_id'], name=action['name'], version=1)
                    recipe['stages'][4]['phases'][1]['duration_h'] *= action.get('maturation_multiplier', 1)
                    record['response'] = api.call('/recipes', recipe, key)
                elif action['kind'] == 'revise_recipe':
                    recipe = copy.deepcopy(json.loads((ROOT / 'src/config/recipes.json').read_text())['recipes'][0])
                    recipe.update(id=action['recipe_id'], version=2)
                    recipe['stages'][4]['phases'][1]['duration_h'] *= action.get('maturation_multiplier', 1)
                    record['response'] = api.call('/recipes/' + action['recipe_id'] + '/versions', recipe, key)
                else:
                    raise ValueError('Unknown scenario action: ' + action['kind'])
                manifest['actions'].append(record)
                next_action += 1
                write(out / 'manifest.json', manifest)
            with (out / 'machine-samples.jsonl').open('a') as stream:
                stream.write(json.dumps({'sim_time_h': status['sim_clock_h'], 'machines': api.call('/machines')}) + '\n')
            if status.get('done') and elapsed < horizon:
                raise ValueError('Experiment stopped before its declared measurement horizon')
            if elapsed >= horizon:
                manifest['outcome'] = 'completed'
                break
            time.sleep(.25)
        else:
            raise TimeoutError('Real-time experiment deadline exceeded')
    except Exception as error:
        failed = error
        manifest.update(outcome='failed', error=str(error))
    finally:
        if started:
            try:
                api.call('/experiment/stop', {}, args.run_id + '-stop')
                # Stop movement; ageing continues. Append-sequence ordering keeps pagination stable.
                api.call('/line/stop', {}, args.run_id + '-line-stop')
                deadline = time.monotonic() + 30
                while api.call('/line').get('machines_pending_stop'):
                    if time.monotonic() >= deadline:
                        raise TimeoutError('Unconfirmed machine STOP; event export cannot be treated as complete')
                    time.sleep(.25)
                events = export_events(api, args.run_id)
                (out / 'events.jsonl').write_text(''.join(json.dumps(e, allow_nan=False) + '\n' for e in events))
                fields = ['event_id', 'event', 'batch_id', 'machine_id', 'run_id', 'sim_time_h', 'ts_utc', 'reason']
                with (out / 'events.csv').open('w', newline='') as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
                    writer.writeheader(); writer.writerows(events)
                write(out / 'orders.json', api.call('/orders'))
                write(out / 'final-status.json', api.call('/experiment/status'))
                write(out / 'summary.json', summarise(events, manifest.get('measurement_start_h', 0), manifest.get('measurement_end_h')))
            except Exception as error:
                manifest.update(outcome='failed', export_error=str(error))
                failed = failed or error
        manifest['finished_utc'] = datetime.now(timezone.utc).isoformat()
        write(out / 'manifest.json', manifest)
    print(f'{manifest["outcome"]}: {out}')
    if failed:
        print(str(failed), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
