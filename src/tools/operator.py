#!/usr/bin/env python3
"""Invoke simulated local physical controls independently of the application brokers."""
import argparse
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['stop', 'resume', 'flush', 'snapshot', 'status'])
    parser.add_argument('--machine', required=True, help='Machine ID, or all for global stop')
    parser.add_argument('--batch-id', '--batch', dest='batch_id')
    parser.add_argument('--kubernetes', action='store_true')
    parser.add_argument('--namespace', default='beerfactory')
    parser.add_argument('--profile', choices=['smoke', '20', '40'], default='smoke')
    args = parser.parse_args()
    if args.machine == 'all' and args.action != 'stop':
        parser.error('Only stop can target all machines; resume and flush require explicit targets')
    if args.action == 'flush' and not args.batch_id:
        parser.error('Flush requires --batch-id so stale clicks cannot flush a later batch')
    compose = ['docker', 'compose', '--project-directory', str(ROOT), '-f', str(ROOT / f'src/deploy/compose/{args.profile}.json')]
    if args.machine == 'all':
        if args.kubernetes:
            result = subprocess.run(['kubectl', '-n', args.namespace, 'get', 'pods', '-l', 'app=machine', '-o', 'json'], check=True, capture_output=True, text=True)
            machines = [p['metadata']['name'] for p in json.loads(result.stdout)['items']]
        else:
            from generate_deployment import machine_ids
            machines = machine_ids({'smoke': 1, '20': 20, '40': 40}[args.profile])
    else:
        machines = [args.machine]
    if not machines:
        raise SystemExit('UNCONFIRMED: no machine Pods were discovered')

    def deliver(machine):
        prefix = ['kubectl', '-n', args.namespace, 'exec', machine, '--'] if args.kubernetes else compose + ['exec', '-T', machine]
        command = ['machinectl', args.action]
        if args.machine == 'all':
            command += ['--scope', 'global']
        if args.batch_id:
            command += ['--batch', args.batch_id]
        try:
            result = subprocess.run(prefix + command, check=False, capture_output=True, text=True, timeout=10)
            return machine, result.returncode == 0, (result.stdout + result.stderr).strip()
        except (subprocess.TimeoutExpired, OSError) as error:
            return machine, False, str(error)

    with ThreadPoolExecutor(max_workers=min(16, len(machines))) as pool:
        outcomes = list(pool.map(deliver, machines))
    failures = []
    for machine, confirmed, output in outcomes:
        if output:
            print(f'{machine}: {output}')
        if not confirmed:
            failures.append(machine)
    if failures:
        raise SystemExit('UNCONFIRMED local controls: ' + ', '.join(failures))
    print('Local input delivered to: ' + ', '.join(machines))


if __name__ == '__main__':
    main()
