#!/usr/bin/env python3
"""Exercise real container restarts and broker outages in an isolated smoke factory."""
import argparse
import copy
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from http_client import Api
from smoke import until

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://localhost:8088')
    parser.add_argument('--output', type=Path, default=Path('failure-result.json'))
    args = parser.parse_args()
    api = Api(args.base_url)
    compose = ['docker', 'compose', '--project-directory', str(ROOT), '-f', str(ROOT / 'src/deploy/compose/smoke.json')]
    def docker(*command):
        return subprocess.run(compose + list(command), check=True, capture_output=True, text=True, timeout=90).stdout
    def snapshot():
        return json.loads(docker('exec', '-T', 'mash-0', 'machinectl', 'snapshot'))
    def step(name, details):
        report['steps'].append({'name': name, 'ts_utc': datetime.now(timezone.utc).isoformat(), 'details': details})
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    line = api.call('/line')
    if line.get('active_wip') or line.get('stopped'):
        raise SystemExit('Failure checks require an empty, resumed, isolated smoke factory')
    if any(o['status'] not in ('FINISHED', 'CANCELLED') for o in api.call('/orders')):
        raise SystemExit('Cancel pending orders before running failure checks')
    report = {'passed': False, 'steps': []}
    key = 'failure-' + str(uuid.uuid4())
    batch_id = None
    try:
        recipe = copy.deepcopy(next(r for r in api.call('/recipes') if r['id'] == 'ale'))
        recipe.update(id=key, name='Recovery check', version=1)
        recipe.pop('recipe_version_id', None)
        # Keep one known execution active throughout the real-time outage checks.
        for stage in recipe['stages']:
            stage.update(max_pause_h=1000, hold_h=None if stage['type'] == 'filter' else 1000)
            for phase in stage['phases']:
                phase['duration_h'] = 1000
        api.call('/recipes', recipe, key + '-recipe')
        order = api.call('/orders', {'recipe_id': key, 'quantity': 1}, key + '-order')
        batch = until(lambda: next((b for b in api.call('/batches') if b['order_id'] == order['id'] and b['status'] == 'PROCESSING'), None), 90, 'recovery batch processing')
        batch_id = batch['id']
        original = snapshot()
        docker('exec', '-T', 'mash-0', 'machinectl', 'stop')
        stopped = snapshot()
        assert stopped['local_stop'] and stopped['batch_id'] == batch_id
        docker('restart', 'mash-0')
        restarted = until(lambda: (s if (s := snapshot())['controller_boot_id'] != original['controller_boot_id'] else None), 30, 'controller restart')
        assert restarted['local_stop'] and restarted['batch_id'] == batch_id
        assert restarted['execution_id'] == original['execution_id']
        assert abs(restarted['phase_elapsed_h'] - stopped['phase_elapsed_h']) < .1
        step('controller restart preserves batch, progress and local STOP', restarted)

        docker('stop', 'backend')
        docker('exec', '-T', 'mash-0', 'machinectl', 'resume')
        expired = until(lambda: (s if not (s := snapshot())['permit_valid'] else None), 10, 'permit expires without coordinator')
        time.sleep(1)
        paused = snapshot()
        assert paused['phase_elapsed_h'] == expired['phase_elapsed_h']
        assert paused['sim_time_h'] > expired['sim_time_h']
        docker('start', 'backend')
        until(lambda: api.call('/health').get('status') == 'ok', 90, 'backend recovery')
        until(lambda: not snapshot()['stopped'], 30, 'permit renewal after backend restart')
        step('coordinator outage inhibits progress while ageing continues', paused)

        docker('stop', 'rabbitmq')
        until(lambda: not snapshot()['permit_valid'], 10, 'permit expires without MQTT')
        assert snapshot()['batch_id'] == batch_id
        docker('start', 'rabbitmq')
        until(lambda: not snapshot()['stopped'], 90, 'MQTT reconnect and permit renewal')
        step('MQTT outage and reconnection preserve execution', snapshot())

        docker('stop', 'kafka')
        time.sleep(2)
        during = snapshot()
        assert not during['stopped'] and during['batch_id'] == batch_id
        docker('start', 'kafka')
        after = datetime.now(timezone.utc).isoformat()
        from urllib.parse import urlencode
        samples = until(lambda: api.call('/telemetry?' + urlencode({'batch_id': batch_id, 'from': after, 'limit': 20})), 90, 'new Kafka telemetry after recovery')
        step('Kafka outage does not change ownership; telemetry resumes', {'recovered_samples': len(samples)})
        assert len([b for b in api.call('/batches') if b['order_id'] == order['id']]) == 1
        api.call('/batches/' + batch_id + '/flush', {}, key + '-flush')
        until(lambda: any(b['id'] == batch_id and b['status'] == 'DISCARDED' for b in api.call('/batches')), 30, 'recovery batch removal')
        losses = [e for e in api.call('/events?batch_id=' + batch_id) if e['event'] == 'LOSS_RECORDED']
        assert len(losses) == 1 and losses[0]['reason'] == 'operator'
        step('one admission and one terminal loss despite restarts', {'batch_id': batch_id})
        report['passed'] = True
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        # Restore services stopped by this harness, preserving all state and evidence.
        try:
            docker('start', 'backend', 'rabbitmq', 'kafka')
            if not report['passed']:
                until(lambda: api.call('/health').get('status') == 'ok', 60, 'backend before failure STOP')
                api.call('/line/stop', {}, key + '-failure-stop')
        except Exception as error:
            report['cleanup_error'] = str(error)
            report['passed'] = False
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(f'Failure checks: {"PASS" if report["passed"] else "FAIL"}; {args.output}')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
