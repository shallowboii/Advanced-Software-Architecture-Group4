#!/usr/bin/env python3
"""Generate matching Compose and Kubernetes factories with isolated machine storage."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TYPES = ('mash', 'lauter', 'kettle', 'whirlpool', 'unitank', 'filter')
IMAGES = {'postgres': 'postgres:17.6-bookworm', 'redis': 'redis:7.4.5-alpine',
          'rabbitmq': 'rabbitmq:4.1.4-management', 'kafka': 'apache/kafka:3.9.1'}
ENV = {'DATABASE_URL': 'jdbc:postgresql://postgres:5432/beerfactory',
       'DATABASE_USER': 'beerfactory', 'DATABASE_PASSWORD': 'development-only',
       'REDIS_HOST': 'redis', 'FACTORY_REDIS_PORT': '6379', 'MQTT_URL': 'tcp://rabbitmq:1883',
       'MQTT_USERNAME': 'beerfactory', 'MQTT_PASSWORD': 'development-only',
       'KAFKA_BROKERS': 'kafka:9092', 'INVENTORY_URL': 'http://inventory:8081',
       'TIME_SCALE': '2', 'POLICY': 'CONWIP', 'RUN_ID': 'interactive',
       'RECIPE_FILE': '/app/config/recipes.json'}


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n')


def recipes(smoke=False):
    result = []
    for name, tank in [('ale', (168, 168)), ('lager', (336, 504))]:
        stages = []
        for kind, duration, temperature in zip(TYPES, [2, 2, 1, 1, 0, 2], [65, 76, 100, 20, 20, 3]):
            phases = [{'name': kind, 'duration_h': duration, 'target_temp_c': temperature}]
            if kind == 'unitank':
                phases = [{'name': 'fermentation', 'duration_h': tank[0], 'target_temp_c': 20 if name == 'ale' else 12},
                          {'name': 'maturation', 'duration_h': tank[1], 'target_temp_c': 2}]
            if smoke:
                phases = [dict(p, duration_h=1) for p in phases]
            stages.append({'type': kind, 'phases': phases, 'hold_h': 168 if kind == 'unitank' else (None if kind == 'filter' else (10 if smoke else 1)),
                           'max_pause_h': 168 if kind == 'unitank' else (10 if smoke else 1)})
        result.append({'id': name, 'name': name.title(), 'version': 1, 'volume_hl': 50,
                       'ingredients': {'malt_kg': 900, 'hops_kg': 20, 'yeast_kg': 10, 'water_l': 6000},
                       'stages': stages, 'transfer_h': .1, 'flush_h': .1})
    return {'recipes': result}


def machine_ids(tanks):
    return [f'{kind}-{i}' for kind in TYPES for i in range(tanks if kind == 'unitank' else 1)]


def compose(tanks, smoke=False):
    env = dict(ENV, WIP_LIMIT=str(tanks))
    recipe = 'recipes-smoke.json' if smoke else 'recipes.json'
    services = {
        'postgres': {'image': IMAGES['postgres'], 'environment': {'POSTGRES_DB': 'beerfactory', 'POSTGRES_USER': env['DATABASE_USER'], 'POSTGRES_PASSWORD': env['DATABASE_PASSWORD']},
                     'volumes': ['postgres-data:/var/lib/postgresql/data'], 'healthcheck': {'test': ['CMD-SHELL', 'pg_isready -U beerfactory -d beerfactory'], 'interval': '3s', 'timeout': '3s', 'retries': 30}},
        'redis': {'image': IMAGES['redis'], 'healthcheck': {'test': ['CMD', 'redis-cli', 'ping'], 'interval': '3s', 'timeout': '3s', 'retries': 30}},
        'rabbitmq': {'image': IMAGES['rabbitmq'], 'environment': {'RABBITMQ_DEFAULT_USER': env['MQTT_USERNAME'], 'RABBITMQ_DEFAULT_PASS': env['MQTT_PASSWORD']},
                     'volumes': ['rabbitmq-data:/var/lib/rabbitmq', './src/deploy/rabbitmq/enabled_plugins:/etc/rabbitmq/enabled_plugins:ro'],
                     'healthcheck': {'test': ['CMD', 'rabbitmq-diagnostics', '-q', 'ping'], 'interval': '5s', 'timeout': '5s', 'retries': 30}},
        'kafka': {'image': IMAGES['kafka'], 'environment': {
            'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller', 'KAFKA_LISTENERS': 'PLAINTEXT://:9092,CONTROLLER://:9093',
            'KAFKA_ADVERTISED_LISTENERS': 'PLAINTEXT://kafka:9092', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
            'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:9093', 'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
            'KAFKA_INTER_BROKER_LISTENER_NAME': 'PLAINTEXT', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
            'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
            'KAFKA_LOG_DIRS': '/var/lib/kafka/data', 'KAFKA_LOG_RETENTION_HOURS': '168', 'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'true',
            'KAFKA_HEAP_OPTS': '-Xms256m -Xmx512m',
            'CLUSTER_ID': 'MkU3OEVBNTcwNTJENDM2Qk'}, 'volumes': ['kafka-data:/var/lib/kafka/data']},
        'inventory': {'build': {'context': '.', 'dockerfile': 'src/inventory/Dockerfile'}, 'image': 'beerfactory-inventory:local',
                      'environment': {'PORT': '8081', 'INVENTORY_STATE_FILE': '/data/inventory.json', 'INITIAL_BATCHES': '1000'}, 'volumes': ['inventory-data:/data']},
        'backend': {'build': {'context': '.', 'dockerfile': 'src/backend/Dockerfile'}, 'image': 'beerfactory-backend:local', 'environment': env,
                    'volumes': [f'./src/config/{recipe}:/app/config/recipes.json:ro'],
                    'depends_on': {'postgres': {'condition': 'service_healthy'}, 'rabbitmq': {'condition': 'service_healthy'}, 'redis': {'condition': 'service_healthy'}, 'kafka': {'condition': 'service_started'}, 'inventory': {'condition': 'service_started'}}},
        'frontend': {'build': {'context': '.', 'dockerfile': 'src/frontend/Dockerfile'}, 'image': 'beerfactory-frontend:local', 'ports': ['${FACTORY_PORT:-8088}:80'], 'depends_on': ['backend']},
    }
    volumes = {f'{name}-data': {} for name in ['postgres', 'rabbitmq', 'kafka', 'inventory']}
    for ident in machine_ids(tanks):
        kind = ident.rsplit('-', 1)[0]
        services[ident] = {'build': {'context': '.', 'dockerfile': 'src/machine/Dockerfile'}, 'image': 'beerfactory-machine:local',
                           'hostname': ident, 'environment': {k: env[k] for k in ['MQTT_URL', 'MQTT_USERNAME', 'MQTT_PASSWORD', 'KAFKA_BROKERS', 'TIME_SCALE']},
                           'volumes': [f'{ident}-data:/data'], 'depends_on': {'rabbitmq': {'condition': 'service_healthy'}, 'kafka': {'condition': 'service_started'}}}
        services[ident]['environment'].update(MACHINE_TYPE=kind, MACHINE_ID=ident, STATE_DB='/data/machine.db', SOCKET_PATH='/tmp/machine.sock', FLUSH_H='0.1')
        volumes[f'{ident}-data'] = {}
    for service in services.values():
        service['restart'] = 'unless-stopped'
    return {'services': services, 'volumes': volumes}


def kube(tanks):
    objects = [{'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': 'beerfactory'}}]

    def add(kind, name, **props):
        obj = {'apiVersion': 'apps/v1' if kind in ('Deployment', 'StatefulSet') else 'v1', 'kind': kind, 'metadata': {'name': name}, **props}
        objects.append(obj)
        return obj

    add('ConfigMap', 'factory-recipes', data={'recipes.json': json.dumps(recipes())})
    add('ConfigMap', 'rabbitmq-plugins', data={'enabled_plugins': '[rabbitmq_management,rabbitmq_mqtt].\n'})
    base = compose(tanks)['services']
    for name in ['postgres', 'redis', 'rabbitmq', 'kafka', 'inventory', 'backend', 'frontend']:
        service = base[name]
        port = {'postgres': 5432, 'redis': 6379, 'rabbitmq': 1883, 'kafka': 9092, 'inventory': 8081, 'backend': 8080, 'frontend': 80}[name]
        env = [{'name': k, 'value': v} for k, v in service.get('environment', {}).items()]
        if name == 'postgres':
            env.append({'name': 'PGDATA', 'value': '/var/lib/postgresql/data/pgdata'})
        if name == 'kafka':
            env = [{'name': e['name'], 'value': '1@kafka-0.kafka:9093' if e['name'] == 'KAFKA_CONTROLLER_QUORUM_VOTERS' else e['value']} for e in env]
        container = {'name': name, 'image': service['image'].replace(':local', ':dev'), 'ports': [{'containerPort': port}], 'env': env,
                     'resources': {'requests': {'cpu': '100m', 'memory': '128Mi'}, 'limits': {'memory': '1Gi'}}}
        container['readinessProbe'] = {'tcpSocket': {'port': port}, 'initialDelaySeconds': 5, 'periodSeconds': 5}
        if name == 'backend':
            container['readinessProbe'] = {'httpGet': {'path': '/api/health', 'port': port}, 'initialDelaySeconds': 5, 'periodSeconds': 2}
        volumes, mounts = [], []
        if name == 'backend':
            volumes.append({'name': 'recipes', 'configMap': {'name': 'factory-recipes'}})
            mounts.append({'name': 'recipes', 'mountPath': '/app/config', 'readOnly': True})
        if name == 'rabbitmq':
            volumes.append({'name': 'plugins', 'configMap': {'name': 'rabbitmq-plugins'}})
            mounts.append({'name': 'plugins', 'mountPath': '/etc/rabbitmq/enabled_plugins', 'subPath': 'enabled_plugins', 'readOnly': True})
        persistent = name in ['postgres', 'rabbitmq', 'kafka', 'inventory']
        if persistent:
            path = {'postgres': '/var/lib/postgresql/data', 'rabbitmq': '/var/lib/rabbitmq', 'kafka': '/var/lib/kafka/data', 'inventory': '/data'}[name]
            mounts.append({'name': 'data', 'mountPath': path})
        if mounts:
            container['volumeMounts'] = mounts
        template = {'metadata': {'labels': {'app': name}}, 'spec': {'containers': [container], 'volumes': volumes, 'terminationGracePeriodSeconds': 30}}
        if name == 'backend':
            # Service-link variables such as REDIS_PORT are URLs, not port numbers.
            template['spec']['enableServiceLinks'] = False
        if name in ['inventory', 'kafka', 'rabbitmq']:
            template['spec']['securityContext'] = {'fsGroup': {'inventory': 10001, 'kafka': 1000, 'rabbitmq': 999}[name]}
        if name == 'kafka':
            container['resources']['limits']['memory'] = '2Gi'
        spec = {'replicas': 2 if name in ['backend', 'frontend'] else 1, 'selector': {'matchLabels': {'app': name}}, 'template': template}
        if persistent:
            spec.update(serviceName=name, volumeClaimTemplates=[{'metadata': {'name': 'data'}, 'spec': {'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '2Gi'}}}}])
        else:
            spec['strategy'] = {'type': 'RollingUpdate', 'rollingUpdate': {'maxUnavailable': 0, 'maxSurge': 1}}
        add('StatefulSet' if persistent else 'Deployment', name, spec=spec)
        svc_spec = {'selector': {'app': name}, 'ports': [{'name': 'main', 'port': port, 'targetPort': port}]}
        if persistent:
            svc_spec['clusterIP'] = 'None'
            svc_spec['publishNotReadyAddresses'] = True
        if name == 'kafka':
            svc_spec['ports'].append({'name': 'controller', 'port': 9093, 'targetPort': 9093})
        add('Service', name, spec=svc_spec)
    for kind in TYPES:
        container = {'name': 'machine', 'image': 'beerfactory-machine:dev', 'env': [
            {'name': 'MACHINE_ID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}, {'name': 'MACHINE_TYPE', 'value': kind},
            *[{'name': k, 'value': ENV[k]} for k in ['MQTT_URL', 'MQTT_USERNAME', 'MQTT_PASSWORD', 'KAFKA_BROKERS', 'TIME_SCALE']],
            {'name': 'STATE_DB', 'value': '/data/machine.db'}, {'name': 'SOCKET_PATH', 'value': '/tmp/machine.sock'}, {'name': 'FLUSH_H', 'value': '0.1'}],
            'volumeMounts': [{'name': 'data', 'mountPath': '/data'}],
            'resources': {'requests': {'cpu': '10m', 'memory': '24Mi'}, 'limits': {'memory': '128Mi'}}}
        add('StatefulSet', kind, spec={'serviceName': kind, 'replicas': tanks if kind == 'unitank' else 1,
            'podManagementPolicy': 'Parallel', 'updateStrategy': {'type': 'OnDelete'},
            'selector': {'matchLabels': {'app': 'machine', 'machine-type': kind}},
            'template': {'metadata': {'labels': {'app': 'machine', 'machine-type': kind}},
                         'spec': {'terminationGracePeriodSeconds': 30, 'securityContext': {'fsGroup': 10001}, 'containers': [container]}},
            'volumeClaimTemplates': [{'metadata': {'name': 'data'}, 'spec': {'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '128Mi'}}}}]})
        add('Service', kind, spec={'clusterIP': 'None', 'selector': {'app': 'machine', 'machine-type': kind}, 'ports': [{'port': 1883, 'name': 'identity'}]})
    return objects


def generate(root=ROOT):
    write(root / 'src/config/recipes.json', recipes())
    write(root / 'src/config/recipes-smoke.json', recipes(True))
    for count, name in [(1, 'smoke'), (20, '20'), (40, '40')]:
        write(root / f'src/deploy/compose/{name}.json', compose(count, count == 1))
    # Run with --project-directory ROOT: all build and bind paths are rooted there.
    write(root / 'compose.yaml', compose(20))
    base_dir = root / 'src/deploy/k8s/base'
    resources = []
    for obj in kube(20):
        filename = f"{obj['kind'].lower()}-{obj['metadata']['name']}.json"
        write(base_dir / filename, obj)
        resources.append(filename)
    write(base_dir / 'kustomization.yaml', {'apiVersion': 'kustomize.config.k8s.io/v1beta1', 'kind': 'Kustomization', 'namespace': 'beerfactory', 'resources': resources})
    for count, name in [(1, 'smoke'), (20, '20'), (40, '40'), (20, 'mock-cluster')]:
        patches = [{'target': {'kind': 'StatefulSet', 'name': 'unitank'}, 'patch': f'- op: replace\n  path: /spec/replicas\n  value: {count}\n'},
                   {'patch': json.dumps({'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': 'backend'}, 'spec': {'template': {'spec': {'containers': [{'name': 'backend', 'env': [{'name': 'WIP_LIMIT', 'value': str(count)}]}]}}}})}]
        if name == 'smoke':
            patches.append({'patch': json.dumps({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'factory-recipes'}, 'data': {'recipes.json': json.dumps(recipes(True))}})})
        if name == 'mock-cluster':
            patches.append({'patch': json.dumps({'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'frontend'}, 'spec': {'type': 'NodePort', 'ports': [{'name': 'main', 'port': 80, 'targetPort': 80, 'nodePort': 30088}]}})})
        write(root / f'src/deploy/k8s/overlays/{name}/kustomization.yaml', {'apiVersion': 'kustomize.config.k8s.io/v1beta1', 'kind': 'Kustomization', 'resources': ['../../base'], 'patches': patches})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT)
    args = parser.parse_args()
    generate(args.output)
