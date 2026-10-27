import importlib.util
import json
import sys
import unittest
from pathlib import Path
from generate_deployment import compose, kube, recipes, machine_ids


class DeploymentTests(unittest.TestCase):
    def test_profiles_have_independent_identities_and_storage(self):
        for count in [1, 20, 40]:
            deployment = compose(count)
            ids = machine_ids(count)
            self.assertEqual(len(ids), count + 5)
            volumes = []
            for ident in ids:
                service = deployment['services'][ident]
                self.assertEqual(service['environment']['MACHINE_ID'], ident)
                volumes.append(service['volumes'][0])
            self.assertEqual(len(volumes), len(set(volumes)))

    def test_six_stateful_machine_types_and_joint_tank(self):
        objects = kube(40)
        machines = [o for o in objects if o['kind'] == 'StatefulSet' and o['metadata']['name'] in ['mash', 'lauter', 'kettle', 'whirlpool', 'unitank', 'filter']]
        self.assertEqual(len(machines), 6)
        self.assertEqual(sum(m['spec']['replicas'] for m in machines), 45)
        self.assertTrue(all(m['spec']['updateStrategy']['type'] == 'OnDelete' for m in machines))
        for recipe in recipes()['recipes']:
            self.assertEqual(recipe['volume_hl'], 50)
            tank = next(s for s in recipe['stages'] if s['type'] == 'unitank')
            self.assertEqual([p['name'] for p in tank['phases']], ['fermentation', 'maturation'])
            self.assertEqual(sum(p['duration_h'] for p in tank['phases']), 336 if recipe['id'] == 'ale' else 840)

    def test_formal_transformation_rejects_unknown_semantics(self):
        path = Path(__file__).resolve().parents[1] / 'models/generate.py'
        spec = importlib.util.spec_from_file_location('model_generator', path)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        model = json.loads((path.parent / 'behaviour.json').read_text())
        model['templates'][0]['transitions'][0]['unmodelled_side_effect'] = True
        with self.assertRaises(ValueError):
            module.transform(model)


if __name__ == '__main__':
    unittest.main()
