import unittest
from analyse import summarise, machine_intervals, paired_comparison


def event(ident, name, hour=1, batch='batch-1', **extra):
    return dict(event_id=ident, event=name, batch_id=batch, sim_time_h=hour, ts_utc=f'2026-01-01T00:00:{hour:02}Z', **extra)


class AnalysisTests(unittest.TestCase):
    def test_loss_then_flush_is_counted_once(self):
        events = [event('a', 'ADMITTED'), event('b', 'SPOILED', 2), event('c', 'FLUSHED', 3)]
        result = summarise(events + events)
        self.assertEqual(result['hl_wasted'], 50)
        self.assertEqual(result['spoiled'], 1)
        self.assertEqual(result['loss_awaiting_flush'], 0)

    def test_operator_loss_and_spoil_are_separate(self):
        result = summarise([event('a', 'FLUSH_REQUESTED', reason='operator'), event('b', 'FLUSHED', 2)])
        self.assertEqual(result['operator_discarded'], 1)
        self.assertEqual(result['spoiled'], 0)

    def test_invalid_terminal_history_is_rejected(self):
        for events in [[event('a', 'SPOILED'), event('b', 'COMPLETED', 2)],
                       [event('a', 'COMPLETED'), event('b', 'COMPLETED', 2)],
                       [event('a', 'COMPLETED'), event('b', 'FLUSHED', 2)]]:
            with self.assertRaises(ValueError):
                summarise(events)

    def test_window_accounts_for_existing_wip(self):
        result = summarise([event('a', 'ADMITTED', 1), event('b', 'COMPLETED', 25)], 24, 48)
        self.assertEqual(result['started'], 0)
        self.assertEqual(result['completed_per_day'], 1)
        self.assertEqual(result['hl_out'], 50)

    def test_same_event_id_with_conflicting_body_is_rejected(self):
        with self.assertRaises(ValueError):
            summarise([event('a', 'SPOILED'), event('a', 'COMPLETED')])

    def test_canonical_loss_overrides_racing_raw_reasons(self):
        result = summarise([event('a', 'FLUSH_REQUESTED', reason='operator'),
                            event('b', 'LOSS_RECORDED', 2, reason='spoilage'),
                            event('c', 'FLUSHED', 3)])
        self.assertEqual(result['spoiled'], 1)
        self.assertEqual(result['operator_discarded'], 0)
        self.assertEqual(result['loss_awaiting_flush'], 0)

    def test_machine_intervals_clip_window_and_distinguish_stop(self):
        def sample(hour, **changes):
            return {'sim_time_h': hour, 'machines': [dict(machine_id='unitank-0', type='unitank',
                    state='PROCESSING', online=True, **changes)]}
        intervals = machine_intervals([sample(1, stopped=False), sample(4, stopped=True), sample(6, stopped=False)], 2, 8)
        self.assertEqual([x['duration_h'] for x in intervals], [2, 2, 2])
        self.assertEqual([x['state'] for x in intervals], ['PROCESSING', 'STOPPED', 'PROCESSING'])

    def test_only_matching_seeded_treatments_are_paired(self):
        def run(policy, seed, tanks, waste):
            return ({'scenario': {'name': 'steady'}, 'tanks': tanks, 'seed': seed, 'policy': policy},
                    {'hl_wasted': waste, 'completed_per_day': 1})
        pairs = paired_comparison([run('PUSH', 1, 20, 100), run('CONWIP', 1, 20, 50),
                                   run('CONWIP', 1, 40, 0), run('PUSH', None, 20, 0)])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]['waste_difference_hl'], -50)
        with self.assertRaises(ValueError):
            paired_comparison([run('PUSH', 1, 20, 100), run('PUSH', 1, 20, 100)])


if __name__ == '__main__':
    unittest.main()
