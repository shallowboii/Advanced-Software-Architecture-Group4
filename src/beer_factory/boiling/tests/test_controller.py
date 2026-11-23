"""Run the real controller with deterministic clock and heartbeat doubles."""
import os
import subprocess
import sys
import unittest

BINARY = os.path.abspath(sys.argv.pop(1))


class ControllerTests(unittest.TestCase):
    def run_controller(self, *args, scenario="normal"):
        return subprocess.run(
            [BINARY, *args], capture_output=True, text=True, timeout=5,
            env={**os.environ, "CONTROLLER_SCENARIO": scenario,
                 "KAFKA_BROKERS": "test-broker:9092"},
        )

    def test_requires_one_argument(self):
        for args in [(), ("1", "2")]:
            with self.subTest(args=args):
                result = self.run_controller(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("<machine-number>", result.stderr)
                self.assertNotIn("INIT", result.stdout)

    def test_machine_number_reaches_heartbeat_service(self):
        for number in ["0", "7", "42"]:
            with self.subTest(number=number):
                result = self.run_controller(number)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"INIT {number}\n", result.stdout)

    def test_batch_deadline_and_heartbeats_while_waiting(self):
        result = self.run_controller("7")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout.count("Boiling started"), 1)
        self.assertEqual(result.stdout.count("Boiling complete; waiting."), 1)
        self.assertEqual(result.stdout.count("KAFKA 4 B-1\n"), 1)
        self.assertEqual(result.stdout.count("KAFKA 3 B-1\n"), 1)
        self.assertNotIn("B-busy", result.stdout)
        for tick in [0, 5, 3599, 3605]:
            self.assertIn(f"SEND {tick} boiling\n", result.stdout)
        self.assertEqual(result.stdout.count("SEND "), 4)
        self.assertNotIn("waiting.", result.stdout.split("SYNC 3599")[0])
        self.assertIn("waiting.", result.stdout.split("SEND 3605")[0])

    def test_heartbeat_failures_do_not_stop_boiling(self):
        for scenario in ["init-error", "send-error", "sync-error"]:
            with self.subTest(scenario=scenario):
                result = self.run_controller("7", scenario=scenario)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("Boiling complete; waiting.", result.stdout)
                self.assertNotIn("SEND 5 ", result.stdout)
                if scenario == "init-error":
                    self.assertNotIn("SEND", result.stdout)
                else:
                    self.assertEqual(result.stderr.count("Error while handling heartbeats"), 1)
                if scenario == "send-error":
                    self.assertNotIn("SYNC", result.stdout)

    def test_clock_failure_cleans_up_and_exits(self):
        result = self.run_controller("7", scenario="clock-error")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("clock_gettime", result.stderr)
        self.assertIn("DESTROY", result.stdout)
        self.assertIn("CONSUMER_CLOSE", result.stdout)
        self.assertIn("PRODUCER_CLOSE", result.stdout)
        self.assertNotIn("Boiling complete", result.stdout)

    def test_waits_for_start_for_this_machine(self):
        for scenario in ["no-start", "wrong-id", "status-only"]:
            with self.subTest(scenario=scenario):
                result = self.run_controller("7", scenario=scenario)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("Boiling started", result.stdout)
                self.assertNotIn("KAFKA ", result.stdout)
                self.assertIn("SEND 3605 boiling", result.stdout)


if __name__ == "__main__":
    unittest.main()
