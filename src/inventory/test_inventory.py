import concurrent.futures
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import patch

from server import ApiError, INGREDIENTS, Inventory, InventoryHTTPServer, PER_BATCH


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "inventory.json"
        self.inventory = Inventory(self.path, 2)

    def tearDown(self):
        self.inventory.close()
        self.directory.cleanup()

    def request(self, batch_id="batch-1", ingredients=None):
        return {"batch_id": batch_id, "ingredients": ingredients or PER_BATCH.copy()}

    def test_reservation_retry_survives_restart(self):
        first = self.inventory.reserve(self.request())
        self.inventory.close()
        self.inventory = Inventory(self.path, 999)
        self.assertEqual(first, self.inventory.reserve(self.request()))
        self.assertEqual(self.inventory.stock()["stock"], PER_BATCH)

    def test_concurrent_reservations_never_overspend(self):
        def reserve(index):
            try:
                return self.inventory.reserve(self.request(f"batch-{index}"))["status"]
            except ApiError as error:
                return error.code
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            results = list(executor.map(reserve, range(30)))
        self.assertEqual(results.count("reserved"), 2)
        self.assertEqual(results.count("insufficient_stock"), 28)
        self.assertEqual(self.inventory.stock()["stock"], dict.fromkeys(INGREDIENTS, 0))

    def test_duplicate_concurrent_requests_deduct_once(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            replies = list(executor.map(lambda _: self.inventory.reserve(self.request()), range(20)))
        self.assertTrue(all(reply == replies[0] for reply in replies))
        self.assertEqual(self.inventory.stock()["stock"], PER_BATCH)

    def test_release_is_durable_and_cannot_revive_batch(self):
        self.inventory.reserve(self.request())
        response = self.inventory.release("batch-1")
        self.inventory.close()
        self.inventory = Inventory(self.path, 0)
        self.assertEqual(response, self.inventory.release("batch-1"))
        self.assertEqual(self.inventory.reservation("batch-1")["status"], "released")
        with self.assertRaises(ApiError) as caught:
            self.inventory.reserve(self.request())
        self.assertEqual(caught.exception.code, "reservation_released")
        self.assertEqual(self.inventory.stock()["stock"], {key: value * 2 for key, value in PER_BATCH.items()})

    def test_conflicting_duplicate_does_not_change_stock(self):
        self.inventory.reserve(self.request())
        changed = {**PER_BATCH, "hops_kg": 2}
        with self.assertRaises(ApiError) as caught:
            self.inventory.reserve(self.request(ingredients=changed))
        self.assertEqual(caught.exception.status, 409)
        self.assertEqual(self.inventory.stock()["stock"], PER_BATCH)

    def test_restock_is_idempotent_and_conflict_checked(self):
        body = {"request_id": "delivery-1", "ingredients": PER_BATCH.copy()}
        first = self.inventory.restock(body)
        self.inventory.close()
        self.inventory = Inventory(self.path, 0)
        self.assertEqual(first, self.inventory.restock(body))
        self.assertEqual(self.inventory.stock()["stock"]["malt_kg"], 2700)
        body["ingredients"]["malt_kg"] = 1
        with self.assertRaises(ApiError) as caught:
            self.inventory.restock(body)
        self.assertEqual(caught.exception.code, "idempotency_conflict")

    def test_invalid_quantities_and_fields_never_mutate(self):
        before = self.inventory.stock()
        for value in [True, False, "1", None, float("nan"), float("inf"), -1, 1e20, 1e-10]:
            with self.subTest(value=value), self.assertRaises(ApiError):
                self.inventory.reserve(self.request(ingredients={**PER_BATCH, "malt_kg": value}))
        for payload in [{}, {**self.request(), "extra": 1}, {"batch_id": "x", "ingredients": {"water_l": 1}}]:
            with self.subTest(payload=payload), self.assertRaises(ApiError):
                self.inventory.reserve(payload)
        self.assertEqual(before, self.inventory.stock())

    def test_decimal_stock_does_not_accumulate_float_errors(self):
        zeroes = dict.fromkeys(INGREDIENTS, 0)
        for index in range(10):
            self.inventory.reserve(self.request(str(index), {**zeroes, "hops_kg": 0.1}))
        self.assertEqual(self.inventory.stock()["stock"]["hops_kg"], 39)

    def test_failed_atomic_replace_does_not_mutate_memory(self):
        before = self.inventory.stock()
        with patch("server.os.replace", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                self.inventory.reserve(self.request())
        self.assertEqual(self.inventory.stock(), before)
        self.assertFalse(list(self.path.parent.glob("*.tmp")))
        self.assertEqual(self.inventory.reserve(self.request())["status"], "reserved")

    def test_second_writer_is_rejected(self):
        with self.assertRaises(BlockingIOError):
            Inventory(self.path, 2)

    def test_corrupt_state_fails_without_reseeding(self):
        self.inventory.close()
        self.path.write_text('{"version":1,"stock":{}}')
        with self.assertRaises(ValueError):
            Inventory(self.path, 200)
        self.assertEqual(self.path.read_text(), '{"version":1,"stock":{}}')

    def test_unknown_reservation_returns_not_found(self):
        with self.assertRaises(ApiError) as caught:
            self.inventory.reservation("absent")
        self.assertEqual(caught.exception.status, 404)

    def test_release_before_reserve_persists_cancellation_fence(self):
        stock = self.inventory.stock()
        response = self.inventory.release("delayed")
        self.assertEqual(self.inventory.stock(), stock)
        self.inventory.close()
        self.inventory = Inventory(self.path, 999)
        self.assertEqual(self.inventory.release("delayed"), response)
        with self.assertRaises(ApiError) as caught:
            self.inventory.reserve(self.request("delayed"))
        self.assertEqual(caught.exception.code, "reservation_released")
        self.assertEqual(self.inventory.stock(), stock)

    def test_concurrent_reserve_release_leaves_no_deduction(self):
        stock = self.inventory.stock()
        def reserve():
            try:
                self.inventory.reserve(self.request("racing"))
            except ApiError as error:
                self.assertEqual(error.code, "reservation_released")
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            tasks = [executor.submit(reserve), executor.submit(self.inventory.release, "racing")]
            for task in tasks:
                task.result()
        self.assertEqual(self.inventory.reservation("racing")["status"], "released")
        self.assertEqual(self.inventory.stock(), stock)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.inventory = Inventory(Path(self.directory.name) / "state.json", 1)
        self.server = InventoryHTTPServer(("127.0.0.1", 0), self.inventory)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.inventory.close()
        self.directory.cleanup()

    def call(self, path, body=None, raw=None):
        data = raw if raw is not None else None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.url + path, data=data,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=3) as reply:
                return reply.status, json.load(reply)
        except urllib.error.HTTPError as error:
            return error.code, json.load(error)

    def test_http_reserve_exhaust_release_restock(self):
        self.assertEqual(self.call("/health"), (200, {"status": "ok"}))
        body = {"batch_id": "b1", "ingredients": PER_BATCH}
        status, first = self.call("/reserve", body)
        self.assertEqual(status, 200)
        self.assertEqual(self.call("/reserve", body), (200, first))
        status, error = self.call("/reserve", {**body, "batch_id": "b2"})
        self.assertEqual((status, error["error"]), (409, "insufficient_stock"))
        self.assertEqual(self.call("/reservations/b1")[1]["status"], "reserved")
        self.assertEqual(self.call("/reservations/b1/release", {})[0], 200)
        self.assertEqual(self.call("/restock", {"request_id": "r1", "ingredients": PER_BATCH})[0], 200)
        self.assertEqual(self.call("/stock")[1]["stock"]["malt_kg"], 1800)

    def test_malformed_json_and_unknown_fields_are_rejected(self):
        for raw in [b"{", b"[]", b'{"batch_id":"a","batch_id":"b"}', b'{"value":NaN}']:
            with self.subTest(raw=raw):
                self.assertEqual(self.call("/reserve", raw=raw)[0], 400)
        self.assertEqual(self.call("/stock?ignored=1")[0], 400)
        self.assertEqual(self.call("/not-found")[0], 404)


if __name__ == "__main__":
    unittest.main()
