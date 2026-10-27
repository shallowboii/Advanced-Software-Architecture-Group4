"""Serialized ingredient reservations with a durable idempotency ledger."""

from __future__ import annotations

import copy
import fcntl
import json
import math
import os
import re
import signal
import tempfile
import threading
from decimal import Decimal, InvalidOperation, localcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit


INGREDIENTS = ("malt_kg", "hops_kg", "yeast_kg", "water_l")
PER_BATCH = {"malt_kg": 900, "hops_kg": 20, "yeast_kg": 10, "water_l": 6000}
MAX_BODY = 65536
MAX_QUANTITY = Decimal("1000000000000")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code = status, code


def fail(message: str) -> None:
    raise ApiError(400, "invalid_request", message)


def fields(body: object, required: set[str]) -> dict:
    if not isinstance(body, dict) or set(body) != required:
        fail("Expected exactly these fields: " + ", ".join(sorted(required)))
    return body


def identifier(value: object) -> str:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        fail("Identifiers must contain 1-128 letters, digits, dots, colons, underscores or hyphens")
    return value


def quantities(value: object) -> dict[str, str]:
    fields(value, set(INGREDIENTS))
    result = {}
    for key in INGREDIENTS:
        number = value[key]
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            fail(f"{key} must be a number")
        if isinstance(number, float) and not math.isfinite(number):
            fail(f"{key} must be finite")
        decimal = Decimal(str(number))
        if decimal < 0 or decimal > MAX_QUANTITY or decimal.as_tuple().exponent < -9:
            fail(f"{key} must be between 0 and 1e12 with at most nine decimal places")
        result[key] = str(decimal.normalize())
    return result


def public_stock(stock: dict[str, str]) -> dict[str, float]:
    return {key: float(value) for key, value in stock.items()}


def add_stock(stock: dict, ingredients: dict, sign: int) -> dict:
    with localcontext() as ctx:
        ctx.prec = 40
        result = {key: Decimal(stock[key]) + sign * Decimal(ingredients[key]) for key in INGREDIENTS}
    if any(value > MAX_QUANTITY for value in result.values()):
        fail("Resulting stock exceeds the supported quantity limit")
    return {key: str(value.normalize()) for key, value in result.items()}


class Inventory:
    def __init__(self, path: str | Path, initial_batches: int = 100):
        if isinstance(initial_batches, bool) or not isinstance(initial_batches, int) or not 0 <= initial_batches <= 1000000:
            raise ValueError("INITIAL_BATCHES must be an integer from 0 to 1000000")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._mutex = threading.RLock()
        self._lock_file = open(str(self.path) + ".lock", "a", encoding="utf-8")
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.path.exists():
                self._state = json.loads(self.path.read_text(encoding="utf-8"))
                self._validate_state()
            else:
                self._state = {}
                self._commit({
                    "version": 1,
                    "stock": {key: str(value * initial_batches) for key, value in PER_BATCH.items()},
                    "reservations": {}, "restocks": {},
                })
        except Exception:
            self._lock_file.close()
            raise

    def _validate_state(self) -> None:
        try:
            state = self._state
            if set(state) != {"version", "stock", "reservations", "restocks"} or state["version"] != 1:
                raise ValueError("unsupported state format")
            if set(state["stock"]) != set(INGREDIENTS):
                raise ValueError("invalid stock keys")
            for amount in state["stock"].values():
                parsed = Decimal(amount)
                if not parsed.is_finite() or not 0 <= parsed <= MAX_QUANTITY:
                    raise ValueError("invalid stock quantity")
            for key, reservation in state["reservations"].items():
                identifier(key)
                if reservation["status"] not in ("reserved", "released"):
                    raise ValueError("invalid reservation status")
                self._validate_stored_quantities(reservation["ingredients"])
                if reservation["response"]["batch_id"] != key:
                    raise ValueError("invalid reservation response")
            for key, restock in state["restocks"].items():
                identifier(key)
                self._validate_stored_quantities(restock["ingredients"])
                if restock["response"]["request_id"] != key:
                    raise ValueError("invalid restock response")
        except (KeyError, TypeError, ValueError, InvalidOperation, ApiError, AttributeError) as error:
            raise ValueError("Inventory state is corrupt; refusing to reseed stock") from error

    @staticmethod
    def _validate_stored_quantities(values: dict) -> None:
        if set(values) != set(INGREDIENTS):
            raise ValueError("invalid ingredient keys")
        if any(not Decimal(value).is_finite() or not 0 <= Decimal(value) <= MAX_QUANTITY for value in values.values()):
            raise ValueError("invalid stored quantity")

    def _commit(self, candidate: dict) -> None:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.path.parent,
                                             prefix=self.path.name + ".", suffix=".tmp", delete=False) as stream:
                temporary = stream.name
                json.dump(candidate, stream, allow_nan=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            self._state = candidate
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)

    def close(self) -> None:
        self._lock_file.close()

    def stock(self) -> dict:
        with self._mutex:
            return {"stock": public_stock(self._state["stock"])}

    def reserve(self, body: dict) -> dict:
        fields(body, {"batch_id", "ingredients"})
        batch_id = identifier(body["batch_id"])
        requested = quantities(body["ingredients"])
        with self._mutex:
            previous = self._state["reservations"].get(batch_id)
            if previous:
                if previous["ingredients"] != requested:
                    raise ApiError(409, "idempotency_conflict", "This batch ID already has different ingredients")
                if previous["status"] == "released":
                    raise ApiError(409, "reservation_released", "A released reservation cannot be reused")
                return copy.deepcopy(previous["response"])
            if any(Decimal(self._state["stock"][key]) < Decimal(requested[key]) for key in INGREDIENTS):
                raise ApiError(409, "insufficient_stock", "Not enough ingredients to reserve this batch")
            candidate = copy.deepcopy(self._state)
            candidate["stock"] = add_stock(candidate["stock"], requested, -1)
            response = {"batch_id": batch_id, "status": "reserved", "stock": public_stock(candidate["stock"])}
            candidate["reservations"][batch_id] = {
                "ingredients": requested, "status": "reserved", "response": response,
            }
            self._commit(candidate)
            return copy.deepcopy(response)

    def reservation(self, batch_id: str) -> dict:
        identifier(batch_id)
        with self._mutex:
            item = self._state["reservations"].get(batch_id)
            if item is None:
                raise ApiError(404, "not_found", "Reservation not found")
            return {"batch_id": batch_id, "status": item["status"], "ingredients": public_stock(item["ingredients"])}

    def release(self, batch_id: str) -> dict:
        identifier(batch_id)
        with self._mutex:
            previous = self._state["reservations"].get(batch_id)
            if previous is None:
                raise ApiError(404, "not_found", "Reservation not found")
            if previous["status"] == "released":
                return copy.deepcopy(previous["release_response"])
            candidate = copy.deepcopy(self._state)
            candidate["stock"] = add_stock(candidate["stock"], previous["ingredients"], 1)
            response = {"batch_id": batch_id, "status": "released", "stock": public_stock(candidate["stock"])}
            candidate["reservations"][batch_id].update(status="released", release_response=response)
            self._commit(candidate)
            return copy.deepcopy(response)

    def restock(self, body: dict) -> dict:
        fields(body, {"request_id", "ingredients"})
        request_id = identifier(body["request_id"])
        requested = quantities(body["ingredients"])
        with self._mutex:
            previous = self._state["restocks"].get(request_id)
            if previous:
                if previous["ingredients"] != requested:
                    raise ApiError(409, "idempotency_conflict", "This request ID already has different ingredients")
                return copy.deepcopy(previous["response"])
            candidate = copy.deepcopy(self._state)
            candidate["stock"] = add_stock(candidate["stock"], requested, 1)
            response = {"request_id": request_id, "status": "restocked", "stock": public_stock(candidate["stock"])}
            candidate["restocks"][request_id] = {"ingredients": requested, "response": response}
            self._commit(candidate)
            return copy.deepcopy(response)


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            fail("Duplicate JSON keys are not allowed")
        result[key] = value
    return result


class InventoryHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], inventory: Inventory):
        self.inventory = inventory
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    server_version = "BeerFactoryInventory/1"

    def _json(self, status: int, data: dict) -> None:
        payload = json.dumps(data, allow_nan=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _body(self, optional: bool = False) -> dict:
        if self.headers.get("Transfer-Encoding"):
            fail("Transfer-Encoding is not supported; send Content-Length")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            fail("Invalid Content-Length")
        if length < 0:
            fail("Invalid Content-Length")
        if length > MAX_BODY:
            raise ApiError(413, "body_too_large", "Body exceeds 64 KiB")
        if length == 0 and optional:
            return {}
        if self.headers.get_content_type() != "application/json":
            raise ApiError(415, "unsupported_media_type", "Expected application/json")
        self.connection.settimeout(10)
        try:
            payload = self.rfile.read(length)
            if len(payload) != length:
                fail("Incomplete request body")
            body = json.loads(payload, object_pairs_hook=unique_object,
                              parse_constant=lambda _: fail("Nonfinite JSON numbers are not allowed"))
        except (UnicodeDecodeError, json.JSONDecodeError, TimeoutError):
            fail("Invalid JSON body")
        if not isinstance(body, dict):
            fail("Expected a JSON object")
        return body

    def _dispatch(self, method: str) -> None:
        try:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                fail("Query parameters are not supported")
            path = unquote(parsed.path)
            inventory = self.server.inventory
            if method == "GET" and path == "/health":
                response = {"status": "ok"}
            elif method == "GET" and path == "/stock":
                response = inventory.stock()
            elif method == "POST" and path == "/reserve":
                response = inventory.reserve(self._body())
            elif method == "POST" and path == "/restock":
                response = inventory.restock(self._body())
            elif method == "GET" and (match := re.fullmatch(r"/reservations/([^/]+)", path)):
                response = inventory.reservation(match[1])
            elif method == "POST" and (match := re.fullmatch(r"/reservations/([^/]+)/release", path)):
                fields(self._body(optional=True), set())
                response = inventory.release(match[1])
            else:
                raise ApiError(404, "not_found", "Endpoint not found")
            self._json(200, response)
        except ApiError as error:
            self._json(error.status, {"error": error.code, "message": str(error)})
        except OSError:
            self._json(503, {"error": "storage_unavailable", "message": "Could not persist the operation; retry with the same ID"})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def log_message(self, template: str, *args: object) -> None:
        print(json.dumps({"service": "inventory", "client": self.client_address[0], "message": template % args}), flush=True)


def main() -> None:
    inventory = Inventory(os.environ.get("INVENTORY_STATE_FILE", "/data/inventory.json"),
                          int(os.environ.get("INITIAL_BATCHES", "100")))
    server = InventoryHTTPServer((os.environ.get("INVENTORY_HOST", "0.0.0.0"),
                                  int(os.environ.get("PORT", "8081"))), inventory)
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(json.dumps({"service": "inventory", "status": "listening", "port": server.server_port}), flush=True)
    try:
        stopping.wait()
    finally:
        server.shutdown()
        server.server_close()
        inventory.close()


if __name__ == "__main__":
    main()
