"""Thread-safe, in-memory mock of the brewery ingredient inventory.

The backend uses this service to reserve ingredients before admitting a batch.
Stock, retry records and cancellation tombstones reset on every process restart.
No database or file persistence belongs to this mock.
"""

from __future__ import annotations

import copy
import json
import math
import os
import re
import signal
import threading
from dataclasses import dataclass
from decimal import Decimal, localcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit


INGREDIENTS = ("malt_kg", "hops_kg", "yeast_kg", "water_l")
PER_BATCH = {"malt_kg": 900, "hops_kg": 20, "yeast_kg": 10, "water_l": 6000}
MAX_BODY = 65536
MAX_QUANTITY = Decimal("1000000000000")
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


class ApiError(Exception):
    """An expected validation/domain failure translated into a JSON HTTP error."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code = status, code


def fail(message: str) -> None:
    """Reject invalid client input consistently with HTTP 400."""
    raise ApiError(400, "invalid_request", message)


def fields(body: object, required: set[str]) -> dict:
    """Require a JSON object with exactly the documented request fields."""
    if not isinstance(body, dict) or set(body) != required:
        fail("Expected exactly these fields: " + ", ".join(sorted(required)))
    return body


def identifier(value: object) -> str:
    """Validate the batch/delivery key used to recognize retries."""
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        fail("Identifiers must contain 1-128 letters, digits, dots, colons, underscores or hyphens")
    return value


def quantities(value: object) -> dict[str, Decimal]:
    """Validate all four quantities and use decimals to avoid stock rounding drift."""
    body = fields(value, set(INGREDIENTS))
    result = {}
    for key in INGREDIENTS:
        number = body[key]
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            fail(f"{key} must be a number")
        if isinstance(number, float) and not math.isfinite(number):
            fail(f"{key} must be finite")
        amount = Decimal(str(number))
        if amount < 0 or amount > MAX_QUANTITY or amount.as_tuple().exponent < -9:
            fail(f"{key} must be between 0 and 1e12 with at most nine decimal places")
        result[key] = amount
    return result


def public_stock(stock: dict[str, Decimal]) -> dict[str, float]:
    """Convert internal decimal amounts into the numeric JSON API representation."""
    return {key: float(value) for key, value in stock.items()}


def add_stock(
    stock: dict[str, Decimal], ingredients: dict[str, Decimal], sign: int
) -> dict[str, Decimal]:
    """Calculate a stock change before mutation, rejecting unsupported totals."""
    with localcontext() as context:
        context.prec = 40
        updated = {
            key: stock[key] + sign * ingredients[key]
            for key in INGREDIENTS
        }
    if any(value > MAX_QUANTITY for value in updated.values()):
        fail("Resulting stock exceeds the supported quantity limit")
    return updated


@dataclass
class Reservation:
    """One process-local batch reservation, including its original retry response."""

    ingredients: dict[str, Decimal]
    status: str
    response: dict
    release_response: dict | None = None


@dataclass
class Delivery:
    """A received delivery and its response, retained to prevent duplicate restocks."""

    ingredients: dict[str, Decimal]
    response: dict


class Inventory:
    """Serialize stock and retry-ledger changes across HTTP request threads.

    The lock covers checking stock, changing quantities and recording an ID.
    This makes a reservation atomic within this process. A new Inventory is a
    fresh mock: neither stock nor retry/cancellation history survives restart.
    """

    def __init__(self, initial_batches: int = 100):
        if (
            isinstance(initial_batches, bool)
            or not isinstance(initial_batches, int)
            or not 0 <= initial_batches <= 1000000
        ):
            raise ValueError("INITIAL_BATCHES must be an integer from 0 to 1000000")
        self._mutex = threading.Lock()
        self._stock = {
            key: Decimal(value * initial_batches) for key, value in PER_BATCH.items()
        }
        self._reservations: dict[str, Reservation] = {}
        self._deliveries: dict[str, Delivery] = {}

    def stock(self) -> dict:
        """Return a detached snapshot of ingredients still available for admission."""
        with self._mutex:
            return {"stock": public_stock(self._stock)}

    def reserve(self, body: dict) -> dict:
        """Deduct a batch once; identical retries return the original response.

        A cancellation tombstone takes precedence over a delayed reserve. Both
        this protection and retry detection apply only during this process life.
        """
        fields(body, {"batch_id", "ingredients"})
        batch_id = identifier(body["batch_id"])
        requested = quantities(body["ingredients"])
        with self._mutex:
            previous = self._reservations.get(batch_id)
            if previous:
                if previous.status == "released":
                    raise ApiError(409, "reservation_released", "A released reservation cannot be reused")
                if previous.ingredients != requested:
                    raise ApiError(
                        409, "idempotency_conflict",
                        "This batch ID already has different ingredients",
                    )
                return copy.deepcopy(previous.response)
            if any(self._stock[key] < requested[key] for key in INGREDIENTS):
                raise ApiError(409, "insufficient_stock", "Not enough ingredients to reserve this batch")
            updated_stock = add_stock(self._stock, requested, -1)
            response = {"batch_id": batch_id, "status": "reserved", "stock": public_stock(updated_stock)}
            self._stock = updated_stock
            self._reservations[batch_id] = Reservation(requested, "reserved", response)
            return copy.deepcopy(response)

    def reservation(self, batch_id: str) -> dict:
        """Look up a reservation or cancellation known by this process."""
        identifier(batch_id)
        with self._mutex:
            reservation = self._reservations.get(batch_id)
            if reservation is None:
                raise ApiError(404, "not_found", "Reservation not found")
            return {
                "batch_id": batch_id,
                "status": reservation.status,
                "ingredients": public_stock(reservation.ingredients),
            }

    def release(self, batch_id: str) -> dict:
        """Refund a cancelled admission once, or fence a not-yet-arrived reserve.

        The backend calls this only before production admission. Flushing beer
        must not refund ingredients. Unknown IDs become zero-quantity tombstones
        so a reserve delayed by the network cannot consume stock afterwards.
        """
        identifier(batch_id)
        with self._mutex:
            previous = self._reservations.get(batch_id)
            if previous and previous.release_response is not None:
                return copy.deepcopy(previous.release_response)
            ingredients = previous.ingredients if previous else {
                key: Decimal(0) for key in INGREDIENTS
            }
            updated_stock = add_stock(self._stock, ingredients, 1)
            response = {"batch_id": batch_id, "status": "released", "stock": public_stock(updated_stock)}
            self._stock = updated_stock
            self._reservations[batch_id] = Reservation(
                ingredients, "released", previous.response if previous else response, response
            )
            return copy.deepcopy(response)

    def restock(self, body: dict) -> dict:
        """Add a delivery once, rejecting reuse of its ID with different amounts."""
        fields(body, {"request_id", "ingredients"})
        request_id = identifier(body["request_id"])
        requested = quantities(body["ingredients"])
        with self._mutex:
            previous = self._deliveries.get(request_id)
            if previous:
                if previous.ingredients != requested:
                    raise ApiError(
                        409, "idempotency_conflict",
                        "This request ID already has different ingredients",
                    )
                return copy.deepcopy(previous.response)
            updated_stock = add_stock(self._stock, requested, 1)
            response = {"request_id": request_id, "status": "restocked", "stock": public_stock(updated_stock)}
            self._stock = updated_stock
            self._deliveries[request_id] = Delivery(requested, response)
            return copy.deepcopy(response)


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    """Reject ambiguous JSON objects containing the same key more than once."""
    result = {}
    for key, value in pairs:
        if key in result:
            fail("Duplicate JSON keys are not allowed")
        result[key] = value
    return result


class InventoryHTTPServer(ThreadingHTTPServer):
    """Share one Inventory across request threads; the domain object owns locking."""

    daemon_threads = True

    def __init__(self, address: tuple[str, int], inventory: Inventory):
        self.inventory = inventory
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    """Translate the small inventory HTTP contract into domain operations."""

    server_version = "BeerFactoryInventory/1"

    def _json(self, status: int, data: dict) -> None:
        """Send a bounded JSON response that must not be reused by an HTTP cache."""
        payload = json.dumps(data, allow_nan=False, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _body(self, optional: bool = False) -> dict:
        """Read a length-limited JSON object and reject ambiguous or nonfinite input."""
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
        """Route requests and expose expected domain failures as structured errors."""
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

    def do_GET(self) -> None:
        """Handle the read-only stock, reservation and health endpoints."""
        self._dispatch("GET")

    def do_POST(self) -> None:
        """Handle reservation, cancellation and delivery mutations."""
        self._dispatch("POST")

    def log_message(self, template: str, *args: object) -> None:
        """Emit one structured log line per request for container log collection."""
        print(json.dumps({
            "service": "inventory",
            "client": self.client_address[0],
            "message": template % args,
        }), flush=True)


def main() -> None:
    """Seed a fresh mock, serve HTTP and close the listener on SIGTERM/SIGINT."""
    inventory = Inventory(int(os.environ.get("INITIAL_BATCHES", "100")))
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


if __name__ == "__main__":
    main()
