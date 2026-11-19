"""Small JSON client shared by reproducible smoke checks and experiment tooling."""

import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen


class Api:
    def __init__(self, base_url, timeout=10):
        """Configure the shared API base URL and bounded request timeout."""
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def call(self, path, body=None, key=None):
        """Send GET or JSON POST, preserving idempotency keys and readable HTTP errors."""
        headers = {"Accept": "application/json", "X-Operator": "experiment-runner"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, allow_nan=False).encode()
        if key:
            headers["Idempotency-Key"] = key
        request = Request(self.base + "/api" + path, data=data, headers=headers)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                content = response.read()
                return json.loads(content) if content else {}
        except HTTPError as error:
            with error:
                details = error.read().decode()
            raise RuntimeError(
                f"{request.get_method()} {path}: HTTP {error.code}: {details}"
            ) from error
