"""Keep transient HTTP failures pollable by smoke and experiment tooling."""

import io
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from http_client import Api


class HttpClientTests(unittest.TestCase):
    def test_http_failures_preserve_method_and_status(self):
        """A startup 502 or rejected POST must remain a readable RuntimeError."""
        for body, method, status in [(None, "GET", 502), ({}, "POST", 409)]:
            with self.subTest(method=method):
                response = io.BytesIO(b'{"error":"temporarily_unavailable"}')
                failure = HTTPError(
                    "http://factory/api/health", status, "failure", {}, response
                )
                with patch("http_client.urlopen", side_effect=failure):
                    with self.assertRaisesRegex(
                        RuntimeError, f"{method} /health: HTTP {status}"
                    ):
                        Api("http://factory").call("/health", body)
                self.assertTrue(response.closed)


if __name__ == "__main__":
    unittest.main()
