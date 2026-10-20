import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from localapp import assets  # noqa: E402


class FakeResponse:
    def __init__(self, status, body=b"", headers=None):
        self.status_code = status
        self._body = body
        self.headers = {"Content-Length": str(len(body)), **(headers or {})}
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size):
        yield self._body

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class RetryAfterTests(unittest.TestCase):
    """The ffprobe mirror rate-limits the second download in a row with
    429 + Retry-After. Setup must wait it out, not fail."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dest = Path(self.tmp.name) / "ffprobe.zip"
        self.body = b"ffprobe-bytes"
        self.sha = hashlib.sha256(self.body).hexdigest()

    def test_waits_out_429_then_installs(self):
        limited = FakeResponse(429, headers={"Retry-After": "60"})
        responses = [limited, FakeResponse(200, self.body)]
        with mock.patch.object(assets.requests, "get", side_effect=responses), \
                mock.patch.object(assets.time, "sleep") as sleep:
            assets._stream_download("https://x/ffprobe.zip", self.dest, {}, self.sha)
        sleep.assert_called_once_with(60.0)
        self.assertTrue(limited.closed)
        self.assertEqual(self.dest.read_bytes(), self.body)

    def test_gives_up_after_max_retries(self):
        responses = [FakeResponse(429, headers={"Retry-After": "5"})
                     for _ in range(assets._MAX_RETRIES + 1)]
        with mock.patch.object(assets.requests, "get", side_effect=responses), \
                mock.patch.object(assets.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError):
                assets._stream_download("https://x/ffprobe.zip", self.dest, {}, self.sha)
        self.assertEqual(sleep.call_count, assets._MAX_RETRIES)
        self.assertFalse(self.dest.exists())

    def test_retry_wait_is_capped_and_defaults(self):
        self.assertEqual(assets._retry_wait(FakeResponse(429, headers={"Retry-After": "9999"})),
                         assets._MAX_RETRY_WAIT)
        self.assertEqual(assets._retry_wait(FakeResponse(429)), 30)


if __name__ == "__main__":
    unittest.main()
