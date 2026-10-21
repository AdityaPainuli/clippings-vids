"""Tests for /health and /ready endpoints.

Requires: pip install httpx  (for FastAPI TestClient)
Run with: python -m pytest backend/tests/test_health.py
      or: python -m unittest backend.tests.test_health
"""

import unittest
from unittest.mock import MagicMock, patch

try:
    import httpx  # noqa: F401 — needed by TestClient
    _HAS_TEST_DEPS = True
except ImportError:
    _HAS_TEST_DEPS = False

import sys

sys.modules.setdefault("supabase_client", MagicMock())
sys.modules.setdefault("clipper", MagicMock())
sys.modules.setdefault("captions", MagicMock())
sys.modules.setdefault("captions.api", MagicMock(router=MagicMock()))


def _get_client():
    from fastapi.testclient import TestClient
    from main import app
    return TestClient(app)


@unittest.skipUnless(_HAS_TEST_DEPS, "httpx not installed — skipping health endpoint tests")
class TestHealth(unittest.TestCase):
    def setUp(self):
        self.client = _get_client()

    def test_health_returns_200(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)

    def test_health_returns_ok_status(self):
        response = self.client.get("/health")
        self.assertEqual(response.json(), {"status": "ok"})


@unittest.skipUnless(_HAS_TEST_DEPS, "httpx not installed — skipping ready endpoint tests")
class TestReady(unittest.TestCase):
    def setUp(self):
        self.client = _get_client()

    def test_ready_returns_200_when_supabase_is_up(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.return_value = None

        with patch("main.supabase", mock_supabase):
            response = self.client.get("/ready")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ready")
        self.assertEqual(body["checks"]["supabase"], "ok")

    def test_ready_returns_503_when_supabase_is_down(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.side_effect = (
            Exception("Connection refused")
        )

        with patch("main.supabase", mock_supabase):
            response = self.client.get("/ready")

        self.assertEqual(response.status_code, 503)
        body = response.json()
        self.assertEqual(body["status"], "not ready")
        self.assertEqual(body["checks"]["supabase"], "unavailable")

    def test_ready_does_not_expose_connection_strings(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.side_effect = (
            Exception("postgresql://user:secret@host/db")
        )

        with patch("main.supabase", mock_supabase):
            response = self.client.get("/ready")

        body_text = response.text
        self.assertNotIn("secret", body_text)
        self.assertNotIn("postgresql://", body_text)


if __name__ == "__main__":
    unittest.main()
