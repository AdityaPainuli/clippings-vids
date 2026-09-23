"""Tests for /health and /ready endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# We need to stub out heavy imports before importing main.
import sys
sys.modules.setdefault("supabase_client", MagicMock())
sys.modules.setdefault("clipper", MagicMock())
sys.modules.setdefault("captions", MagicMock())
sys.modules.setdefault("captions.api", MagicMock(router=MagicMock()))

from main import app  # noqa: E402  (import after sys.modules stubs)

client = TestClient(app)


class TestHealth:
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_status(self):
        response = client.get("/health")
        assert response.json() == {"status": "ok"}


class TestReady:
    def test_ready_returns_200_when_supabase_is_up(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.return_value = None

        with patch("main.supabase", mock_supabase):
            response = client.get("/ready")

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ready"
        assert body["checks"]["supabase"] == "ok"

    def test_ready_returns_503_when_supabase_is_down(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.side_effect = (
            Exception("Connection refused")
        )

        with patch("main.supabase", mock_supabase):
            response = client.get("/ready")

        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "not ready"
        assert body["checks"]["supabase"] == "unavailable"

    def test_ready_does_not_expose_connection_strings(self):
        mock_supabase = MagicMock()
        mock_supabase.table.return_value.select.return_value.limit.return_value.execute.side_effect = (
            Exception("postgresql://user:secret@host/db")
        )

        with patch("main.supabase", mock_supabase):
            response = client.get("/ready")

        body_text = response.text
        assert "secret" not in body_text
        assert "postgresql://" not in body_text
