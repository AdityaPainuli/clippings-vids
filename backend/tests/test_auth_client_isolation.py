import asyncio
import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from test_main_job_race import _install_main_import_stubs


def _session_response():
    return types.SimpleNamespace(
        session=types.SimpleNamespace(access_token="user-jwt", refresh_token="user-refresh"),
        user=types.SimpleNamespace(id="user-1", email="user@example.com"),
    )


class FakeAuthError(Exception):
    def __init__(self, code, status):
        super().__init__(code)
        self.code = code
        self.status = status


class AuthRoutesUseThrowawayClientTest(unittest.TestCase):
    """Login/signup/refresh must never run on the shared service-role client.

    supabase-py swaps the shared client's PostgREST Authorization header to the
    user's JWT on SIGNED_IN/TOKEN_REFRESHED, so later caption_jobs writes would
    run under RLS as that user.
    """

    @classmethod
    def setUpClass(cls):
        _install_main_import_stubs()
        cls.main = importlib.import_module("main")

    def setUp(self):
        self.shared = MagicMock(name="shared_service_client")
        self.fresh = []

        def factory():
            client = MagicMock(name="auth_client")
            client.auth.sign_up.return_value = _session_response()
            client.auth.sign_in_with_password.return_value = _session_response()
            client.auth.refresh_session.return_value = _session_response()
            self.fresh.append(client)
            return client

        patcher_shared = patch.object(self.main, "supabase", self.shared)
        patcher_factory = patch.object(self.main, "new_auth_client", side_effect=factory)
        patcher_shared.start()
        patcher_factory.start()
        self.addCleanup(patcher_shared.stop)
        self.addCleanup(patcher_factory.stop)

    def assert_shared_auth_untouched(self):
        self.shared.auth.sign_up.assert_not_called()
        self.shared.auth.sign_in_with_password.assert_not_called()
        self.shared.auth.refresh_session.assert_not_called()

    def test_login_uses_throwaway_client(self):
        res = asyncio.run(self.main.login(email="user@example.com", password="pw"))
        self.assertEqual(res["access_token"], "user-jwt")
        self.assertEqual(len(self.fresh), 1)
        self.fresh[0].auth.sign_in_with_password.assert_called_once()
        self.assert_shared_auth_untouched()

    def test_signup_uses_throwaway_client(self):
        res = asyncio.run(self.main.signup(email="user@example.com", password="pw"))
        self.assertEqual(res["user_id"], "user-1")
        self.assertEqual(len(self.fresh), 1)
        self.fresh[0].auth.sign_up.assert_called_once()
        self.assert_shared_auth_untouched()

    def test_refresh_uses_throwaway_client(self):
        res = asyncio.run(self.main.refresh_token(refresh_token="user-refresh"))
        self.assertEqual(res["refresh_token"], "user-refresh")
        self.assertEqual(len(self.fresh), 1)
        self.fresh[0].auth.refresh_session.assert_called_once_with("user-refresh")
        self.assert_shared_auth_untouched()

    def assert_refresh_error_status(self, error, expected_status):
        auth_client = MagicMock(name="auth_client")
        auth_client.auth.refresh_session.side_effect = error
        with patch.object(
            self.main, "new_auth_client", return_value=auth_client
        ):
            with self.assertRaises(self.main.HTTPException) as context:
                asyncio.run(
                    self.main.refresh_token(refresh_token="user-refresh")
                )
        self.assertEqual(context.exception.status_code, expected_status)
        self.assert_shared_auth_untouched()

    def test_rejected_refresh_token_returns_401(self):
        self.assert_refresh_error_status(
            FakeAuthError("refresh_token_not_found", 400), 401
        )

    def test_invalid_refresh_input_returns_400(self):
        self.assert_refresh_error_status(
            FakeAuthError("validation_failed", 400), 400
        )

    def test_temporary_refresh_failure_returns_503(self):
        self.assert_refresh_error_status(
            FakeAuthError("request_timeout", 408), 503
        )

    def test_upstream_server_failure_returns_503(self):
        self.assert_refresh_error_status(
            FakeAuthError("unexpected_failure", 500), 503
        )

    def test_unexpected_local_failure_returns_500(self):
        self.assert_refresh_error_status(
            RuntimeError("Unexpected internal error"), 500
        )

    def test_each_call_gets_its_own_client(self):
        asyncio.run(self.main.login(email="a@example.com", password="pw"))
        asyncio.run(self.main.login(email="b@example.com", password="pw"))
        self.assertEqual(len(self.fresh), 2)
        self.assertIsNot(self.fresh[0], self.fresh[1])


if __name__ == "__main__":
    unittest.main()
