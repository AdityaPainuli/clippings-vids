import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


class _Response:
    def __init__(self, status_code=200, text=""):
        self.status_code = status_code
        self.text = text


class _Query:
    def __init__(self, data):
        self.data = data
        self.delete_called = False

    def select(self, *_args, **_kwargs):
        return self

    def lt(self, *_args, **_kwargs):
        return self

    def delete(self):
        self.delete_called = True
        return self

    def in_(self, *_args, **_kwargs):
        return self

    def execute(self):
        return self


class _Supabase:
    def __init__(self, rows):
        self.query = _Query(rows)

    def table(self, _name):
        return self.query


class StorageDeleteFailureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
        os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-key")

        supabase_client_stub = types.ModuleType("supabase_client")
        supabase_client_stub.supabase = object()
        supabase_client_stub.SUPABASE_URL = os.environ["SUPABASE_URL"]
        supabase_client_stub.SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
        sys.modules["supabase_client"] = supabase_client_stub

        sys.modules.pop("captions.storage", None)
        cls.storage = importlib.import_module("captions.storage")

    def test_delete_storage_paths_raises_on_http_failure(self):
        with patch.object(
            self.storage.requests,
            "delete",
            return_value=_Response(500, "storage unavailable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Storage delete failed 500"):
                self.storage._delete_storage_paths(["user/job/output.mp4"])

    def test_delete_storage_paths_accepts_successful_delete(self):
        with patch.object(
            self.storage.requests,
            "delete",
            return_value=_Response(200),
        ) as delete:
            self.storage._delete_storage_paths(["user/job/output.mp4"])

        delete.assert_called_once()

    def test_delete_expired_does_not_delete_rows_after_storage_failure(self):
        supabase = _Supabase(
            [{
                "id": "job-1",
                "source_path": "user/sources/job-1/input.mp4",
                "output_path": "user/outputs/job-1/output.mp4",
            }]
        )
        self.storage.supabase = supabase

        with patch.object(
            self.storage.requests,
            "delete",
            return_value=_Response(503, "storage unavailable"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Storage delete failed 503"):
                self.storage.delete_expired(local_dir="__missing_cleanup_dir__")

        self.assertFalse(supabase.query.delete_called)


if __name__ == "__main__":
    unittest.main()
