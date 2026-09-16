import importlib
import json
import os
import sys
import types
import unittest
from unittest.mock import patch


class _SupabaseClient:
    def table(self, *args, **kwargs):
        raise AssertionError("database access is not expected in these tests")


supabase_module = types.ModuleType("supabase")
supabase_module.Client = _SupabaseClient
supabase_module.create_client = lambda *args, **kwargs: _SupabaseClient()
sys.modules.setdefault("supabase", supabase_module)


dotenv_module = types.ModuleType("dotenv")
dotenv_module.load_dotenv = lambda: None
sys.modules.setdefault("dotenv", dotenv_module)

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-service-key")


class _Response:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self.payload = payload
        self.text = text

    def json(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class StorageListingErrorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage = importlib.import_module("supabase_client")

    def test_list_prefix_raises_on_storage_http_error(self):
        response = _Response(status_code=503, text="storage unavailable")

        with patch.object(self.storage.requests, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, r"Storage listing failed 503: storage unavailable"):
                self.storage._list_prefix("user-1")

    def test_list_prefix_raises_on_invalid_json(self):
        response = _Response(status_code=200, payload=ValueError("not json"))

        with patch.object(self.storage.requests, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "Storage listing returned invalid JSON"):
                self.storage._list_prefix("user-1")

    def test_list_prefix_rejects_unexpected_response_shape(self):
        response = _Response(status_code=200, payload={"error": "broken"})

        with patch.object(self.storage.requests, "post", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "Storage listing returned an unexpected response shape"):
                self.storage._list_prefix("user-1")

    def test_delete_old_clips_does_not_convert_nested_listing_failure_to_empty_cleanup(self):
        responses = [
            _Response(status_code=200, payload=[{"name": "user-1"}]),
            _Response(status_code=500, text="storage unavailable"),
        ]

        with patch.object(self.storage.requests, "post", side_effect=responses):
            with self.assertRaisesRegex(RuntimeError, r"Storage listing failed 500: storage unavailable"):
                self.storage.delete_old_clips()


if __name__ == "__main__":
    unittest.main()
