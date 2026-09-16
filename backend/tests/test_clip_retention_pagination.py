import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch


class FakeResponse:
    def __init__(self, status_code=200, data=None, text=""):
        self.status_code = status_code
        self._data = data
        self.text = text

    def json(self):
        return self._data


class ClipRetentionPaginationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
        os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-key")

        supabase_module = types.ModuleType("supabase")
        supabase_module.Client = object
        supabase_module.create_client = lambda *args, **kwargs: object()
        sys.modules.setdefault("supabase", supabase_module)

        dotenv_module = types.ModuleType("dotenv")
        dotenv_module.load_dotenv = lambda: None
        sys.modules.setdefault("dotenv", dotenv_module)

        existing = sys.modules.get("supabase_client")
        if existing is not None and not hasattr(existing, "requests"):
            sys.modules.pop("supabase_client", None)
        cls.supabase_client = importlib.import_module("supabase_client")

    def test_list_prefix_fetches_pages_after_first_1000_items(self):
        first_page = [{"name": f"clip-{i}"} for i in range(1000)]
        second_page = [{"name": "clip-1000"}]

        def post(url, **kwargs):
            payload = kwargs["json"]
            offset = payload["offset"]
            return FakeResponse(data=first_page if offset == 0 else second_page)

        with patch.object(self.supabase_client.requests, "post", side_effect=post) as post_mock:
            result = self.supabase_client._list_prefix("user/job")

        self.assertEqual(len(result), 1001)
        self.assertEqual(
            [call.kwargs["json"]["offset"] for call in post_mock.call_args_list],
            [0, 1000],
        )

    def test_retention_scan_deletes_expired_object_on_second_page(self):
        fresh = "2099-01-01T00:00:00+00:00"
        old = "2000-01-01T00:00:00+00:00"
        root_page = [{"name": "user-1"}]
        job_page = [{"name": "job-1"}]
        first_file_page = [
            {"name": f"clip-{i}.mp4", "created_at": fresh} for i in range(1000)
        ]
        second_file_page = [{"name": "expired.mp4", "created_at": old}]

        def post(url, **kwargs):
            payload = kwargs["json"]
            prefix = payload["prefix"]
            offset = payload["offset"]
            if prefix == "":
                return FakeResponse(data=root_page)
            if prefix == "user-1":
                return FakeResponse(data=job_page)
            if prefix == "user-1/job-1" and offset == 0:
                return FakeResponse(data=first_file_page)
            if prefix == "user-1/job-1" and offset == 1000:
                return FakeResponse(data=second_file_page)
            raise AssertionError(f"Unexpected listing request: {payload}")

        delete_mock = MagicMock()

        with patch.object(self.supabase_client.requests, "post", side_effect=post), patch.object(
            self.supabase_client.requests, "delete", delete_mock
        ):
            deleted = self.supabase_client.delete_old_clips()

        self.assertEqual(deleted, 1)
        delete_mock.assert_called_once()
        self.assertEqual(
            delete_mock.call_args.kwargs["json"],
            {"prefixes": ["user-1/job-1/expired.mp4"]},
        )


if __name__ == "__main__":
    unittest.main()
