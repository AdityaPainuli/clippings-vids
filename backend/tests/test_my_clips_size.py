import importlib
import os
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


class FakeResponse:
    def __init__(self, status_code=200, data=None):
        self.status_code = status_code
        self._data = data

    def json(self):
        return self._data


class MyClipsSizeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
        os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-key")

        supabase_module = types.ModuleType("supabase")
        supabase_module.Client = object
        supabase_module.create_client = lambda *args, **kwargs: object()
        supabase_module.ClientOptions = lambda **kwargs: kwargs
        sys.modules.setdefault("supabase", supabase_module)

        dotenv_module = types.ModuleType("dotenv")
        dotenv_module.load_dotenv = lambda: None
        sys.modules.setdefault("dotenv", dotenv_module)

        # Other suites stub supabase_client at import time, some with a
        # MagicMock, which passes any hasattr check. Only keep a real module.
        existing = sys.modules.get("supabase_client")
        if existing is not None and (
            not isinstance(existing, types.ModuleType)
            or not hasattr(existing, "requests")
        ):
            sys.modules.pop("supabase_client", None)
        cls.supabase_client = importlib.import_module("supabase_client")

    def _row(self, job_id, filename):
        return {
            "user_id": "user-1",
            "job_id": job_id,
            "filename": filename,
            "storage_path": f"user-1/{job_id}/{filename}",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    def _supabase_with_rows(self, rows):
        client = MagicMock()
        client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value.data = rows
        return client

    def test_clip_sizes_maps_listed_objects_to_their_storage_path(self):
        listing = [
            {"name": "a.mp4", "metadata": {"size": 1048576}},
            {"name": "b.mp4", "metadata": {"size": 2097152}},
        ]
        with patch.object(self.supabase_client, "_list_prefix", return_value=listing) as lister:
            sizes = self.supabase_client._clip_sizes("user-1", ["job-1"])

        lister.assert_called_once_with("user-1/job-1")
        self.assertEqual(
            sizes,
            {"user-1/job-1/a.mp4": 1048576, "user-1/job-1/b.mp4": 2097152},
        )

    def test_clip_sizes_ignores_objects_without_a_usable_size(self):
        listing = [
            {"name": "no-metadata.mp4"},
            {"name": "null-metadata.mp4", "metadata": None},
            {"name": "string-size.mp4", "metadata": {"size": "10"}},
            {"name": "bool-size.mp4", "metadata": {"size": True}},
            {"name": "ok.mp4", "metadata": {"size": 5}},
        ]
        with patch.object(self.supabase_client, "_list_prefix", return_value=listing):
            sizes = self.supabase_client._clip_sizes("user-1", ["job-1"])

        self.assertEqual(sizes, {"user-1/job-1/ok.mp4": 5})

    def test_get_user_clips_reports_size_bytes_and_none_when_unknown(self):
        rows = [self._row("job-1", "a.mp4"), self._row("job-1", "b.mp4")]
        listing = [{"name": "a.mp4", "metadata": {"size": 3145728}}]

        with patch.object(self.supabase_client, "supabase", self._supabase_with_rows(rows)), \
             patch.object(self.supabase_client.requests, "post",
                          return_value=FakeResponse(data={"signedURL": "https://signed.example/x"})), \
             patch.object(self.supabase_client, "_list_prefix", return_value=listing) as lister:
            clips = self.supabase_client.get_user_clips("user-1")

        by_name = {c["filename"]: c for c in clips}
        self.assertEqual(by_name["a.mp4"]["size_bytes"], 3145728)
        self.assertIsNone(by_name["b.mp4"]["size_bytes"])
        # Two clips from one job need only one listing.
        lister.assert_called_once_with("user-1/job-1")

    def test_get_user_clips_lists_each_job_once(self):
        rows = [self._row("job-1", "a.mp4"), self._row("job-2", "c.mp4")]

        with patch.object(self.supabase_client, "supabase", self._supabase_with_rows(rows)), \
             patch.object(self.supabase_client.requests, "post",
                          return_value=FakeResponse(data={"signedURL": "https://signed.example/x"})), \
             patch.object(self.supabase_client, "_list_prefix", return_value=[]) as lister:
            self.supabase_client.get_user_clips("user-1")

        self.assertEqual(
            sorted(call.args[0] for call in lister.call_args_list),
            ["user-1/job-1", "user-1/job-2"],
        )

    def test_get_user_clips_skips_listing_when_the_user_has_no_clips(self):
        with patch.object(self.supabase_client, "supabase", self._supabase_with_rows([])), \
             patch.object(self.supabase_client, "_list_prefix") as lister:
            clips = self.supabase_client.get_user_clips("user-1")

        self.assertEqual(clips, [])
        lister.assert_not_called()


if __name__ == "__main__":
    unittest.main()
