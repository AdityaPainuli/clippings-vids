import importlib.util
import io
import os
import sys
import types
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

_BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

OLD = "2000-01-01T00:00:00+00:00"
FRESH = "2099-01-01T00:00:00+00:00"


class FakeResponse:
    def __init__(self, status_code=200, data=None, text=""):
        self.status_code = status_code
        self._data = data
        self.text = text

    def json(self):
        return self._data


class FakeQuery:
    """Just enough of the supabase-py builder for delete_expired."""

    def __init__(self, db):
        self.db = db
        self.op = None
        self.in_values = None

    def select(self, *_):
        self.op = "select"
        return self

    def delete(self):
        self.op = "delete"
        return self

    def lt(self, *_):
        self.op = "select_expired"
        return self

    def in_(self, column, values):
        self.in_values = (column, list(values))
        return self

    def execute(self):
        if self.op == "select_expired":
            return types.SimpleNamespace(data=list(self.db.expired))
        if self.op == "delete":
            self.db.deleted_ids.extend(self.in_values[1])
            return types.SimpleNamespace(data=[])
        column, values = self.in_values
        hits = [{"source_path": p} for p in values if p in self.db.live_sources]
        return types.SimpleNamespace(data=hits)


class FakeDB:
    def __init__(self, expired=(), live_sources=()):
        self.expired = list(expired)
        self.live_sources = set(live_sources)
        self.deleted_ids = []

    def table(self, name):
        assert name == "caption_jobs", name
        return FakeQuery(self)


def _load_storage():
    # Load the real captions/storage.py regardless of what other suites
    # have put in sys.modules for supabase_client or captions.
    stub = types.ModuleType("supabase_client")
    stub.supabase = None
    stub.SUPABASE_URL = "https://example.supabase.co"
    stub.SUPABASE_SERVICE_KEY = "test-key"
    spec = importlib.util.spec_from_file_location(
        "_captions_storage_under_test", os.path.join(_BACKEND, "captions", "storage.py"))
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"supabase_client": stub}):
        spec.loader.exec_module(module)
    return module


class OrphanedUploadRetentionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.storage = _load_storage()

    def _run(self, listing, db, delete_status=200, local_dir="/nonexistent-captions-dir",
             list_error=None):
        def post(url, **kwargs):
            if list_error:
                raise list_error
            self.assertIn("timeout", kwargs)
            payload = kwargs["json"]
            key = (payload["prefix"], payload["offset"])
            if key not in listing:
                raise AssertionError(f"Unexpected listing request: {payload}")
            return FakeResponse(data=listing[key])

        delete_mock = MagicMock(return_value=FakeResponse(status_code=delete_status, text="boom"))
        out = io.StringIO()
        with patch.object(self.storage, "supabase", db), \
             patch.object(self.storage.requests, "post", side_effect=post), \
             patch.object(self.storage.requests, "delete", delete_mock), \
             redirect_stdout(out):
            self.result = self.storage.delete_expired(local_dir=local_dir)
        deleted = [p for c in delete_mock.call_args_list for p in c.kwargs["json"]["prefixes"]]
        return deleted, out.getvalue()

    def test_old_unreferenced_upload_is_deleted(self):
        listing = {
            ("", 0): [{"name": "user-1"}],
            ("user-1/sources", 0): [{"name": "orphan"}, {"name": "live"}, {"name": "recent"}],
            ("user-1/sources/orphan", 0): [{"name": "a.mp4", "created_at": OLD}],
            ("user-1/sources/live", 0): [{"name": "b.mp4", "created_at": OLD}],
            ("user-1/sources/recent", 0): [{"name": "c.mp4", "created_at": FRESH}],
        }
        db = FakeDB(live_sources={"user-1/sources/live/b.mp4"})

        deleted, _ = self._run(listing, db)

        self.assertEqual(deleted, ["user-1/sources/orphan/a.mp4"])

    def test_orphan_on_second_listing_page_is_deleted(self):
        first_page = [{"name": f"f{i}.mp4", "created_at": FRESH} for i in range(1000)]
        listing = {
            ("", 0): [{"name": "user-1"}],
            ("user-1/sources", 0): [{"name": "job"}],
            ("user-1/sources/job", 0): first_page,
            ("user-1/sources/job", 1000): [{"name": "old.mp4", "created_at": OLD}],
        }

        deleted, _ = self._run(listing, FakeDB())

        self.assertEqual(deleted, ["user-1/sources/job/old.mp4"])

    def test_failed_storage_delete_is_logged_and_rows_kept(self):
        listing = {("", 0): []}
        db = FakeDB(expired=[{"id": "job-1", "source_path": "u/sources/j/a.mp4",
                              "output_path": None}])

        deleted, out = self._run(listing, db, delete_status=500)

        self.assertEqual(deleted, ["u/sources/j/a.mp4"])
        self.assertIn("Storage delete failed 500", out)
        self.assertEqual(db.deleted_ids, [])
        self.assertEqual(self.result, 0)
        self.assertNotIn("storage object(s) removed", out)

    def test_listing_timeout_still_cleans_local_files(self):
        import tempfile
        import time
        with tempfile.TemporaryDirectory() as local:
            old = os.path.join(local, "old.ass")
            open(old, "w").close()
            past = time.time() - self.storage.RETENTION_SECONDS - 60
            os.utime(old, (past, past))

            _, out = self._run({}, FakeDB(), local_dir=local,
                               list_error=self.storage.requests.ReadTimeout("slow"))

            self.assertFalse(os.path.exists(old))
        self.assertIn("orphaned upload sweep failed", out)


if __name__ == "__main__":
    unittest.main()
