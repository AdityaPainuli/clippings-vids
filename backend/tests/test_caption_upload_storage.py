import importlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


supabase_client_stub = types.ModuleType("supabase_client")
supabase_client_stub.supabase = object()
supabase_client_stub.SUPABASE_URL = "https://example.supabase.co"
supabase_client_stub.SUPABASE_SERVICE_KEY = "test-service-key"
sys.modules.setdefault("supabase_client", supabase_client_stub)


class _Result:
    def __init__(self, data=None):
        self.data = data or []


class _FakeQuery:
    def __init__(self, table):
        self.table = table
        self._action = None
        self._row = None
        self._filters = []

    def insert(self, row):
        self._action = "insert"
        self._row = dict(row)
        return self

    def select(self, *_columns):
        self._action = "select"
        return self

    def update(self, fields):
        self._action = "update"
        self._row = dict(fields)
        return self

    def delete(self):
        self._action = "delete"
        return self

    def eq(self, key, value):
        self._filters.append(("eq", key, value))
        return self

    def lt(self, key, value):
        self._filters.append(("lt", key, value))
        return self

    def in_(self, key, values):
        self._filters.append(("in", key, list(values)))
        return self

    def _matches(self, row):
        for kind, key, value in self._filters:
            if kind == "eq" and row.get(key) != value:
                return False
            if kind == "lt" and not (row.get(key, "") < value):
                return False
            if kind == "in" and row.get(key) not in value:
                return False
        return True

    def execute(self):
        rows = [row for row in self.table.rows if self._matches(row)]

        if self._action == "insert":
            self.table.rows.append(self._row)
            return _Result([self._row])
        if self._action == "select":
            return _Result([dict(row) for row in rows])
        if self._action == "update":
            for row in rows:
                row.update(self._row)
            return _Result(rows)
        if self._action == "delete":
            self.table.rows[:] = [row for row in self.table.rows if not self._matches(row)]
            return _Result([])
        raise AssertionError(f"unsupported action: {self._action}")


class _FakeTable:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def insert(self, row):
        return _FakeQuery(self).insert(row)

    def select(self, *columns):
        return _FakeQuery(self).select(*columns)

    def update(self, fields):
        return _FakeQuery(self).update(fields)

    def delete(self):
        return _FakeQuery(self).delete()


class _FakeSupabase:
    def __init__(self, tables):
        self.tables = tables

    def table(self, name):
        return self.tables[name]


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


class CaptionUploadStorageTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module("captions.storage")
        self.jobs = _FakeTable()
        self.uploads = _FakeTable()
        self.supabase = _FakeSupabase({
            "caption_jobs": self.jobs,
            "caption_uploads": self.uploads,
        })

    def test_signed_upload_is_registered_before_url_is_returned(self):
        response = _FakeResponse(payload={"url": "/object/upload/sign/captions/path"})

        with patch.object(self.storage, "supabase", self.supabase), \
             patch.object(self.storage.requests, "post", return_value=response):
            result = self.storage.create_signed_upload(
                "user-1", "upload-1", "video.mp4"
            )

        self.assertEqual(
            result["storage_path"],
            "user-1/sources/upload-1/video.mp4",
        )
        self.assertTrue(self.uploads.rows)
        self.assertEqual(
            self.uploads.rows[0]["storage_path"],
            result["storage_path"],
        )
        self.assertGreater(
            datetime.fromisoformat(self.uploads.rows[0]["expires_at"]),
            datetime.now(timezone.utc),
        )

    def test_failed_signed_url_creation_removes_tracking_row(self):
        response = _FakeResponse(status_code=500, text="storage unavailable")

        with patch.object(self.storage, "supabase", self.supabase), \
             patch.object(self.storage.requests, "post", return_value=response):
            with self.assertRaises(RuntimeError):
                self.storage.create_signed_upload(
                    "user-1", "upload-1", "video.mp4"
                )

        self.assertEqual(self.uploads.rows, [])

    def test_expired_unclaimed_upload_is_deleted_with_storage_object(self):
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        path = "user-1/sources/upload-1/video.mp4"
        self.uploads.rows.append({
            "id": "upload-row-1",
            "storage_path": path,
            "expires_at": past,
        })

        with patch.object(self.storage, "supabase", self.supabase), \
             patch.object(self.storage.requests, "delete") as mock_delete:
            deleted_jobs = self.storage.delete_expired(local_dir="__missing__")

        self.assertEqual(deleted_jobs, 0)
        self.assertEqual(self.uploads.rows, [])
        mock_delete.assert_called_once()
        self.assertEqual(mock_delete.call_args.kwargs["json"], {"prefixes": [path]})


if __name__ == "__main__":
    unittest.main()
