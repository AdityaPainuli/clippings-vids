import importlib.util
import os
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


class _Query:
    def __init__(self, rows, calls):
        self.rows = rows
        self.calls = calls
        self.filters = []
        self.deleting = False

    def select(self, columns):
        return self

    def eq(self, column, value):
        self.filters.append((column, value))
        return self

    def delete(self):
        self.deleting = True
        return self

    def execute(self):
        matched = [row for row in self.rows
                   if all(row.get(k) == v for k, v in self.filters)]
        if self.deleting:
            self.rows[:] = [row for row in self.rows if row not in matched]
            self.calls.append(("delete", list(self.filters)))
            return types.SimpleNamespace(data=matched)
        self.calls.append(("select", list(self.filters)))
        return types.SimpleNamespace(data=matched)


class _Supabase:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def table(self, name):
        assert name == "clip_metadata"
        return _Query(self.rows, self.calls)


class DeleteClipRecordsTest(unittest.TestCase):
    def setUp(self):
        rows = [
            {"user_id": "u1", "job_id": "j1", "storage_path": "u1/j1/a.mp4"},
            {"user_id": "u1", "job_id": "j1", "storage_path": "u1/j1/b.mp4"},
            {"user_id": "u2", "job_id": "j1", "storage_path": "u2/j1/c.mp4"},
            {"user_id": "u1", "job_id": "j2", "storage_path": "u1/j2/d.mp4"},
        ]
        fake_supabase = types.ModuleType("supabase")
        fake_supabase.Client = object
        fake_supabase.ClientOptions = object
        self.client = _Supabase(rows)
        fake_supabase.create_client = lambda *a, **k: self.client
        self.fake_supabase = fake_supabase
        self.rows = rows

        path = Path(__file__).resolve().parents[1] / "supabase_client.py"
        with mock.patch.dict(os.environ, {
                "SUPABASE_URL": "https://example.test",
                "SUPABASE_SERVICE_KEY": "service-key"}), \
                mock.patch.dict(sys.modules, {"supabase": fake_supabase}):
            spec = importlib.util.spec_from_file_location(
                "supabase_client_under_test", path)
            self.module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.module)

    def test_deletes_only_this_users_job_and_returns_paths(self):
        delete_paths = mock.Mock()
        self.module._delete_paths = delete_paths

        paths = self.module.delete_clip_records("j1", "u1")

        self.assertEqual(paths, ["u1/j1/a.mp4", "u1/j1/b.mp4"])
        delete_paths.assert_called_once_with(paths)
        self.assertEqual(
            [(r["user_id"], r["job_id"]) for r in self.rows],
            [("u2", "j1"), ("u1", "j2")])
        self.assertEqual(self.client.calls, [
            ("select", [("user_id", "u1"), ("job_id", "j1")]),
            ("delete", [("user_id", "u1"), ("job_id", "j1")]),
        ])

    def test_no_rows_means_no_storage_or_database_delete(self):
        delete_paths = mock.Mock()
        self.module._delete_paths = delete_paths

        paths = self.module.delete_clip_records("missing", "u1")

        self.assertEqual(paths, [])
        delete_paths.assert_not_called()
        self.assertEqual(len(self.rows), 4)


if __name__ == "__main__":
    unittest.main()
