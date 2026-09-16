import sys
import types
import unittest


# Keep this unit test independent of the optional production Supabase SDK.
supabase_client_stub = types.ModuleType("supabase_client")
supabase_client_stub.supabase = object()
sys.modules.setdefault("supabase_client", supabase_client_stub)

import clip_cache


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, store, operation):
        self.store = store
        self.operation = operation
        self.filters = []
        self.selected = None
        self.payload = None
        self.conflict = None

    def select(self, columns):
        self.selected = columns
        return self

    def eq(self, column, value):
        self.filters.append((column, lambda current: current == value))
        return self

    def lte(self, column, value):
        self.filters.append((column, lambda current: current <= value))
        return self

    def limit(self, value):
        return self

    def upsert(self, payload, on_conflict=None):
        self.operation = "upsert"
        self.payload = payload
        self.conflict = on_conflict
        return self

    def delete(self):
        self.operation = "delete"
        return self

    def execute(self):
        if self.operation == "upsert":
            key = self.payload[self.conflict]
            self.store[key] = dict(self.payload)
            return FakeResponse([self.store[key]])

        rows = list(self.store.values())
        for column, predicate in self.filters:
            rows = [row for row in rows if predicate(row.get(column))]

        if self.operation == "delete":
            for row in rows:
                self.store.pop(row["cache_key"], None)
            return FakeResponse(rows)

        if self.selected:
            fields = [field.strip() for field in self.selected.split(",")]
            rows = [{field: row.get(field) for field in fields} for row in rows]
        return FakeResponse(rows[:1])


class FakeSupabase:
    def __init__(self):
        self.rows = {}

    def table(self, _name):
        return FakeQuery(self.rows, "select")


class ClipCacheTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeSupabase()
        self.original_supabase = clip_cache.supabase
        clip_cache.supabase = self.client
        self.addCleanup(self._restore_supabase)

    def _restore_supabase(self):
        clip_cache.supabase = self.original_supabase

    def test_cache_written_by_one_worker_is_visible_to_another(self):
        results = [{"storage_path": "user/job/clip.mp4", "url": "signed-url"}]

        # Worker A writes the result into shared persistence.
        clip_cache.put("cache-key", "user-1", results)

        # Worker B has no process-local cache, but reads the same persistent store.
        self.assertEqual(clip_cache.get("cache-key", "user-1"), results)

    def test_cache_is_scoped_to_user(self):
        results = [{"storage_path": "user/job/clip.mp4"}]
        clip_cache.put("cache-key", "user-1", results)

        self.assertIsNone(clip_cache.get("cache-key", "user-2"))

    def test_expired_cache_entry_is_not_returned(self):
        results = [{"storage_path": "user/job/clip.mp4"}]
        clip_cache.put("cache-key", "user-1", results)
        self.client.rows["cache-key"]["expires_at"] = "2000-01-01T00:00:00+00:00"

        self.assertIsNone(clip_cache.get("cache-key", "user-1"))
        self.assertNotIn("cache-key", self.client.rows)

    def test_expired_entries_can_be_cleaned(self):
        results = [{"storage_path": "user/job/clip.mp4"}]
        clip_cache.put("old", "user-1", results)
        clip_cache.put("new", "user-1", results)
        self.client.rows["old"]["expires_at"] = "2000-01-01T00:00:00+00:00"
        self.client.rows["new"]["expires_at"] = "2999-01-01T00:00:00+00:00"

        deleted = clip_cache.delete_expired()

        self.assertEqual(deleted, 1)
        self.assertNotIn("old", self.client.rows)
        self.assertIn("new", self.client.rows)

    def test_malformed_results_are_not_returned(self):
        clip_cache.put("cache-key", "user-1", {"not": "a list"})

        self.assertIsNone(clip_cache.get("cache-key", "user-1"))


if __name__ == "__main__":
    unittest.main()
