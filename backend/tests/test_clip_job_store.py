import importlib
import sys
import types
import unittest


class _Response:
    def __init__(self, data):
        self.data = data


class _FakeTable:
    def __init__(self, rows):
        self.rows = rows
        self.operation = None
        self.filters = []
        self.payload = None

    def select(self, *_):
        self.operation = "select"
        return self

    def update(self, payload):
        self.operation = "update"
        self.payload = payload
        return self

    def eq(self, key, value):
        self.filters.append((key, "eq", value))
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args):
        return self

    def execute(self):
        if self.operation == "select":
            rows = [dict(row) for row in self.rows if self._matches(row)]
            return _Response(rows[:1])

        updated = []
        for row in self.rows:
            if self._matches(row):
                row.update(self.payload)
                updated.append(dict(row))
                break
        return _Response(updated)

    def _matches(self, row):
        for key, operator, value in self.filters:
            if operator == "eq" and row.get(key) != value:
                return False
        return True


class _FakeSupabase:
    def __init__(self, rows):
        self.rows = rows

    def table(self, _name):
        return _FakeTable(self.rows)


class ClaimNextJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fake_module = types.ModuleType("supabase_client")
        fake_module.SUPABASE_SERVICE_KEY = "test-key"
        fake_module.SUPABASE_URL = "https://example.supabase.co"
        fake_module.supabase = None
        sys.modules["supabase_client"] = fake_module

        cls.module = importlib.import_module("clip_job_store")

    def test_only_one_worker_can_claim_a_queued_job(self):
        rows = [{
            "id": "job-1",
            "status": "queued",
            "worker_id": None,
            "lease_until": None,
        }]
        self.module.supabase = _FakeSupabase(rows)

        first = self.module.claim_next_job("worker-a", 300)
        second = self.module.claim_next_job("worker-b", 300)

        self.assertEqual(first["id"], "job-1")
        self.assertEqual(first["status"], "downloading")
        self.assertEqual(first["worker_id"], "worker-a")
        self.assertIsNone(second)


if __name__ == "__main__":
    unittest.main()
