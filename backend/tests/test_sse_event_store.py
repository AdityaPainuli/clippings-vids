import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


# Keep these unit tests independent of the optional Supabase SDK. The production
# module imports the repository's supabase_client, but the tests only need to
# replace its client with the in-memory fake below.
supabase_client_stub = types.ModuleType("supabase_client")
supabase_client_stub.supabase = object()
sys.modules.setdefault("supabase_client", supabase_client_stub)

import sse_event_store


class FakeResponse:
    def __init__(self, data=None):
        self.data = data or []


class FakeQuery:
    def __init__(self, database, table, operation="select"):
        self.database = database
        self.table = table
        self.operation = operation
        self.filters = []
        self.payload = None
        self.limit_value = None
        self.descending = False

    def select(self, _columns):
        return self

    def insert(self, payload):
        self.operation = "insert"
        self.payload = payload
        return self

    def delete(self):
        self.operation = "delete"
        return self

    def eq(self, key, value):
        self.filters.append((key, "eq", value))
        return self

    def gt(self, key, value):
        self.filters.append((key, "gt", value))
        return self

    def lt(self, key, value):
        self.filters.append((key, "lt", value))
        return self

    def order(self, key, desc=False):
        self.order_key = key
        self.descending = desc
        return self

    def limit(self, value):
        self.limit_value = value
        return self

    def execute(self):
        rows = self.database.setdefault(self.table, [])

        if self.operation == "insert":
            next_id = max((row["id"] for row in rows), default=0) + 1
            row = {
                "id": next_id,
                "job_id": self.payload["job_id"],
                "event_data": self.payload["event_data"],
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            rows.append(row)
            return FakeResponse([row])

        matching = list(rows)
        for key, operator, value in self.filters:
            if operator == "eq":
                matching = [row for row in matching if row[key] == value]
            elif operator == "gt":
                matching = [row for row in matching if row[key] > value]
            elif operator == "lt":
                matching = [
                    row for row in matching
                    if datetime.fromisoformat(row[key].replace("Z", "+00:00"))
                    < datetime.fromisoformat(value.replace("Z", "+00:00"))
                ]

        if hasattr(self, "order_key"):
            matching.sort(key=lambda row: row[self.order_key], reverse=self.descending)
        if self.limit_value is not None:
            matching = matching[: self.limit_value]

        if self.operation == "delete":
            for row in matching:
                rows.remove(row)
            return FakeResponse(matching)

        return FakeResponse(matching)


class FakeSupabase:
    def __init__(self):
        self.database = {}

    def table(self, name):
        return FakeQuery(self.database, name)


class DurableSSEEventStoreTests(unittest.TestCase):
    def setUp(self):
        self.supabase = FakeSupabase()
        self.patch = patch.object(sse_event_store, "supabase", self.supabase)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_publish_assigns_monotonic_ids(self):
        first = sse_event_store.publish("job-1", {"status": "analyzing"})
        second = sse_event_store.publish("job-1", {"status": "clipping"})

        self.assertLess(first, second)

    def test_events_after_returns_only_newer_events(self):
        first = sse_event_store.publish("job-1", {"status": "analyzing"})
        sse_event_store.publish("job-1", {"status": "clipping"})
        sse_event_store.publish("job-2", {"status": "uploading"})

        events = sse_event_store.events_after("job-1", first)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_data"]["status"], "clipping")

    def test_latest_event_id_is_scoped_to_job(self):
        job_one_id = sse_event_store.publish("job-1", {"status": "complete"})
        job_two_id = sse_event_store.publish("job-2", {"status": "complete"})

        self.assertEqual(sse_event_store.latest_event_id("job-1"), job_one_id)
        self.assertEqual(sse_event_store.latest_event_id("job-2"), job_two_id)

    def test_cleanup_older_than_removes_expired_events(self):
        sse_event_store.publish("job-1", {"status": "old"})
        rows = self.supabase.database[sse_event_store.TABLE]
        rows[0]["created_at"] = (
            datetime.now(timezone.utc) - timedelta(hours=3)
        ).isoformat()
        sse_event_store.publish("job-1", {"status": "new"})

        deleted = sse_event_store.cleanup_older_than(3600)

        self.assertEqual(deleted, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_data"]["status"], "new")

    def test_independent_store_instances_share_event_history(self):
        first_worker = sse_event_store
        second_worker = sse_event_store

        event_id = first_worker.publish("job-1", {"status": "uploading"})

        events = second_worker.events_after("job-1", event_id - 1)

        self.assertEqual(events[0]["id"], event_id)
        self.assertEqual(events[0]["event_data"]["status"], "uploading")


if __name__ == "__main__":
    unittest.main()
