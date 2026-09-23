import importlib.util
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch


# Force the production module to use a deterministic test double even if another
# test imported the real Supabase client earlier in the test process.
supabase_client_stub = types.ModuleType("supabase_client")
supabase_client_stub.supabase = None
sys.modules["supabase_client"] = supabase_client_stub
sys.modules.pop("sse_event_store", None)

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
            next_id = max(
                (row["id"] for row in rows if "id" in row),
                default=0,
            ) + 1
            if self.table == sse_event_store.EVENTS_TABLE:
                row = {
                    "id": next_id,
                    "job_id": self.payload["job_id"],
                    "user_id": self.payload["user_id"],
                    "event_data": self.payload["event_data"],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            else:
                row = {
                    "token_hash": self.payload["token_hash"],
                    "user_id": self.payload["user_id"],
                    "expires_at": self.payload["expires_at"],
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            rows.append(row)
            return FakeResponse([row])

        matching = list(rows)
        for key, operator, value in self.filters:
            if operator == "eq":
                matching = [row for row in matching if row[key] == value]
            elif operator == "gt":
                if key == "id":
                    matching = [row for row in matching if row[key] > value]
                else:
                    matching = [
                        row for row in matching
                        if datetime.fromisoformat(
                            row[key].replace("Z", "+00:00")
                        ) > datetime.fromisoformat(
                            value.replace("Z", "+00:00")
                        )
                    ]
            elif operator == "lt":
                matching = [
                    row for row in matching
                    if datetime.fromisoformat(
                        row[key].replace("Z", "+00:00")
                    ) < datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    )
                ]

        if hasattr(self, "order_key"):
            matching.sort(
                key=lambda row: row[self.order_key],
                reverse=self.descending,
            )

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


def _load_worker_module(name):
    store_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "sse_event_store.py",
    )
    spec = importlib.util.spec_from_file_location(name, store_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class DurableSSEEventStoreTests(unittest.TestCase):
    def setUp(self):
        self.supabase = FakeSupabase()
        supabase_client_stub.supabase = self.supabase
        self.patch = patch.object(sse_event_store, "supabase", self.supabase)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    def test_publish_assigns_monotonic_ids(self):
        first = sse_event_store.publish(
            "job-1", "user-1", {"status": "analyzing"}
        )
        second = sse_event_store.publish(
            "job-1", "user-1", {"status": "clipping"}
        )

        self.assertLess(first, second)

    def test_events_are_scoped_to_job_and_user(self):
        first = sse_event_store.publish(
            "job-1", "user-1", {"status": "analyzing"}
        )
        sse_event_store.publish(
            "job-1", "user-2", {"status": "private"}
        )
        sse_event_store.publish(
            "job-2", "user-1", {"status": "uploading"}
        )

        events = sse_event_store.events_after("job-1", "user-1", first - 1)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_data"]["status"], "analyzing")

    def test_latest_event_is_scoped_to_user_owned_job(self):
        event_id = sse_event_store.publish(
            "job-1", "user-1", {"status": "complete"}
        )
        sse_event_store.publish(
            "job-1", "user-2", {"status": "private"}
        )

        self.assertEqual(
            sse_event_store.latest_event("job-1", "user-1")["id"],
            event_id,
        )
        self.assertIsNone(
            sse_event_store.latest_event("job-1", "user-3")
        )

    def test_cleanup_older_than_removes_expired_events(self):
        sse_event_store.publish(
            "job-1", "user-1", {"status": "old"}
        )
        rows = self.supabase.database[sse_event_store.EVENTS_TABLE]
        rows[0]["created_at"] = (
            datetime.now(timezone.utc) - timedelta(hours=3)
        ).isoformat()
        sse_event_store.publish(
            "job-1", "user-1", {"status": "new"}
        )

        deleted = sse_event_store.cleanup_older_than(3600)

        self.assertEqual(deleted, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_data"]["status"], "new")

    def test_publish_error_contains_table_and_job_context(self):
        with patch.object(
            sse_event_store.supabase,
            "table",
            side_effect=RuntimeError("database unavailable"),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                r"Failed to insert into sse_events for job job-42",
            ):
                sse_event_store.publish(
                    "job-42", "user-1", {"status": "queued"}
                )

    def test_independent_worker_modules_share_event_history(self):
        first_worker = _load_worker_module("sse_event_store_worker_a")
        second_worker = _load_worker_module("sse_event_store_worker_b")

        event_id = first_worker.publish(
            "job-1", "user-1", {"status": "uploading"}
        )
        events = second_worker.events_after(
            "job-1", "user-1", event_id - 1
        )

        self.assertEqual(events[0]["id"], event_id)
        self.assertEqual(
            events[0]["event_data"]["status"],
            "uploading",
        )

    def test_stream_tokens_are_single_use_and_cross_worker(self):
        first_worker = _load_worker_module("sse_event_store_worker_token_a")
        second_worker = _load_worker_module("sse_event_store_worker_token_b")

        token = first_worker.issue_stream_token("user-1", 300)

        self.assertEqual(second_worker.consume_stream_token(token), "user-1")
        self.assertIsNone(first_worker.consume_stream_token(token))

    def test_expired_stream_tokens_are_rejected_and_cleanup_is_scoped(self):
        token = sse_event_store.issue_stream_token("user-1", 1)
        rows = self.supabase.database[sse_event_store.TOKENS_TABLE]
        rows[0]["expires_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).isoformat()

        self.assertIsNone(sse_event_store.consume_stream_token(token))
        self.assertEqual(
            sse_event_store.cleanup_expired_stream_tokens(),
            1,
        )


if __name__ == "__main__":
    unittest.main()
