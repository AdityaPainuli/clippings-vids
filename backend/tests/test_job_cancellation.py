import unittest
from unittest.mock import Mock

from captions import storage


class FakeTable:
    def __init__(self, rows=None, update_result=None):
        self.rows = rows or []
        self.update_result = update_result
        self.operation = None
        self.filters = {}
        self.update_payload = None

    def select(self, *_args):
        self.operation = "select"
        return self

    def update(self, payload):
        self.operation = "update"
        self.update_payload = payload
        return self

    def insert(self, *_args):
        return self

    def delete(self):
        return self

    def eq(self, key, value):
        self.filters[key] = value
        return self

    def in_(self, key, values):
        self.filters[key] = values
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args):
        return self

    def execute(self):
        if self.operation == "select":
            return Mock(data=self.rows)
        return Mock(data=self.update_result if self.update_result is not None else [])


class FakeSupabase:
    def __init__(self, row, update_result=None):
        self.row = row
        self.table_instance = FakeTable(
            rows=[row] if row else [],
            update_result=update_result,
        )

    def table(self, _name):
        return self.table_instance


class JobCancellationTests(unittest.TestCase):
    def test_owner_can_cancel_active_job(self):
        row = {
            "id": "job-1",
            "user_id": "user-1",
            "status": "rendering",
        }
        fake = FakeSupabase({**row}, update_result=[{"id": "job-1", "status": "cancelled"}])
        original = storage.supabase
        storage.supabase = fake
        try:
            self.assertEqual(storage.request_cancel("job-1", "user-1"), "cancelled")
            self.assertEqual(fake.table_instance.update_payload["status"], "cancelled")
            self.assertEqual(fake.table_instance.filters["user_id"], "user-1")
            self.assertEqual(fake.table_instance.filters["status"], ["queued", "transcribing", "romanizing", "rendering"])
        finally:
            storage.supabase = original

    def test_other_user_cannot_cancel_job(self):
        row = {"id": "job-1", "user_id": "owner", "status": "queued"}
        fake = FakeSupabase(row)
        original = storage.supabase
        storage.supabase = fake
        try:
            self.assertEqual(storage.request_cancel("job-1", "attacker"), "forbidden")
            self.assertIsNone(fake.table_instance.update_payload)
        finally:
            storage.supabase = original

    def test_completed_job_is_not_cancellable(self):
        row = {"id": "job-1", "user_id": "owner", "status": "completed"}
        fake = FakeSupabase(row)
        original = storage.supabase
        storage.supabase = fake
        try:
            self.assertEqual(storage.request_cancel("job-1", "owner"), "completed")
            self.assertIsNone(fake.table_instance.update_payload)
        finally:
            storage.supabase = original

    def test_worker_transition_is_rejected_when_cancellation_wins(self):
        fake = FakeSupabase(
            {"id": "job-1", "user_id": "owner", "status": "cancelled"},
            update_result=[],
        )
        original = storage.supabase
        storage.supabase = fake
        try:
            self.assertFalse(
                storage.update_job("job-1", status="completed", filename="out.mp4")
            )
            self.assertEqual(
                fake.table_instance.filters["status"],
                ["queued", "transcribing", "romanizing", "rendering"],
            )
            self.assertEqual(fake.table_instance.update_payload["status"], "completed")
        finally:
            storage.supabase = original


if __name__ == "__main__":
    unittest.main()
