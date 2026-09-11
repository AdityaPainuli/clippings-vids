import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from job_store import InMemoryJobStore  # noqa: E402


class InMemoryJobStoreTest(unittest.TestCase):
    def test_set_get_delete(self):
        store = InMemoryJobStore(ttl_seconds=60)
        record = {"status": "queued", "created_at": 100.0}

        store.set("job-1", record)

        self.assertIs(store.get("job-1"), record)
        self.assertEqual(store.all(), {"job-1": record})

        store.delete("job-1")

        self.assertIsNone(store.get("job-1"))
        self.assertEqual(store.all(), {})

    def test_cleanup_only_removes_stale_records_from_that_store(self):
        clipper_jobs = InMemoryJobStore(ttl_seconds=10)
        caption_jobs = InMemoryJobStore(ttl_seconds=100)
        clipper_jobs.set("old", {"status": "completed", "created_at": 89.0})
        clipper_jobs.set("fresh", {"status": "queued", "created_at": 95.0})
        caption_jobs.set("old", {"status": "completed", "created_at": 50.0})

        removed = clipper_jobs.cleanup(now=100.0)

        self.assertEqual(removed, ["old"])
        self.assertIsNone(clipper_jobs.get("old"))
        self.assertIsNotNone(clipper_jobs.get("fresh"))
        self.assertIsNotNone(caption_jobs.get("old"))

    def test_same_job_id_does_not_collide_between_domains(self):
        clipper_jobs = InMemoryJobStore(ttl_seconds=10)
        caption_jobs = InMemoryJobStore(ttl_seconds=10)
        clipper_jobs.set("42", {"status": "clipping", "created_at": 100.0})
        caption_jobs.set("42", {"status": "transcribing", "created_at": 100.0})

        clipper_jobs.delete("42")

        self.assertIsNone(clipper_jobs.get("42"))
        self.assertEqual(caption_jobs.get("42"), {"status": "transcribing", "created_at": 100.0})


if __name__ == "__main__":
    unittest.main()
