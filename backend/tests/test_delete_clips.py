import asyncio
import importlib
import sys
import unittest
from unittest import mock

try:
    from tests.test_main_job_race import _install_main_import_stubs
except ImportError:
    from test_main_job_race import _install_main_import_stubs


class DeleteClipsTest(unittest.TestCase):
    def setUp(self):
        _install_main_import_stubs()
        self.main = importlib.import_module("main")
        self.main.jobs.clear()
        self.main._clip_cache.clear()

    def _delete(self, job_id, user_id="user-1"):
        return asyncio.run(
            self.main.delete_clips(job_id, {"user_id": user_id}))

    def test_deletes_metadata_after_the_in_memory_job_is_gone(self):
        path = "user-1/job-1/clip.mp4"
        with mock.patch.object(self.main, "delete_clip_records",
                               return_value=[path]) as delete:
            result = self._delete("job-1")
        delete.assert_called_once_with("job-1", "user-1")
        self.assertEqual(result, {"deleted_clips": 1})

    def test_other_users_job_is_forbidden(self):
        self.main.jobs["job-1"] = {"user_id": "someone-else", "results": []}
        with mock.patch.object(self.main, "delete_clip_records") as delete:
            with self.assertRaises(Exception) as ctx:
                self._delete("job-1")
        self.assertEqual(ctx.exception.status_code, 403)
        delete.assert_not_called()
        self.assertIn("job-1", self.main.jobs)

    def test_missing_job_and_metadata_is_404(self):
        with mock.patch.object(self.main, "delete_clip_records",
                               return_value=[]):
            with self.assertRaises(Exception) as ctx:
                self._delete("missing")
        self.assertEqual(ctx.exception.status_code, 404)

    def test_cache_is_evicted_by_key_and_by_path(self):
        path = "user-1/job-1/clip.mp4"
        self.main.jobs["job-1"] = {
            "user_id": "user-1", "cache_key": "by-key", "results": []}
        self.main._clip_cache["by-key"] = [{"storage_path": "other.mp4"}]
        self.main._clip_cache["by-path"] = [{"storage_path": path}]
        self.main._clip_cache["unrelated"] = [{"storage_path": "keep.mp4"}]

        with mock.patch.object(self.main, "delete_clip_records",
                               return_value=[path]):
            self._delete("job-1")

        self.assertNotIn("by-key", self.main._clip_cache)
        self.assertNotIn("by-path", self.main._clip_cache)
        self.assertIn("unrelated", self.main._clip_cache)
        self.assertNotIn("job-1", self.main.jobs)

    def test_in_memory_results_are_a_fallback_when_metadata_is_empty(self):
        path = "user-1/job-1/partial.mp4"
        self.main.jobs["job-1"] = {
            "user_id": "user-1", "results": [{"storage_path": path}]}
        delete_paths = mock.Mock()
        sys.modules["supabase_client"]._delete_paths = delete_paths

        with mock.patch.object(self.main, "delete_clip_records",
                               return_value=[]):
            result = self._delete("job-1")

        delete_paths.assert_called_once_with([path])
        self.assertEqual(result, {"deleted_clips": 1})


if __name__ == "__main__":
    unittest.main()
