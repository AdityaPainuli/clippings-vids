import asyncio
import importlib
import unittest

try:
    from test_main_job_race import _install_main_import_stubs
except ImportError:  # run as tests.test_status_allowlist
    from tests.test_main_job_race import _install_main_import_stubs


class StatusAllowlistTests(unittest.TestCase):
    def setUp(self):
        _install_main_import_stubs()
        self.main = importlib.import_module("main")
        self.main.jobs.clear()

    def tearDown(self):
        self.main.jobs.clear()

    def _status(self, job_id, user_id="user-1"):
        return asyncio.run(self.main.get_status(job_id, user={"user_id": user_id}))

    def test_status_hides_server_side_fields(self):
        self.main.jobs["job-1"] = {
            "status": "downloading",
            "url": "https://youtu.be/abc",
            "results": None,
            "error": None,
            "created_at": 1700000000.0,
            "user_id": "user-1",
            "cached": False,
            "video_path": "/srv/app/downloads/abc.mp4",
        }

        body = self._status("job-1")

        self.assertNotIn("video_path", body)
        self.assertNotIn("user_id", body)
        self.assertEqual(body["status"], "downloading")

    def test_status_keeps_every_field_the_frontend_reads(self):
        results = [{"filename": "clip_1.mp4", "url": "https://example.test/clip_1.mp4"}]
        failed = [{"filename": "clip_2.mp4", "error": "render failed"}]
        self.main.jobs["job-1"] = {
            "status": "completed",
            "url": "https://youtu.be/abc",
            "results": results,
            "error": None,
            "warnings": "1 of 2 clips failed",
            "failed_clips": failed,
            "created_at": 1700000000.0,
            "user_id": "user-1",
            "cached": True,
            "video_path": "/srv/app/downloads/abc.mp4",
        }

        body = self._status("job-1")

        self.assertEqual(
            body,
            {
                "status": "completed",
                "url": "https://youtu.be/abc",
                "results": results,
                "error": None,
                "warnings": "1 of 2 clips failed",
                "failed_clips": failed,
                "created_at": 1700000000.0,
                "cached": True,
            },
        )

    def test_status_omits_optional_fields_that_were_never_set(self):
        self.main.jobs["job-1"] = {"status": "queued", "user_id": "user-1"}

        self.assertEqual(self._status("job-1"), {"status": "queued"})

    def test_status_does_not_modify_the_stored_job(self):
        self.main.jobs["job-1"] = {
            "status": "downloading",
            "user_id": "user-1",
            "video_path": "/srv/app/downloads/abc.mp4",
        }

        self._status("job-1")

        self.assertEqual(self.main.jobs["job-1"]["video_path"], "/srv/app/downloads/abc.mp4")
        self.assertEqual(self.main.jobs["job-1"]["user_id"], "user-1")

    def test_status_still_rejects_unknown_and_foreign_jobs(self):
        self.main.jobs["job-1"] = {"status": "queued", "user_id": "user-2"}

        with self.assertRaises(self.main.HTTPException) as unknown:
            self._status("missing")
        with self.assertRaises(self.main.HTTPException) as foreign:
            self._status("job-1", user_id="user-1")

        self.assertEqual(unknown.exception.status_code, 404)
        self.assertEqual(foreign.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()
