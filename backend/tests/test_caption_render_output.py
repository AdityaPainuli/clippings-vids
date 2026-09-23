import importlib
import os
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


supabase_client_stub = types.ModuleType("supabase_client")
supabase_client_stub.supabase = object()
supabase_client_stub.SUPABASE_URL = "https://example.supabase.co"
supabase_client_stub.SUPABASE_SERVICE_KEY = "test-service-key"
sys.modules.setdefault("supabase_client", supabase_client_stub)


class _FakeStorage:
    def __init__(self, upload_error=None, sign_error=None):
        self.upload_error = upload_error
        self.sign_error = sign_error
        self.updated = []

    def upload_output(self, local_path, user_id, job_id):
        if self.upload_error:
            raise self.upload_error
        return f"{user_id}/outputs/{job_id}/{os.path.basename(local_path)}"

    def signed_download_url(self, output_path):
        if self.sign_error:
            raise self.sign_error
        return f"https://example.test/{output_path}"

    def update_job(self, job_id, **fields):
        self.updated.append((job_id, fields))


class _FakeNotify:
    def __init__(self):
        self.completed = []
        self.failed = []

    def notify_completed(self, email, job_id, filename, download_url):
        self.completed.append((email, job_id, filename, download_url))

    def notify_failed(self, email, job_id, error):
        self.failed.append((email, job_id, error))


class RenderOutputDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.api = importlib.import_module("captions.api")

    def _run_render_task(self, storage, notify, work_dir):
        def fake_build_ass(*args, **kwargs):
            return "[Script Info]\n"

        with patch.object(self.api, "WORK_DIR", work_dir), \
             patch.object(self.api.storage, "update_job", side_effect=storage.update_job), \
             patch.object(self.api.storage, "upload_output", side_effect=storage.upload_output), \
             patch.object(self.api.storage, "signed_download_url", side_effect=storage.signed_download_url), \
             patch.object(self.api.notify, "notify_completed", side_effect=notify.notify_completed), \
             patch.object(self.api.notify, "notify_failed", side_effect=notify.notify_failed), \
             patch.object(self.api.engine, "build_ass", side_effect=fake_build_ass):
            self.api._render_task(
                job_id="job-1",
                user_id="user-1",
                email="user@example.com",
                source_path=None,
                words=[{"start": 0.0, "end": 1.0, "text": "hello"}],
                style=types.SimpleNamespace(words_per_line=3),
                export="ass",
                text_key="text",
                video_info={"width": 1080, "height": 1920, "duration": 1.0, "fps": 30},
            )

    def test_successful_output_upload_persists_output_path(self):
        storage = _FakeStorage()
        notify = _FakeNotify()

        with tempfile.TemporaryDirectory() as work_dir:
            self._run_render_task(storage, notify, work_dir)

        completed = [fields for _, fields in storage.updated if fields.get("status") == "completed"]
        self.assertEqual(len(completed), 1)
        self.assertIn("output_path", completed[0])
        self.assertTrue(completed[0]["output_path"].endswith("job-1.ass"))
        self.assertEqual(len(notify.completed), 1)
        self.assertEqual(len(notify.failed), 0)

    def test_failed_output_upload_marks_job_failed(self):
        storage = _FakeStorage(upload_error=RuntimeError("storage unavailable"))
        notify = _FakeNotify()

        with tempfile.TemporaryDirectory() as work_dir:
            self._run_render_task(storage, notify, work_dir)

        self.assertFalse(
            any(fields.get("status") == "completed" for _, fields in storage.updated)
        )
        failed = [fields for _, fields in storage.updated if fields.get("status") == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertIn("storage unavailable", failed[0]["error"])
        self.assertEqual(len(notify.completed), 0)
        self.assertEqual(len(notify.failed), 1)

    def test_failed_output_upload_removes_local_output(self):
        storage = _FakeStorage(upload_error=RuntimeError("storage unavailable"))
        notify = _FakeNotify()

        with tempfile.TemporaryDirectory() as work_dir:
            self._run_render_task(storage, notify, work_dir)
            self.assertFalse(
                any(name.endswith(".ass") for name in os.listdir(work_dir)),
                os.listdir(work_dir),
            )

    def test_failed_download_url_signing_preserves_uploaded_output_path(self):
        storage = _FakeStorage(sign_error=RuntimeError("signing unavailable"))
        notify = _FakeNotify()

        with tempfile.TemporaryDirectory() as work_dir:
            self._run_render_task(storage, notify, work_dir)

        failed = [fields for _, fields in storage.updated if fields.get("status") == "failed"]
        self.assertEqual(len(failed), 1)
        self.assertEqual(
            failed[0]["output_path"],
            "user-1/outputs/job-1/job-1.ass",
        )
        self.assertEqual(failed[0]["filename"], "job-1.ass")
        self.assertEqual(len(notify.completed), 0)
        self.assertEqual(len(notify.failed), 1)

    def test_local_cleanup_failure_does_not_change_completed_job(self):
        storage = _FakeStorage()
        notify = _FakeNotify()

        with tempfile.TemporaryDirectory() as work_dir:
            with patch.object(self.api.os, "remove", side_effect=OSError("busy file")):
                self._run_render_task(storage, notify, work_dir)

        completed = [fields for _, fields in storage.updated if fields.get("status") == "completed"]
        failed = [fields for _, fields in storage.updated if fields.get("status") == "failed"]
        self.assertEqual(len(completed), 1)
        self.assertEqual(len(failed), 0)
        self.assertEqual(len(notify.completed), 1)
        self.assertEqual(len(notify.failed), 0)


if __name__ == "__main__":
    unittest.main()
