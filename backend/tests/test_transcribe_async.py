import asyncio
import os
import sys
import unittest
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, call, mock_open, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock supabase dependencies if not installed in local environment
_MOCKED_MODULES = [
    "supabase",
    "supabase_client",
]
for mod in _MOCKED_MODULES:
    sys.modules.setdefault(mod, mock.MagicMock())

from fastapi import BackgroundTasks
from captions import api


class TranscribeAsyncTest(unittest.TestCase):
    def test_endpoint_queues_immediately(self):
        """Ensure transcribe_endpoint does not call download_to_file synchronously."""
        bg = BackgroundTasks()
        user = {"user_id": "user123", "email": "user@example.com"}

        with patch.object(api.storage, "create_job", return_value="job_abc") as mock_create_job, \
             patch.object(api.storage, "download_to_file") as mock_sync_download:

            res = asyncio.run(api.transcribe_endpoint(
                background_tasks=bg,
                file=None,
                storage_path="user123/sources/job_abc/video.mp4",
                language=None,
                hinglish=True,
                user=user,
            ))

            self.assertEqual(res, {"job_id": "job_abc", "status": "queued"})
            mock_create_job.assert_called_once_with("user123", "transcribe", source_path="user123/sources/job_abc/video.mp4")
            mock_sync_download.assert_not_called()  # MUST NOT be called in request handler

            # Check background task was queued with source_path
            self.assertEqual(len(bg.tasks), 1)
            task = bg.tasks[0]
            self.assertEqual(task.func, api._transcribe_task)
            self.assertEqual(task.kwargs.get("source_path"), "user123/sources/job_abc/video.mp4")
            self.assertTrue(task.kwargs.get("cleanup_local"))

    def test_endpoint_file_precedence_over_storage_path(self):
        """Ensure direct-upload file takes precedence over storage_path when both are supplied,
        while retaining storage_path on the job row for cleanup."""
        bg = BackgroundTasks()
        user = {"user_id": "user123", "email": "user@example.com"}
        mock_file = MagicMock()
        mock_file.read = AsyncMock(side_effect=[b"sample_audio_content", b""])

        with patch.object(api.storage, "create_job", return_value="job_xyz") as mock_create_job, \
             patch("builtins.open", mock_open()):

            res = asyncio.run(api.transcribe_endpoint(
                background_tasks=bg,
                file=mock_file,
                storage_path="user123/sources/job_xyz/video.mp4",
                language=None,
                hinglish=True,
                user=user,
            ))

            self.assertEqual(res, {"job_id": "job_xyz", "status": "queued"})
            # Retain original storage_path in caption_jobs for lifecycle cleanup
            mock_create_job.assert_called_once_with("user123", "transcribe", source_path="user123/sources/job_xyz/video.mp4")

            # But pass source_path=None to background task so the direct-upload file isn't overwritten
            self.assertEqual(len(bg.tasks), 1)
            task = bg.tasks[0]
            self.assertEqual(task.func, api._transcribe_task)
            self.assertIsNone(task.kwargs.get("source_path"))
            self.assertTrue(task.kwargs.get("cleanup_local"))

    def test_transcribe_task_downloads_and_handles_error(self):
        """Ensure _transcribe_task marks transcribing first, downloads when source_path is present, and marks failed on error."""
        with patch.object(api.storage, "download_to_file", side_effect=RuntimeError("Storage connection failed")) as mock_dl, \
             patch.object(api.storage, "update_job") as mock_update:

            api._transcribe_task(
                job_id="job_abc",
                local_path="captions_output/job_abc_source",
                language=None,
                hinglish=True,
                cleanup_local=False,
                source_path="user123/sources/job_abc/video.mp4",
            )

            mock_dl.assert_called_once_with("user123/sources/job_abc/video.mp4", "captions_output/job_abc_source")
            self.assertEqual(mock_update.call_args_list, [
                call("job_abc", status="transcribing"),
                call("job_abc", status="failed", error="Storage connection failed"),
            ])

    def test_transcribe_task_success(self):
        """Ensure _transcribe_task downloads, transcribes, probes, and completes."""
        with patch.object(api.storage, "download_to_file") as mock_dl, \
             patch.object(api.storage, "update_job") as mock_update, \
             patch.object(api.transcribe, "transcribe_video", return_value={"words": [{"start": 0, "end": 1, "text": "hi"}]}), \
             patch.object(api.render, "probe_video", return_value={"duration": 1.0}), \
             patch("os.remove") as mock_remove:

            api._transcribe_task(
                job_id="job_abc",
                local_path="captions_output/job_abc_source",
                language=None,
                hinglish=False,
                cleanup_local=True,
                source_path="user123/sources/job_abc/video.mp4",
            )

            mock_dl.assert_called_once_with("user123/sources/job_abc/video.mp4", "captions_output/job_abc_source")
            # Status should transition to transcribing then completed
            self.assertTrue(any(call.kwargs.get("status") == "transcribing" for call in mock_update.call_args_list))
            self.assertTrue(any(call.kwargs.get("status") == "completed" for call in mock_update.call_args_list))
            mock_remove.assert_called_once_with("captions_output/job_abc_source")


if __name__ == "__main__":
    unittest.main()
