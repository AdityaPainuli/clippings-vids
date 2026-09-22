#!/usr/bin/env python3
"""
Check that POST /captions/transcribe queues storage downloads asynchronously
in _transcribe_task instead of blocking the request handler synchronously.

    python scripts/check_transcribe_async.py
"""

import asyncio
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock supabase dependencies if not installed in local environment
for mod in ("supabase", "supabase_client"):
    if mod not in sys.modules:
        m = MagicMock()
        sys.modules[mod] = m

from fastapi import BackgroundTasks
from captions import api


def test_endpoint_queues_immediately():
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

        assert res == {"job_id": "job_abc", "status": "queued"}, f"Unexpected response: {res}"
        mock_create_job.assert_called_once_with("user123", "transcribe", source_path="user123/sources/job_abc/video.mp4")
        mock_sync_download.assert_not_called()  # MUST NOT be called in request handler

        # Check background task was queued with source_path
        assert len(bg.tasks) == 1
        task = bg.tasks[0]
        assert task.func == api._transcribe_task
        assert task.kwargs.get("source_path") == "user123/sources/job_abc/video.mp4"
        assert task.kwargs.get("cleanup_local") is True


def test_transcribe_task_downloads_and_handles_error():
    """Ensure _transcribe_task downloads when source_path is present and marks failed on error."""
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
        mock_update.assert_called_once_with("job_abc", status="failed", error="Storage connection failed")


def test_transcribe_task_success():
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
        assert any(call.kwargs.get("status") == "transcribing" for call in mock_update.call_args_list)
        assert any(call.kwargs.get("status") == "completed" for call in mock_update.call_args_list)
        mock_remove.assert_called_once_with("captions_output/job_abc_source")


def main():
    test_endpoint_queues_immediately()
    test_transcribe_task_downloads_and_handles_error()
    test_transcribe_task_success()
    print("PASS — transcribe endpoint queues immediately without blocking event loop, background task downloads and handles errors.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
