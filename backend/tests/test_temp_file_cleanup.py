import asyncio
import importlib
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from test_main_job_race import _install_main_import_stubs


def _touch(path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"\x00")
    return path


class TempFileCleanupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_main_import_stubs()
        cls.main = importlib.import_module("main")

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.upload_dir = os.path.join(self.tmpdir.name, "uploads")
        self.output_dir = os.path.join(self.tmpdir.name, "output")
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        self.orig_upload = self.main.UPLOAD_DIR
        self.orig_output = self.main.OUTPUT_DIR
        self.main.UPLOAD_DIR = self.upload_dir
        self.main.OUTPUT_DIR = self.output_dir
        self.main.jobs.clear()

    def tearDown(self):
        self.main.jobs.clear()
        self.main.UPLOAD_DIR = self.orig_upload
        self.main.OUTPUT_DIR = self.orig_output
        self.tmpdir.cleanup()

    def test_analysis_failure_cleans_up_and_protects_shared_active(self):
        """Case A: Analysis failure cleans video and .vtt while protecting shared active files."""
        video = os.path.join(self.upload_dir, "vid_a.mp4")
        vtt = os.path.join(self.upload_dir, "vid_a.en.vtt")
        shared_video = os.path.join(self.upload_dir, "shared.mp4")
        shared_vtt = os.path.join(self.upload_dir, "shared.en.vtt")
        _touch(video)
        _touch(vtt)
        _touch(shared_video)
        _touch(shared_vtt)

        # Another active job references shared_video
        self.main.jobs["job-active-shared"] = {
            "status": "clipping",
            "video_path": shared_video,
            "created_at": time.time(),
            "user_id": "u2",
        }

        job_1 = "job-fail-1"
        self.main.jobs[job_1] = {
            "status": "queued",
            "video_path": video,
            "created_at": time.time(),
            "user_id": "u1",
        }

        with patch.object(self.main.clipper, "analyze_video", side_effect=RuntimeError("Analysis crashed")):
            asyncio.run(self.main.process_video_task(job_1, video, None, "u1"))

        # Job 1 failed and its files are cleaned up
        self.assertEqual(self.main.jobs[job_1]["status"], "failed")
        self.assertEqual(self.main.jobs[job_1]["error"], "Analysis crashed")
        self.assertFalse(os.path.exists(video))
        self.assertFalse(os.path.exists(vtt))

        # Shared active files are protected from cleanup
        self.assertTrue(os.path.exists(shared_video))
        self.assertTrue(os.path.exists(shared_vtt))

    def test_upload_failure_cleans_clip_raw_and_ass(self):
        """Case B: Upload failure cleans local clip, _raw, and .ass files, including failed renders."""
        video = os.path.join(self.upload_dir, "vid_b.mp4")
        clip = os.path.join(self.output_dir, "clip_b.mp4")
        raw = os.path.join(self.output_dir, "clip_b_raw.mp4")
        ass = os.path.join(self.output_dir, "clip_b.ass")

        failed_render_clip = os.path.join(self.output_dir, "clip_failed_render.mp4")
        failed_render_raw = os.path.join(self.output_dir, "clip_failed_render_raw.mp4")
        failed_render_ass = os.path.join(self.output_dir, "clip_failed_render.ass")

        for p in (video, clip, raw, ass, failed_render_clip, failed_render_raw, failed_render_ass):
            _touch(p)

        job_id = "job-upload-fail"
        self.main.jobs[job_id] = {
            "status": "queued",
            "video_path": video,
            "created_at": time.time(),
            "user_id": "u1",
        }

        clips_mock = (
            [{"filename": "clip_b.mp4", "description": "test", "start_time": 0, "end_time": 5}],
            [{"filename": "clip_failed_render.mp4", "error": "ffmpeg render failed"}],
        )

        with patch.object(self.main.clipper, "analyze_video", return_value=[{"start_time": 0, "end_time": 5}]), \
             patch.object(self.main.clipper, "create_clips", return_value=clips_mock), \
             patch.object(self.main, "upload_clip_to_storage", side_effect=RuntimeError("Storage upload failed")):
            asyncio.run(self.main.process_video_task(job_id, video, None, "u1"))

        self.assertEqual(self.main.jobs[job_id]["status"], "failed")
        self.assertFalse(os.path.exists(clip), "Failed upload clip must be cleaned up")
        self.assertFalse(os.path.exists(raw), "Failed upload _raw file must be cleaned up")
        self.assertFalse(os.path.exists(ass), "Failed upload .ass file must be cleaned up")
        self.assertFalse(os.path.exists(failed_render_clip), "Failed render clip must be cleaned up")
        self.assertFalse(os.path.exists(failed_render_raw), "Failed render _raw file must be cleaned up")
        self.assertFalse(os.path.exists(failed_render_ass), "Failed render .ass file must be cleaned up")
        self.assertFalse(os.path.exists(video), "Source video must be cleaned up")

    def test_download_failure_cleans_own_files_and_preserves_concurrent(self):
        """Case C: Download failure cleans own files (.mp4, .vtt, .part) and preserves concurrent job files."""
        dl_video = os.path.join(self.upload_dir, "dl_job.mp4")
        dl_vtt = os.path.join(self.upload_dir, "dl_job.en.vtt")
        dl_part = os.path.join(self.upload_dir, "dl_job.mp4.part")
        _touch(dl_video)
        _touch(dl_vtt)
        _touch(dl_part)

        other_video = os.path.join(self.upload_dir, "other_job.mp4")
        other_vtt = os.path.join(self.upload_dir, "other_job.en.vtt")
        other_part = os.path.join(self.upload_dir, "other_job.mp4.part")
        _touch(other_video)
        _touch(other_vtt)
        _touch(other_part)

        self.main.jobs["concurrent-job"] = {
            "status": "downloading",
            "video_path": other_video,
            "created_at": time.time(),
            "user_id": "u2",
        }

        job_id = "job-dl-fail"
        self.main.jobs[job_id] = {
            "status": "queued",
            "url": "https://youtu.be/test",
            "created_at": time.time(),
            "user_id": "u1",
        }

        def _failing_download(url, outdir):
            raise RuntimeError("Download network failure")

        with patch.object(self.main.clipper, "download_video", side_effect=_failing_download):
            self.main.jobs[job_id]["video_path"] = dl_video
            asyncio.run(self.main.download_and_process(job_id, "https://youtu.be/test", None, "u1", "ck"))

        self.assertEqual(self.main.jobs[job_id]["status"], "failed")
        # Files belonging to the failed download are cleaned
        self.assertFalse(os.path.exists(dl_video))
        self.assertFalse(os.path.exists(dl_vtt))
        self.assertFalse(os.path.exists(dl_part))

        # Another concurrent job's files are NOT deleted
        self.assertTrue(os.path.exists(other_video))
        self.assertTrue(os.path.exists(other_vtt))
        self.assertTrue(os.path.exists(other_part))

    def test_stale_sweep_removes_old_keeps_fresh_and_active(self):
        """Case D: Sweep removes stale files while keeping fresh files and active job files."""
        self.main.STALE_FILE_AGE = 3600
        now = time.time()

        old_file = os.path.join(self.upload_dir, "old_stale.mp4")
        fresh_file = os.path.join(self.upload_dir, "fresh.mp4")
        active_file = os.path.join(self.upload_dir, "active_job.mp4")
        _touch(old_file)
        _touch(fresh_file)
        _touch(active_file)

        os.utime(old_file, (now - 7200, now - 7200))
        os.utime(fresh_file, (now - 600, now - 600))
        os.utime(active_file, (now - 7200, now - 7200))

        self.main.jobs["active-job"] = {
            "status": "clipping",
            "video_path": active_file,
            "created_at": now,
            "user_id": "u1",
        }

        deleted = self.main._sweep_stale_local_files()
        self.assertEqual(deleted, 1)

        self.assertFalse(os.path.exists(old_file), "Old stale file must be removed")
        self.assertTrue(os.path.exists(fresh_file), "Fresh file must be kept")
        self.assertTrue(os.path.exists(active_file), "Active job file must be kept")

    def test_shared_video_protected_until_all_jobs_finish(self):
        """Case E: Shared video is protected while another job is active and cleaned once all jobs finish."""
        shared_video = os.path.join(self.upload_dir, "shared_vid.mp4")
        shared_vtt = os.path.join(self.upload_dir, "shared_vid.en.vtt")
        _touch(shared_video)
        _touch(shared_vtt)

        job_a = "job-a"
        job_b = "job-b"

        self.main.jobs[job_a] = {
            "status": "queued",
            "video_path": shared_video,
            "created_at": time.time(),
            "user_id": "u1",
        }
        self.main.jobs[job_b] = {
            "status": "clipping",
            "video_path": shared_video,
            "created_at": time.time(),
            "user_id": "u2",
        }

        # Job A fails analysis while Job B is still active
        with patch.object(self.main.clipper, "analyze_video", return_value=[]):
            asyncio.run(self.main.process_video_task(job_a, shared_video, None, "u1"))

        self.assertEqual(self.main.jobs[job_a]["status"], "failed")
        # Shared files must NOT be deleted because Job B is still active
        self.assertTrue(os.path.exists(shared_video), "Shared video must NOT be deleted while Job B is active")
        self.assertTrue(os.path.exists(shared_vtt), "Shared VTT must NOT be deleted while Job B is active")

        # Job B finishes/fails
        with patch.object(self.main.clipper, "analyze_video", return_value=[]):
            asyncio.run(self.main.process_video_task(job_b, shared_video, None, "u2"))

        self.assertEqual(self.main.jobs[job_b]["status"], "failed")
        # Now that both jobs are no longer active, shared files must be removed
        self.assertFalse(os.path.exists(shared_video), "Shared video must be removed when no active jobs remain")
        self.assertFalse(os.path.exists(shared_vtt), "Shared VTT must be removed when no active jobs remain")

    def test_cleanup_error_does_not_hide_original_exception(self):
        """Cleanup errors must never hide or overwrite the original processing exception."""
        video = os.path.join(self.upload_dir, "vid_err.mp4")
        _touch(video)

        job_id = "job-err"
        self.main.jobs[job_id] = {
            "status": "queued",
            "video_path": video,
            "created_at": time.time(),
            "user_id": "u1",
        }

        with patch.object(self.main.clipper, "analyze_video", side_effect=RuntimeError("ORIGINAL_ANALYSIS_ERROR")), \
             patch.object(self.main, "_safe_remove", side_effect=OSError("Disk permission error")):
            asyncio.run(self.main.process_video_task(job_id, video, None, "u1"))

        self.assertEqual(self.main.jobs[job_id]["status"], "failed")
        self.assertEqual(self.main.jobs[job_id]["error"], "ORIGINAL_ANALYSIS_ERROR")


if __name__ == "__main__":
    unittest.main()
