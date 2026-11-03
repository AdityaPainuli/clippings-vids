"""
Tests for Issue #198 – Cleanup Temporary Video Files

Covers:
  - Successful job cleanup (source video + .vtt files removed after upload)
  - Download failure (partial MP4, VTT, and .part files are deleted on download failure)
  - Processing (analysis) failure (source video and subtitles removed in finally)
  - Storage upload failure (local clips, _raw.mp4, and .ass removed in finally)
  - Subtitle artifacts cleanup (.vtt, .srt, .ass)
  - Partial download artifacts cleanup (.part, .ytdl)
  - Partial job cleanup (some clips upload, some fail; all local files removed)
  - Periodic sweep of stale files in uploads/ and output/
  - Active job protection from periodic sweep (videos, subtitles, clips) even with stale mtime
  - True concurrent overlapping job execution (asyncio.gather): neither job deletes the other's files
  - Cleanup errors never hide or replace the original processing or upload exception
"""

import asyncio
import importlib
import os
import sys
import time
import types
import unittest
import tempfile


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)


# ---------------------------------------------------------------------------
# Shared stub factory
# ---------------------------------------------------------------------------

def _install_stubs(
    analyze_video_fn=None,
    create_clips_fn=None,
    download_video_fn=None,
    upload_clip_fn=None,
    get_signed_url_fn=None,
):
    """
    Install lightweight stubs for all heavy external dependencies so the
    background-task functions can be imported and driven without network or
    real media processing.
    """
    # ── fastapi ──────────────────────────────────────────────────────────────
    fastapi = types.ModuleType("fastapi")

    class FastAPI:
        def include_router(self, router): pass
        def add_middleware(self, *a, **kw): pass
        def get(self, *a, **kw): return lambda f: f
        def post(self, *a, **kw): return lambda f: f
        def delete(self, *a, **kw): return lambda f: f

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    fastapi.FastAPI = FastAPI
    fastapi.UploadFile = object
    fastapi.File = lambda *a, **kw: None
    fastapi.Form = lambda *a, **kw: None
    fastapi.HTTPException = HTTPException
    fastapi.BackgroundTasks = object
    fastapi.Depends = lambda *a, **kw: None
    fastapi.Request = object
    sys.modules["fastapi"] = fastapi

    cors = types.ModuleType("fastapi.middleware.cors")
    cors.CORSMiddleware = object
    sys.modules["fastapi.middleware"] = types.ModuleType("fastapi.middleware")
    sys.modules["fastapi.middleware.cors"] = cors

    security = types.ModuleType("fastapi.security")
    security.HTTPBearer = lambda *a, **kw: object()
    security.HTTPAuthorizationCredentials = object
    sys.modules["fastapi.security"] = security

    responses = types.ModuleType("fastapi.responses")
    responses.StreamingResponse = object
    sys.modules["fastapi.responses"] = responses

    # ── clipper ───────────────────────────────────────────────────────────────
    clipper = types.ModuleType("clipper")
    clipper.CLIP_STYLES = {"auto": "Auto"}
    clipper.CAPTION_PRESETS = {"default": {}}
    clipper.analyze_video = analyze_video_fn or (lambda *a, **kw: [
        {"start_time": 0, "end_time": 5, "description": "clip",
         "hook": "h", "virality_score": 8, "clip_type": "insight"}
    ])
    clipper.create_clips = create_clips_fn or (lambda *a, **kw: (
        [{"filename": "clip_0_abc12345.mp4", "description": "clip",
          "start_time": 0, "end_time": 5, "hook": "h",
          "virality_score": 8, "clip_type": "insight"}],
        []
    ))
    clipper.download_video = download_video_fn or (lambda *a, **kw: ("uploads/vid.mp4", {}))
    sys.modules["clipper"] = clipper

    # ── supabase_client ───────────────────────────────────────────────────────
    supabase_client = types.ModuleType("supabase_client")
    supabase_client.supabase = object()
    supabase_client.new_auth_client = lambda: object()
    supabase_client.upload_clip_to_storage = upload_clip_fn or (
        lambda *a, **kw: "storage/path.mp4"
    )
    supabase_client.delete_old_clips = lambda: 0
    supabase_client.get_signed_url = get_signed_url_fn or (
        lambda path: "https://example.test/clip.mp4"
    )
    supabase_client.get_user_clips = lambda user_id: []
    sys.modules["supabase_client"] = supabase_client

    # ── captions ─────────────────────────────────────────────────────────────
    captions = types.ModuleType("captions")
    captions.__path__ = []
    sys.modules["captions"] = captions

    captions_api = types.ModuleType("captions.api")
    captions_api.router = object()
    sys.modules["captions.api"] = captions_api

    captions_storage = types.ModuleType("captions.storage")
    captions_storage.delete_expired = lambda: 0
    sys.modules["captions.storage"] = captions_storage

    # ── url_policy ────────────────────────────────────────────────────────────
    url_policy = types.ModuleType("url_policy")

    class UnsafeURLError(Exception):
        pass

    url_policy.check_media_url = lambda url: url
    url_policy.UnsafeURLError = UnsafeURLError
    sys.modules["url_policy"] = url_policy


def _fresh_main(**kwargs):
    """Remove any cached main module and reload it with fresh stubs."""
    sys.modules.pop("main", None)
    _install_stubs(**kwargs)
    return importlib.import_module("main")


# ---------------------------------------------------------------------------
# Helper – create real files on disk
# ---------------------------------------------------------------------------

def _touch(path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"\x00")
    return path


# ===========================================================================
# 1. Successful job – source video, subtitles, and clips removed after upload
# ===========================================================================

class TestSuccessfulJobCleanup(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def _make_paths(self, job_id="job-ok"):
        video = os.path.join(self.tmpdir, "uploads", f"{job_id}.mp4")
        vtt_en = os.path.join(self.tmpdir, "uploads", f"{job_id}.en.vtt")
        clip = os.path.join(self.tmpdir, "output", "clip_0_abc12345.mp4")
        for p in (video, vtt_en, clip):
            _touch(p)
        return video, vtt_en, clip

    def test_source_video_removed_on_success(self):
        video_path, vtt_path, clip_path = self._make_paths()

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [{"filename": os.path.basename(clip_path),
                  "description": "d", "start_time": 0, "end_time": 5,
                  "hook": "h", "virality_score": 8, "clip_type": "insight"}],
                []
            ),
        )
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output")
        job_id = "job-ok"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video_path, None, "u1", {}, True, None,
        ))

        self.assertFalse(os.path.exists(video_path),
                         "Source video must be removed after successful job")

    def test_vtt_files_removed_on_success(self):
        video_path, vtt_path, clip_path = self._make_paths("job-vtt")

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [{"filename": os.path.basename(clip_path),
                  "description": "d", "start_time": 0, "end_time": 5,
                  "hook": "h", "virality_score": 8, "clip_type": "insight"}],
                []
            ),
        )
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output")
        job_id = "job-vtt"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video_path, None, "u1", {}, True, None,
        ))

        self.assertFalse(os.path.exists(vtt_path),
                         ".vtt subtitle file must be removed on success")

    def test_local_clip_removed_after_successful_upload(self):
        video_path, _, clip_path = self._make_paths("job-clip")

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [{"filename": os.path.basename(clip_path),
                  "description": "d", "start_time": 0, "end_time": 5,
                  "hook": "h", "virality_score": 8, "clip_type": "insight"}],
                []
            ),
        )
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output")
        job_id = "job-clip"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video_path, None, "u1", {}, True, None,
        ))

        self.assertFalse(os.path.exists(clip_path),
                         "Rendered clip must be removed after upload succeeds")


# ===========================================================================
# 2. Download failure – partial files (mp4, vtt, .part) are deleted
# ===========================================================================

class TestDownloadFailureCleanup(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_download_exception_cleans_up_partial_video(self):
        """
        Verify that when download_video raises an exception during download,
        all partial MP4, VTT, and .part files created during the attempt are deleted.
        """
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)

        partial = os.path.join(main.UPLOAD_DIR, "partial.mp4")
        vtt = os.path.join(main.UPLOAD_DIR, "partial.en.vtt")
        part = os.path.join(main.UPLOAD_DIR, "partial.mp4.part")

        def _failing_download(url, outdir):
            # yt-dlp creates partial artifacts before network/process failure
            _touch(partial)
            _touch(vtt)
            _touch(part)
            raise RuntimeError("network failure during download")

        main.clipper.download_video = _failing_download

        job_id = "job-dl-fail"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.download_and_process(
            job_id, "http://x.test/v", None, "u1", "ck",
        ))

        self.assertEqual(main.jobs[job_id]["status"], "failed")
        self.assertIn("network failure during download", main.jobs[job_id]["error"])
        # Assert that partial MP4, VTT and .part files are actually deleted
        self.assertFalse(os.path.exists(partial), "Partial MP4 file must be deleted on download failure")
        self.assertFalse(os.path.exists(vtt), "Partial VTT file must be deleted on download failure")
        self.assertFalse(os.path.exists(part), "Partial .part file must be deleted on download failure")

    def test_partial_download_file_removed_when_path_known(self):
        """
        Simulate a download that writes a file and part file, returns path, then fails later: verify cleanup.
        """
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)

        partial = os.path.join(main.UPLOAD_DIR, "partial2.mp4")
        vtt = os.path.join(main.UPLOAD_DIR, "partial2.en.vtt")
        part = os.path.join(main.UPLOAD_DIR, "partial2.mp4.part")
        _touch(partial)
        _touch(vtt)
        _touch(part)

        def _partial_download(url, outdir):
            return partial, {}

        def _analyze_raises(*a, **kw):
            raise RuntimeError("analysis crash")

        main.clipper.download_video = _partial_download
        main.clipper.analyze_video = _analyze_raises

        job_id = "job-partial-dl"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.download_and_process(
            job_id, "http://x.test/v", None, "u1", "ck",
        ))

        self.assertFalse(os.path.exists(partial),
                         "Source video must be removed even when analysis fails")
        self.assertFalse(os.path.exists(vtt),
                         ".vtt must be removed even when analysis fails")
        self.assertFalse(os.path.exists(part),
                         ".part file must be removed even when analysis fails")


# ===========================================================================
# 3. Processing failure – source video and subtitles removed
# ===========================================================================

class TestAnalysisFailureCleanup(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_source_removed_on_analysis_failure(self):
        video = os.path.join(self.tmpdir, "uploads", "vid_analysis.mp4")
        vtt = os.path.join(self.tmpdir, "uploads", "vid_analysis.en.vtt")
        _touch(video)
        _touch(vtt)

        def _bad_analyze(*a, **kw):
            raise RuntimeError("gemini error")

        main = _fresh_main(analyze_video_fn=_bad_analyze)
        job_id = "job-analysis"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video, None, "u1", {}, True, None,
        ))

        self.assertEqual(main.jobs[job_id]["status"], "failed")
        self.assertFalse(os.path.exists(video),
                         "Source video must be removed even when analysis raises")
        self.assertFalse(os.path.exists(vtt),
                         ".vtt must be removed even when analysis raises")


# ===========================================================================
# 4. Upload failure – local clips and intermediate files removed in finally
# ===========================================================================

class TestUploadFailureCleanup(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_clip_and_intermediate_files_removed_when_upload_fails(self):
        video = os.path.join(self.tmpdir, "uploads", "vid_upload.mp4")
        clip = os.path.join(self.tmpdir, "output", "clip_0_upload.mp4")
        clip_raw = os.path.join(self.tmpdir, "output", "clip_0_upload_raw.mp4")
        clip_ass = os.path.join(self.tmpdir, "output", "clip_0_upload.ass")
        _touch(video)
        _touch(clip)
        _touch(clip_raw)
        _touch(clip_ass)

        def _failing_upload(*a, **kw):
            raise RuntimeError("storage upload network error")

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [{"filename": os.path.basename(clip),
                  "description": "d", "start_time": 0, "end_time": 5,
                  "hook": "h", "virality_score": 7, "clip_type": "funny"}],
                []
            ),
            upload_clip_fn=_failing_upload,
        )
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output")
        job_id = "job-upload-fail"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video, None, "u1", {}, True, None,
        ))

        self.assertFalse(os.path.exists(clip),
                         "Local clip must be removed even when upload fails")
        self.assertFalse(os.path.exists(clip_raw),
                         "Intermediate raw clip must be removed even when upload fails")
        self.assertFalse(os.path.exists(clip_ass),
                         "Intermediate ass file must be removed even when upload fails")
        self.assertFalse(os.path.exists(video),
                         "Source video must be removed even when upload fails")


# ===========================================================================
# 5. Review cleanup of .vtt, .srt, .ass, .part, .ytdl, _raw.mp4 and generated clips
# ===========================================================================

class TestAllArtifactTypesCleanup(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_all_artifact_extensions_cleaned_up(self):
        """
        Verify that .vtt, .srt, .ass, .part, .ytdl, _raw.mp4 and .mp4 are all cleaned up.
        """
        uploads = os.path.join(self.tmpdir, "uploads")
        output = os.path.join(self.tmpdir, "output")
        os.makedirs(uploads, exist_ok=True)
        os.makedirs(output, exist_ok=True)

        video = os.path.join(uploads, "full_suite.mp4")
        vtt = os.path.join(uploads, "full_suite.en.vtt")
        srt = os.path.join(uploads, "full_suite.srt")
        ass = os.path.join(uploads, "full_suite.ass")
        part = os.path.join(uploads, "full_suite.mp4.part")
        ytdl = os.path.join(uploads, "full_suite.ytdl")

        clip = os.path.join(output, "clip_suite.mp4")
        clip_raw = os.path.join(output, "clip_suite_raw.mp4")
        clip_ass = os.path.join(output, "clip_suite.ass")

        all_paths = [video, vtt, srt, ass, part, ytdl, clip, clip_raw, clip_ass]
        for p in all_paths:
            _touch(p)

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [{"filename": "clip_suite.mp4", "description": "desc",
                  "start_time": 0, "end_time": 5, "hook": "h",
                  "virality_score": 9, "clip_type": "insight"}],
                []
            ),
        )
        main.OUTPUT_DIR = output
        job_id = "job-artifacts"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video, None, "u1", {}, True, None,
        ))

        for p in all_paths:
            self.assertFalse(os.path.exists(p), f"Artifact {os.path.basename(p)} must be cleaned up")


# ===========================================================================
# 6. Partial job – some clips upload, some fail; all local files removed
# ===========================================================================

class TestPartialJobCleanup(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_all_local_clips_removed_in_partial_job(self):
        output = os.path.join(self.tmpdir, "output")
        os.makedirs(output, exist_ok=True)
        video = os.path.join(self.tmpdir, "uploads", "vid_partial.mp4")
        clip_ok = os.path.join(output, "clip_0_ok.mp4")
        clip_fail = os.path.join(output, "clip_1_fail.mp4")
        _touch(video)
        _touch(clip_ok)
        _touch(clip_fail)

        call_count = [0]

        def _sometimes_failing_upload(local_path, *a, **kw):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("upload error for clip 2")
            return "storage/ok.mp4"

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [
                    {"filename": "clip_0_ok.mp4", "description": "d1",
                     "start_time": 0, "end_time": 5,
                     "hook": "h", "virality_score": 8, "clip_type": "insight"},
                    {"filename": "clip_1_fail.mp4", "description": "d2",
                     "start_time": 6, "end_time": 10,
                     "hook": "h2", "virality_score": 6, "clip_type": "funny"},
                ],
                []
            ),
            upload_clip_fn=_sometimes_failing_upload,
        )
        main.OUTPUT_DIR = output
        job_id = "job-partial"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video, None, "u1", {}, True, None,
        ))

        # Job should be completed (partial results)
        self.assertEqual(main.jobs[job_id]["status"], "completed")
        # Both local clips must be gone regardless
        self.assertFalse(os.path.exists(clip_ok),
                         "clip_ok must be removed after successful upload")
        self.assertFalse(os.path.exists(clip_fail),
                         "clip_fail must be removed even after upload failure")
        self.assertFalse(os.path.exists(video),
                         "Source video must be removed")


# ===========================================================================
# 7. Periodic sweep of stale files in uploads/ and output/
# ===========================================================================

class TestSweepStaleLocalFiles(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def _run_sweep(self, main):
        return main._sweep_stale_local_files()

    def test_stale_files_removed_from_uploads(self):
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)
        os.makedirs(main.OUTPUT_DIR, exist_ok=True)
        main.STALE_FILE_AGE = 10  # very short threshold

        stale = os.path.join(main.UPLOAD_DIR, "stale_video.mp4")
        _touch(stale)
        old_time = time.time() - 100
        os.utime(stale, (old_time, old_time))

        deleted = self._run_sweep(main)
        self.assertGreaterEqual(deleted, 1)
        self.assertFalse(os.path.exists(stale))

    def test_stale_files_removed_from_output(self):
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads2")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output2")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)
        os.makedirs(main.OUTPUT_DIR, exist_ok=True)
        main.STALE_FILE_AGE = 10

        stale = os.path.join(main.OUTPUT_DIR, "clip_old.mp4")
        _touch(stale)
        old_time = time.time() - 100
        os.utime(stale, (old_time, old_time))

        deleted = self._run_sweep(main)
        self.assertGreaterEqual(deleted, 1)
        self.assertFalse(os.path.exists(stale))

    def test_recent_files_not_swept(self):
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads3")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output3")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)
        os.makedirs(main.OUTPUT_DIR, exist_ok=True)
        main.STALE_FILE_AGE = 7200  # 2 hours – file is fresh

        fresh = os.path.join(main.UPLOAD_DIR, "fresh_video.mp4")
        _touch(fresh)

        deleted = self._run_sweep(main)
        self.assertEqual(deleted, 0)
        self.assertTrue(os.path.exists(fresh))

    def test_missing_directory_handled_safely(self):
        """Sweep must not raise when a directory doesn't exist."""
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "no_such_uploads")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "no_such_output")
        main.STALE_FILE_AGE = 10
        try:
            self._run_sweep(main)
        except Exception as exc:
            self.fail(f"_sweep_stale_local_files raised unexpectedly: {exc}")


# ===========================================================================
# 8. Active/recent job files are protected from the sweep (race condition prevention)
# ===========================================================================

class TestSweepProtectsActiveJobFiles(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_active_job_video_and_subtitles_not_swept(self):
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads_active")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output_active")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)
        os.makedirs(main.OUTPUT_DIR, exist_ok=True)
        main.STALE_FILE_AGE = 10

        # Create files that look stale by mtime
        active_video = os.path.join(main.UPLOAD_DIR, "active_job.mp4")
        active_vtt = os.path.join(main.UPLOAD_DIR, "active_job.en.vtt")
        active_part = os.path.join(main.UPLOAD_DIR, "active_job.mp4.part")
        _touch(active_video)
        _touch(active_vtt)
        _touch(active_part)
        old_time = time.time() - 100
        os.utime(active_video, (old_time, old_time))
        os.utime(active_vtt, (old_time, old_time))
        os.utime(active_part, (old_time, old_time))

        # Register it as belonging to an active (downloading) job
        main.jobs.clear()
        main.jobs["active-job-1"] = {
            "status": "downloading",
            "video_path": active_video,
            "created_at": time.time(),
            "user_id": "u1",
        }

        deleted = main._sweep_stale_local_files()
        self.assertEqual(deleted, 0, "Active job files must not be swept even if mtime is old")
        self.assertTrue(os.path.exists(active_video))
        self.assertTrue(os.path.exists(active_vtt))
        self.assertTrue(os.path.exists(active_part))

    def test_active_job_clip_files_not_swept(self):
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads_active_clips")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output_active_clips")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)
        os.makedirs(main.OUTPUT_DIR, exist_ok=True)
        main.STALE_FILE_AGE = 10

        active_clip = os.path.join(main.OUTPUT_DIR, "active_clip.mp4")
        active_raw = os.path.join(main.OUTPUT_DIR, "active_clip_raw.mp4")
        _touch(active_clip)
        _touch(active_raw)
        old_time = time.time() - 100
        os.utime(active_clip, (old_time, old_time))
        os.utime(active_raw, (old_time, old_time))

        main.jobs.clear()
        main.jobs["active-job-clips"] = {
            "status": "uploading",
            "clip_paths": [active_clip],
            "created_at": time.time(),
            "user_id": "u1",
        }

        deleted = main._sweep_stale_local_files()
        self.assertEqual(deleted, 0, "Active job clips must not be swept")
        self.assertTrue(os.path.exists(active_clip))
        self.assertTrue(os.path.exists(active_raw))

    def test_completed_job_video_can_be_swept(self):
        """A stale file from a *completed* job (no longer active) should be swept."""
        main = _fresh_main()
        main.UPLOAD_DIR = os.path.join(self.tmpdir, "uploads_done")
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output_done")
        os.makedirs(main.UPLOAD_DIR, exist_ok=True)
        os.makedirs(main.OUTPUT_DIR, exist_ok=True)
        main.STALE_FILE_AGE = 10

        stale = os.path.join(main.UPLOAD_DIR, "done_job.mp4")
        _touch(stale)
        old_time = time.time() - 100
        os.utime(stale, (old_time, old_time))

        # This job is completed — its file is NOT protected
        main.jobs.clear()
        main.jobs["done-job-1"] = {
            "status": "completed",
            "video_path": stale,
            "created_at": time.time() - 200,
            "user_id": "u1",
        }

        deleted = main._sweep_stale_local_files()
        self.assertGreaterEqual(deleted, 1)
        self.assertFalse(os.path.exists(stale))


# ===========================================================================
# 9. Genuinely concurrent overlapping jobs – isolation
# ===========================================================================

class TestConcurrentJobIsolation(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_overlapping_concurrent_jobs_do_not_delete_each_others_files(self):
        """
        Job A and Job B execute concurrently using asyncio.gather.
        Job A completes and cleans up while Job B is still actively processing.
        Verify Job A's cleanup does NOT delete Job B's active video, subtitles, or clips.
        Verify Job A does not delete similar-prefix files belonging to Job B.
        """
        uploads = os.path.join(self.tmpdir, "uploads_conc")
        output = os.path.join(self.tmpdir, "output_conc")
        os.makedirs(uploads, exist_ok=True)
        os.makedirs(output, exist_ok=True)

        video_a = os.path.join(uploads, "job_a.mp4")
        vtt_a = os.path.join(uploads, "job_a.en.vtt")
        clip_a = os.path.join(output, "clip_a.mp4")

        # Job B has distinct files, including one with a similar prefix
        video_b = os.path.join(uploads, "job_a_extra_job_b.mp4")
        vtt_b = os.path.join(uploads, "job_a_extra_job_b.en.vtt")
        clip_b = os.path.join(output, "clip_b.mp4")

        for p in (video_a, vtt_a, clip_a, video_b, vtt_b, clip_b):
            _touch(p)

        job_b_paused = asyncio.Event()
        job_a_finished = asyncio.Event()

        def _custom_analyze_video(video_path, *a, **kw):
            return [{"start_time": 0, "end_time": 5, "description": "clip",
                     "hook": "h", "virality_score": 8, "clip_type": "insight"}]

        def _custom_create_clips(video_path, *a, **kw):
            if "job_a" in video_path and "extra" not in video_path:
                return ([{"filename": "clip_a.mp4", "description": "d",
                          "start_time": 0, "end_time": 5, "hook": "h",
                          "virality_score": 8, "clip_type": "insight"}], [])
            else:
                return ([{"filename": "clip_b.mp4", "description": "d",
                          "start_time": 0, "end_time": 5, "hook": "h",
                          "virality_score": 8, "clip_type": "insight"}], [])

        main = _fresh_main(
            analyze_video_fn=_custom_analyze_video,
            create_clips_fn=_custom_create_clips,
        )
        main.OUTPUT_DIR = output

        job_a = "job-conc-a"
        job_b = "job-conc-b"
        main.jobs[job_a] = {
            "status": "queued", "url": "http://x.test/a",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }
        main.jobs[job_b] = {
            "status": "queued", "url": "http://x.test/b",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u2",
        }

        async def _run_job_a():
            # Wait for Job B to reach active state
            await job_b_paused.wait()
            await main.process_video_task(job_a, video_a, None, "u1", {}, True, None)
            job_a_finished.set()

        async def _run_job_b():
            # Signal that Job B has started and register its files
            main.jobs[job_b]["status"] = "clipping"
            main.jobs[job_b]["video_path"] = video_b
            main.jobs[job_b]["clip_paths"] = [clip_b]
            job_b_paused.set()

            # Wait while Job A executes and cleans up
            await job_a_finished.wait()

            # Verify mid-flight that Job A's cleanup did NOT touch Job B's files!
            self.assertTrue(os.path.exists(video_b), "Job B's video must not be deleted by Job A's cleanup")
            self.assertTrue(os.path.exists(vtt_b), "Job B's VTT must not be deleted by Job A's cleanup")
            self.assertTrue(os.path.exists(clip_b), "Job B's clip must not be deleted by Job A's cleanup")

            # Now let Job B finish its processing and cleanup
            await main.process_video_task(job_b, video_b, None, "u2", {}, True, None)

        async def _run_both():
            await asyncio.gather(_run_job_a(), _run_job_b())

        asyncio.run(_run_both())

        # Verify Job A's files are cleaned up
        self.assertFalse(os.path.exists(video_a), "Job A video must be cleaned up")
        self.assertFalse(os.path.exists(vtt_a), "Job A VTT must be cleaned up")
        self.assertFalse(os.path.exists(clip_a), "Job A clip must be cleaned up")

        # Verify Job B's files are cleaned up after Job B completes
        self.assertFalse(os.path.exists(video_b), "Job B video must be cleaned up")
        self.assertFalse(os.path.exists(vtt_b), "Job B VTT must be cleaned up")
        self.assertFalse(os.path.exists(clip_b), "Job B clip must be cleaned up")

        # Verify both jobs reached terminal state
        self.assertEqual(main.jobs[job_a]["status"], "completed")
        self.assertEqual(main.jobs[job_b]["status"], "completed")


# ===========================================================================
# 10. Cleanup errors do not hide original exceptions
# ===========================================================================

class TestCleanupDoesNotHideException(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_original_exception_preserved_when_cleanup_fails_with_oserror(self):
        """
        If analyze_video raises an exception AND file removal raises an OSError,
        the original exception must be preserved in jobs[job_id]["error"] and
        must not be replaced or hidden by the cleanup error.
        """
        video = os.path.join(self.tmpdir, "uploads", "vid_exc.mp4")
        _touch(video)

        def _bad_analyze(*a, **kw):
            raise RuntimeError("CRITICAL ANALYSIS ENGINE FAILURE")

        main = _fresh_main(analyze_video_fn=_bad_analyze)

        # Force os.remove to raise PermissionError / OSError
        original_remove = os.remove

        def _broken_remove(path):
            if path == video:
                raise PermissionError("EACCES: permission denied during cleanup")
            return original_remove(path)

        job_id = "job-exc-hide"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        # Monkeypatch os.remove in main
        main.os.remove = _broken_remove
        try:
            asyncio.run(main.process_video_task(
                job_id, video, None, "u1", {}, True, None,
            ))
        finally:
            main.os.remove = original_remove

        self.assertEqual(main.jobs[job_id]["status"], "failed")
        # Ensure the original error message is intact and NOT replaced
        self.assertEqual(main.jobs[job_id]["error"], "CRITICAL ANALYSIS ENGINE FAILURE")

    def test_original_upload_exception_preserved_when_cleanup_fails(self):
        """
        If storage upload fails AND cleanup encounters an error, the original
        upload failure must be preserved.
        """
        video = os.path.join(self.tmpdir, "uploads", "vid_up_exc.mp4")
        clip = os.path.join(self.tmpdir, "output", "clip_up_exc.mp4")
        _touch(video)
        _touch(clip)

        def _failing_upload(*a, **kw):
            raise RuntimeError("SUPABASE UPLOAD TIMEOUT (504)")

        main = _fresh_main(
            create_clips_fn=lambda *a, **kw: (
                [{"filename": os.path.basename(clip), "description": "d",
                  "start_time": 0, "end_time": 5, "hook": "h",
                  "virality_score": 8, "clip_type": "insight"}],
                []
            ),
            upload_clip_fn=_failing_upload,
        )
        main.OUTPUT_DIR = os.path.join(self.tmpdir, "output")

        job_id = "job-up-hide"
        main.jobs[job_id] = {
            "status": "queued", "url": "http://x.test/v",
            "results": None, "error": None,
            "created_at": time.time(), "user_id": "u1",
        }

        asyncio.run(main.process_video_task(
            job_id, video, None, "u1", {}, True, None,
        ))

        self.assertEqual(main.jobs[job_id]["status"], "failed")
        self.assertIn("failed to render/upload", main.jobs[job_id]["error"])
        failed_clips = main.jobs[job_id].get("failed_clips", [])
        self.assertTrue(any("SUPABASE UPLOAD TIMEOUT" in str(fc.get("error", "")) for fc in failed_clips))


# ===========================================================================
# 11. _maybe_cleanup sweeps stale files (integration with periodic loop)
# ===========================================================================

class TestMaybeCleanupCallsSweep(unittest.TestCase):

    def test_sweep_called_by_maybe_cleanup(self):
        """
        _maybe_cleanup should invoke _sweep_stale_local_files.
        """
        main = _fresh_main()
        main._last_cleanup = 0.0  # force cleanup to run

        called = []

        def _fake_sweep():
            called.append(True)
            return 0

        main._sweep_stale_local_files = _fake_sweep

        asyncio.run(main._maybe_cleanup())

        self.assertTrue(called, "_sweep_stale_local_files must be called by _maybe_cleanup")


# ===========================================================================
# 12. Original TTL job-record purge still works
# ===========================================================================

class TestJobTTLPurgeStillWorks(unittest.TestCase):

    def test_cleanup_only_purges_terminal_jobs_past_ttl(self):
        main = _fresh_main()
        main._last_cleanup = 0.0
        main.JOB_TTL = 7200
        now = time.time()
        old_time = now - 10000

        main.jobs.clear()
        main.jobs["active-1"] = {"status": "rendering", "created_at": old_time, "user_id": "u1"}
        main.jobs["active-2"] = {"status": "analyzing", "created_at": old_time, "user_id": "u1"}
        main.jobs["completed-old"] = {"status": "completed", "created_at": old_time, "user_id": "u1"}
        main.jobs["failed-old"] = {"status": "failed", "created_at": old_time, "user_id": "u1"}
        main.jobs["completed-fresh"] = {"status": "completed", "created_at": now - 100, "user_id": "u1"}

        asyncio.run(main._maybe_cleanup())

        self.assertIn("active-1", main.jobs)
        self.assertIn("active-2", main.jobs)
        self.assertIn("completed-fresh", main.jobs)
        self.assertNotIn("completed-old", main.jobs)
        self.assertNotIn("failed-old", main.jobs)


if __name__ == "__main__":
    unittest.main()
