"""
Tests for GitHub Issue #199: GOOGLE_API_KEY missing -> clear failure, no fake clip.

Covers:
  1. Missing key raises MissingAPIKeyError.
  2. Empty string key raises MissingAPIKeyError.
  3. No fake clip metadata is ever returned.
  4. A configured key preserves the normal Gemini analysis path.
  5. process_video_task (main.py) marks the job as "failed" when
     analyze_video raises MissingAPIKeyError.

All external dependencies (Gemini, genai, yt-dlp, ffmpeg, supabase, ...)
are stubbed.  No real API key, network access, or video rendering needed.

Isolation note:
  Every test class that needs stubs saves the original sys.modules entries
  in setUp and restores them exactly in tearDown so that tests which rely on
  the *real* modules (e.g. test_media_url_policy) are unaffected regardless
  of collection order.
"""

import asyncio
import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

# -- Make sure the backend directory is on sys.path --------------------------
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)


# ---------------------------------------------------------------------------
# Stub builders  (called per-test, never at module import time)
# ---------------------------------------------------------------------------

_CLIPPER_STUB_KEYS = [
    "google",
    "google.generativeai",
    "yt_dlp",
    "dotenv",
    "url_policy",
    "moviepy",
    "clipper",
]

_MAIN_STUB_KEYS = _CLIPPER_STUB_KEYS + [
    "fastapi",
    "fastapi.middleware",
    "fastapi.middleware.cors",
    "fastapi.security",
    "fastapi.responses",
    "supabase_client",
    "captions",
    "captions.api",
    "captions.storage",
    "main",
]


def _save_modules(keys):
    """Return a snapshot of the named sys.modules entries (present or absent)."""
    return {k: sys.modules.get(k, None) for k in keys}


def _restore_modules(snapshot):
    """Put sys.modules back to the state captured by _save_modules."""
    for k, v in snapshot.items():
        if v is None:
            sys.modules.pop(k, None)
        else:
            sys.modules[k] = v


def _build_genai_stub():
    google_pkg = types.ModuleType("google")
    google_pkg.__path__ = []
    genai_mod = types.ModuleType("google.generativeai")
    genai_mod.configure = MagicMock()
    genai_mod.GenerativeModel = MagicMock()
    sys.modules["google"] = google_pkg
    sys.modules["google.generativeai"] = genai_mod
    return genai_mod


def _build_clipper_stubs():
    """Install stubs for every heavy dep that clipper.py imports at module level."""
    _build_genai_stub()

    yt_dlp = types.ModuleType("yt_dlp")
    sys.modules["yt_dlp"] = yt_dlp

    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None
    sys.modules["dotenv"] = dotenv

    url_policy = types.ModuleType("url_policy")
    url_policy.check_media_url = lambda url: url
    url_policy.UnsafeURLError = type("UnsafeURLError", (Exception,), {})
    sys.modules["url_policy"] = url_policy

    moviepy_mod = types.ModuleType("moviepy")
    moviepy_mod.VideoFileClip = MagicMock()
    sys.modules["moviepy"] = moviepy_mod


def _build_main_stubs(analyze_video_side_effect):
    """Install stubs needed for importing main.py in unit tests."""
    _build_clipper_stubs()

    fastapi = types.ModuleType("fastapi")

    class _FastAPI:
        def include_router(self, r): pass
        def add_middleware(self, *a, **kw): pass
        def get(self, *a, **kw): return lambda f: f
        def post(self, *a, **kw): return lambda f: f
        def delete(self, *a, **kw): return lambda f: f

    class _HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    fastapi.FastAPI = _FastAPI
    fastapi.UploadFile = object
    fastapi.File = lambda *a, **kw: None
    fastapi.Form = lambda *a, **kw: None
    fastapi.HTTPException = _HTTPException
    fastapi.BackgroundTasks = object
    fastapi.Depends = lambda *a, **kw: None
    fastapi.Request = object
    sys.modules["fastapi"] = fastapi

    for sub in ("fastapi.middleware", "fastapi.middleware.cors"):
        m = types.ModuleType(sub)
        m.CORSMiddleware = object
        sys.modules[sub] = m

    sec = types.ModuleType("fastapi.security")
    sec.HTTPBearer = lambda *a, **kw: object()
    sec.HTTPAuthorizationCredentials = object
    sys.modules["fastapi.security"] = sec

    resp = types.ModuleType("fastapi.responses")
    resp.StreamingResponse = object
    sys.modules["fastapi.responses"] = resp

    clipper_stub = types.ModuleType("clipper")
    clipper_stub.CLIP_STYLES = {"auto": "Auto"}
    clipper_stub.CAPTION_PRESETS = {"default": {}}
    clipper_stub.analyze_video = MagicMock(side_effect=analyze_video_side_effect)
    clipper_stub.create_clips = MagicMock(return_value=([], []))
    clipper_stub.download_video = MagicMock(return_value=("video.mp4", {}))
    sys.modules["clipper"] = clipper_stub

    supabase_stub = types.ModuleType("supabase_client")
    supabase_stub.supabase = object()
    supabase_stub.new_auth_client = lambda: object()
    supabase_stub.upload_clip_to_storage = lambda *a, **kw: "path.mp4"
    supabase_stub.delete_old_clips = lambda: 0
    supabase_stub.get_signed_url = lambda p: "https://example.test/clip.mp4"
    supabase_stub.get_user_clips = lambda uid: []
    sys.modules["supabase_client"] = supabase_stub

    captions = types.ModuleType("captions")
    captions.__path__ = []
    sys.modules["captions"] = captions

    cap_api = types.ModuleType("captions.api")
    cap_api.router = object()
    sys.modules["captions.api"] = cap_api

    cap_store = types.ModuleType("captions.storage")
    cap_store.delete_expired = lambda: 0
    sys.modules["captions.storage"] = cap_store

    return clipper_stub


def _reload_clipper(key_value):
    """
    Reload clipper with GOOGLE_API_KEY set to *key_value* (None = absent).
    Stubs must already be installed before calling this.
    """
    sys.modules.pop("clipper", None)
    if key_value is None:
        env = {k: v for k, v in os.environ.items() if k != "GOOGLE_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            return importlib.import_module("clipper")
    else:
        new_env = dict(os.environ)
        new_env["GOOGLE_API_KEY"] = key_value
        with patch.dict(os.environ, new_env, clear=True):
            return importlib.import_module("clipper")


# =============================================================================
# Base class: saves/restores sys.modules for clipper-level stubs
# =============================================================================

class _ClipperStubTestCase(unittest.TestCase):
    """setUp installs clipper stubs; tearDown removes every stub cleanly."""

    def setUp(self):
        self._snapshot = _save_modules(_CLIPPER_STUB_KEYS)
        _build_clipper_stubs()

    def tearDown(self):
        _restore_modules(self._snapshot)


# =============================================================================
# 1 & 2. MissingAPIKeyError raised (no key / empty string)
# =============================================================================

class TestMissingAPIKeyRaisesError(_ClipperStubTestCase):
    """analyze_video must raise MissingAPIKeyError -- never return fake metadata."""

    def _check(self, clipper_mod):
        with self.assertRaises(clipper_mod.MissingAPIKeyError) as ctx:
            clipper_mod.analyze_video("fake_video.mp4")
        msg = str(ctx.exception)
        self.assertIn("GOOGLE_API_KEY", msg,
                      "Exception message must mention GOOGLE_API_KEY")
        self.assertNotIn("API Key missing", msg)

    def test_missing_key_unset(self):
        """GOOGLE_API_KEY absent -> MissingAPIKeyError."""
        self._check(_reload_clipper(None))

    def test_missing_key_empty_string(self):
        """GOOGLE_API_KEY='' (empty) -> MissingAPIKeyError."""
        self._check(_reload_clipper(""))


# =============================================================================
# 3. No fake clip metadata is returned
# =============================================================================

class TestNoFakeClipReturned(_ClipperStubTestCase):
    """analyze_video must never silently return the fake 0-10 s placeholder."""

    FAKE_CLIP = {
        "start_time": 0,
        "end_time": 10,
        "description": "Short clip (API Key missing)",
    }

    def _assert_no_fake_clip(self, key_value):
        mod = _reload_clipper(key_value)
        try:
            result = mod.analyze_video("fake_video.mp4")
        except mod.MissingAPIKeyError:
            return   # Correct -- exception raised, no fake data
        if isinstance(result, list):
            self.assertNotIn(self.FAKE_CLIP, result,
                             "analyze_video returned the legacy fake-clip placeholder")
        self.assertNotEqual(result, [self.FAKE_CLIP])

    def test_no_fake_clip_when_key_missing(self):
        self._assert_no_fake_clip(None)

    def test_no_fake_clip_when_key_empty(self):
        self._assert_no_fake_clip("")


# =============================================================================
# 4. Configured key -> Gemini analysis path is reached
# =============================================================================

class TestConfiguredKeyPreservesNormalPath(_ClipperStubTestCase):
    """When GOOGLE_API_KEY is present, analyze_video must call Gemini."""

    def test_configured_key_calls_gemini(self):
        # Rebuild genai stub so GenerativeModel is a fresh mock we can inspect.
        genai_mod = _build_genai_stub()
        clipper_mod = _reload_clipper("test-key-abc123")

        fake_response = MagicMock()
        fake_response.text = (
            '[{"start_time": 5.0, "end_time": 35.0, "description": "Great moment",'
            ' "hook": "Hook text", "virality_score": 8, "clip_type": "insight"}]'
        )
        fake_model = MagicMock()
        fake_model.generate_content.return_value = fake_response
        genai_mod.GenerativeModel.return_value = fake_model

        with patch.object(clipper_mod, "_extract_keyframes",
                          return_value=([{"timestamp": 5.0, "data": "aGVsbG8="}], 60.0)), \
             patch.object(clipper_mod, "_find_vtt_file", return_value=None):
            result = clipper_mod.analyze_video("fake_video.mp4", info={})

        fake_model.generate_content.assert_called_once()
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        for clip in result:
            self.assertNotEqual(clip.get("description"), "Short clip (API Key missing)")


# =============================================================================
# 5. Job-level failure via process_video_task
# =============================================================================

class TestJobFailsOnMissingKey(unittest.TestCase):
    """
    process_video_task marks the job as 'failed' when analyze_video raises.
    """

    def setUp(self):
        self._snapshot = _save_modules(_MAIN_STUB_KEYS)

    def tearDown(self):
        _restore_modules(self._snapshot)

    def _run_task(self, exc):
        _build_main_stubs(analyze_video_side_effect=exc)
        sys.modules.pop("main", None)
        main_mod = importlib.import_module("main")

        job_id = "test-job-001"
        main_mod.jobs[job_id] = {
            "status": "queued",
            "url": "https://youtube.com/watch?v=test",
            "results": None,
            "error": None,
        }

        asyncio.run(main_mod.process_video_task(
            job_id=job_id,
            video_path="fake.mp4",
            instructions=None,
            user_id="user-abc",
        ))
        return main_mod.jobs[job_id]

    def test_job_marked_failed_on_missing_key_error(self):
        """MissingAPIKeyError causes job status == 'failed' with a useful message."""
        exc = RuntimeError(
            "GOOGLE_API_KEY is not configured. "
            "Set the GOOGLE_API_KEY environment variable to enable video analysis."
        )
        job = self._run_task(exc)
        self.assertEqual(job["status"], "failed")
        self.assertIn("GOOGLE_API_KEY", job.get("error", ""),
                      "Job error field must mention GOOGLE_API_KEY")

    def test_job_not_completed_on_missing_key(self):
        """A missing-key error must never leave the job in 'completed' state."""
        exc = RuntimeError("GOOGLE_API_KEY is not configured.")
        job = self._run_task(exc)
        self.assertNotEqual(job["status"], "completed")

    def test_job_has_no_results_on_missing_key(self):
        """results must remain None / empty when the key is absent."""
        exc = RuntimeError("GOOGLE_API_KEY is not configured.")
        job = self._run_task(exc)
        self.assertFalse(bool(job.get("results")))


if __name__ == "__main__":
    unittest.main()
