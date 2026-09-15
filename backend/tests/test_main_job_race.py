import asyncio
import importlib
import os
import sys
import time
import types
import unittest


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)


def _install_main_import_stubs():
    fastapi = types.ModuleType("fastapi")

    class FastAPI:
        def include_router(self, router):
            pass

        def add_middleware(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            return lambda func: func

        def post(self, *args, **kwargs):
            return lambda func: func

        def delete(self, *args, **kwargs):
            return lambda func: func

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    fastapi.FastAPI = FastAPI
    fastapi.UploadFile = object
    fastapi.File = lambda *args, **kwargs: None
    fastapi.Form = lambda *args, **kwargs: None
    fastapi.HTTPException = HTTPException
    fastapi.BackgroundTasks = object
    fastapi.Depends = lambda *args, **kwargs: None
    fastapi.Request = object
    sys.modules["fastapi"] = fastapi

    cors = types.ModuleType("fastapi.middleware.cors")
    cors.CORSMiddleware = object
    sys.modules["fastapi.middleware"] = types.ModuleType("fastapi.middleware")
    sys.modules["fastapi.middleware.cors"] = cors

    security = types.ModuleType("fastapi.security")
    security.HTTPBearer = lambda *args, **kwargs: object()
    security.HTTPAuthorizationCredentials = object
    sys.modules["fastapi.security"] = security

    responses = types.ModuleType("fastapi.responses")
    responses.StreamingResponse = object
    sys.modules["fastapi.responses"] = responses

    clipper = types.ModuleType("clipper")
    clipper.CLIP_STYLES = {"auto": "Auto"}
    clipper.CAPTION_PRESETS = {"default": {}}
    clipper.analyze_video = lambda *args, **kwargs: []
    clipper.create_clips = lambda *args, **kwargs: ([], [])
    clipper.download_video = lambda *args, **kwargs: ("", {})
    sys.modules["clipper"] = clipper

    supabase_client = types.ModuleType("supabase_client")
    supabase_client.supabase = object()
    supabase_client.upload_clip_to_storage = lambda *args, **kwargs: "storage/path.mp4"
    supabase_client.delete_old_clips = lambda: 0
    supabase_client.get_signed_url = lambda path: "https://example.test/clip.mp4"
    supabase_client.get_user_clips = lambda user_id: []
    sys.modules["supabase_client"] = supabase_client

    captions = types.ModuleType("captions")
    captions.__path__ = []
    sys.modules["captions"] = captions

    captions_api = types.ModuleType("captions.api")
    captions_api.router = object()
    sys.modules["captions.api"] = captions_api

    captions_storage = types.ModuleType("captions.storage")
    captions_storage.delete_expired = lambda: 0
    sys.modules["captions.storage"] = captions_storage


class MainJobCleanupTTLTest(unittest.TestCase):
    def test_cleanup_only_purges_terminal_jobs_past_ttl(self):
        _install_main_import_stubs()
        main = importlib.import_module("main")

        main._last_cleanup = 0.0  # Force _maybe_cleanup to execute
        main.JOB_TTL = 7200
        now = time.time()
        old_time = now - 10000

        # Set up test jobs
        main.jobs.clear()
        main.jobs["active-1"] = {"status": "rendering", "created_at": old_time, "user_id": "user-1"}
        main.jobs["active-2"] = {"status": "analyzing", "created_at": old_time, "user_id": "user-1"}
        main.jobs["completed-old"] = {"status": "completed", "created_at": old_time, "user_id": "user-1"}
        main.jobs["failed-old"] = {"status": "failed", "created_at": old_time, "user_id": "user-1"}
        main.jobs["completed-fresh"] = {"status": "completed", "created_at": now - 100, "user_id": "user-1"}

        asyncio.run(main._maybe_cleanup())

        # Assert active old jobs and fresh completed job were preserved
        self.assertIn("active-1", main.jobs)
        self.assertIn("active-2", main.jobs)
        self.assertIn("completed-fresh", main.jobs)

        # Assert stale terminal jobs were purged
        self.assertNotIn("completed-old", main.jobs)
        self.assertNotIn("failed-old", main.jobs)


if __name__ == "__main__":
    unittest.main()
