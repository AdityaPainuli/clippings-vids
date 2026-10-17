import asyncio
import importlib
import os
import sys
import types
import unittest


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)


def _install_main_import_stubs():
    fastapi = types.ModuleType("fastapi")

    class FastAPI:
        def __init__(self):
            self.routes = []

        def include_router(self, router):
            pass

        def add_middleware(self, *args, **kwargs):
            pass

        def get(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

        def post(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

        def delete(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    class HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail
            super().__init__(f"HTTP {status_code}: {detail}")

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
    supabase_client.SUPABASE_URL = "https://example.supabase.co"
    supabase_client.SUPABASE_SERVICE_KEY = "test-service-key"
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


class TestHealthAndReadyEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_main_import_stubs()
        cls.main = importlib.import_module("main")

    def test_health_endpoint(self):
        res = asyncio.run(self.main.health())
        self.assertEqual(res, {"status": "ok"})

    def test_ready_endpoint_healthy(self):
        res = asyncio.run(self.main.ready())
        self.assertEqual(res["status"], "ready")
        self.assertEqual(res["checks"]["upload_dir"], "ok")
        self.assertEqual(res["checks"]["output_dir"], "ok")
        self.assertEqual(res["checks"]["supabase"], "ok")
        # Ensure no secrets leak
        self.assertNotIn("test-service-key", str(res))
        self.assertNotIn("example.supabase.co", str(res))

    def test_ready_endpoint_unhealthy_missing_dir(self):
        original_upload_dir = self.main.UPLOAD_DIR
        try:
            self.main.UPLOAD_DIR = "/non/existent/path/for/sure"
            with self.assertRaises(Exception) as ctx:
                asyncio.run(self.main.ready())
            exc = ctx.exception
            self.assertEqual(getattr(exc, "status_code", None), 503)
            self.assertEqual(exc.detail["status"], "not_ready")
            self.assertIn("upload_dir", exc.detail["failed_checks"])
        finally:
            self.main.UPLOAD_DIR = original_upload_dir

    def test_ready_endpoint_unhealthy_missing_supabase(self):
        import supabase_client
        orig_key = getattr(supabase_client, "SUPABASE_SERVICE_KEY", None)
        try:
            supabase_client.SUPABASE_SERVICE_KEY = None
            with self.assertRaises(Exception) as ctx:
                asyncio.run(self.main.ready())
            exc = ctx.exception
            self.assertEqual(getattr(exc, "status_code", None), 503)
            self.assertEqual(exc.detail["status"], "not_ready")
            self.assertIn("supabase", exc.detail["failed_checks"])
        finally:
            supabase_client.SUPABASE_SERVICE_KEY = orig_key


if __name__ == "__main__":
    unittest.main()
