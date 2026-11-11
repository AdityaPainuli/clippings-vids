import asyncio
import importlib
import os
import sys
import tempfile
import types
import unittest


BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)


def _install_main_stubs():
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

    class BackgroundTasks:
        def __init__(self):
            self.tasks = []

        def add_task(self, func, *args, **kwargs):
            self.tasks.append((func, args, kwargs))

    fastapi.FastAPI = FastAPI
    fastapi.UploadFile = object
    fastapi.File = lambda default=None, *args, **kwargs: default
    fastapi.Form = lambda default=None, *args, **kwargs: default
    fastapi.HTTPException = HTTPException
    fastapi.BackgroundTasks = BackgroundTasks
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
    supabase_client.new_auth_client = lambda: object()
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


class FakeUploadFile:
    def __init__(self, filename, chunks=None, error_on_read=False):
        self.filename = filename
        self.chunks = list(chunks if chunks is not None else [b"dummy video data"])
        self.read_chunk_sizes = []
        self.error_on_read = error_on_read
        self.closed = False

    async def read(self, size=-1):
        self.read_chunk_sizes.append(size)
        if self.error_on_read:
            raise IOError("Simulated disk/network read failure")
        if self.chunks:
            return self.chunks.pop(0)
        return b""

    async def close(self):
        self.closed = True


class UploadSanitizationAndStreamingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_main_stubs()
        cls.main = importlib.import_module("main")

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.original_upload_dir = self.main.UPLOAD_DIR
        self.main.UPLOAD_DIR = self.test_dir.name
        self.dummy_user = {"user_id": "test-user-123"}

    def tearDown(self):
        self.main.UPLOAD_DIR = self.original_upload_dir
        self.test_dir.cleanup()

    def test_path_traversal_filename_sanitized(self):
        # Traversal payload like ../../../tmp/evil.mp4 should be sanitized
        fake_file = FakeUploadFile(filename="../../../tmp/evil.mp4", chunks=[b"sample content"])
        bg_tasks = self.main.BackgroundTasks()

        res = asyncio.run(
            self.main.upload_video(
                background_tasks=bg_tasks,
                file=fake_file,
                user=self.dummy_user,
            )
        )

        job_id = res["job_id"]
        # The file should be stored directly inside UPLOAD_DIR as <job_id>_evil.mp4
        expected_path = os.path.join(self.main.UPLOAD_DIR, f"{job_id}_evil.mp4")
        self.assertTrue(os.path.exists(expected_path), f"Expected file at {expected_path}")

        # Ensure no file was written outside UPLOAD_DIR
        with open(expected_path, "rb") as f:
            self.assertEqual(f.read(), b"sample content")

        self.assertTrue(fake_file.closed)

    def test_windows_path_traversal_sanitized(self):
        fake_file = FakeUploadFile(filename=r"..\..\..\evil.mp4", chunks=[b"content"])
        bg_tasks = self.main.BackgroundTasks()

        res = asyncio.run(
            self.main.upload_video(
                background_tasks=bg_tasks,
                file=fake_file,
                user=self.dummy_user,
            )
        )

        job_id = res["job_id"]
        expected_path = os.path.join(self.main.UPLOAD_DIR, f"{job_id}_evil.mp4")
        self.assertTrue(os.path.exists(expected_path))
        self.assertTrue(fake_file.closed)

    def test_missing_or_empty_filename_fallback(self):
        fake_file = FakeUploadFile(filename=None, chunks=[b"video data"])
        bg_tasks = self.main.BackgroundTasks()

        res = asyncio.run(
            self.main.upload_video(
                background_tasks=bg_tasks,
                file=fake_file,
                user=self.dummy_user,
            )
        )

        job_id = res["job_id"]
        expected_path = os.path.join(self.main.UPLOAD_DIR, f"{job_id}_video.mp4")
        self.assertTrue(os.path.exists(expected_path))
        self.assertTrue(fake_file.closed)

    def test_streaming_chunked_reads(self):
        # 3 chunks of data
        fake_file = FakeUploadFile(
            filename="large_video.mp4",
            chunks=[b"chunk1", b"chunk2", b"chunk3"],
        )
        bg_tasks = self.main.BackgroundTasks()

        res = asyncio.run(
            self.main.upload_video(
                background_tasks=bg_tasks,
                file=fake_file,
                user=self.dummy_user,
            )
        )

        job_id = res["job_id"]
        saved_path = os.path.join(self.main.UPLOAD_DIR, f"{job_id}_large_video.mp4")
        with open(saved_path, "rb") as f:
            self.assertEqual(f.read(), b"chunk1chunk2chunk3")

        # Verify reads were chunked with 1 MiB (1048576 bytes)
        self.assertGreater(len(fake_file.read_chunk_sizes), 0)
        for chunk_size in fake_file.read_chunk_sizes:
            self.assertEqual(chunk_size, 1024 * 1024)
        self.assertTrue(fake_file.closed)

    def test_cleanup_on_upload_failure(self):
        fake_file = FakeUploadFile(
            filename="broken.mp4",
            chunks=[b"partial content"],
            error_on_read=True,
        )
        bg_tasks = self.main.BackgroundTasks()

        with self.assertRaises(self.main.HTTPException) as ctx:
            asyncio.run(
                self.main.upload_video(
                    background_tasks=bg_tasks,
                    file=fake_file,
                    user=self.dummy_user,
                )
            )

        self.assertEqual(ctx.exception.status_code, 500)
        # Ensure directory is empty (partial file cleaned up)
        remaining_files = os.listdir(self.main.UPLOAD_DIR)
        self.assertEqual(remaining_files, [])
        self.assertTrue(fake_file.closed)


if __name__ == "__main__":
    unittest.main()
