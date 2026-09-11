import asyncio
import importlib
import os
import sys
import tempfile
import types
import unittest
from unittest import mock


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


class MainJobRaceTest(unittest.TestCase):
    def test_deleted_job_after_clip_render_cleans_files_and_does_not_write_final_state(self):
        _install_main_import_stubs()
        main = importlib.import_module("main")

        with tempfile.TemporaryDirectory() as tmpdir:
            upload_dir = os.path.join(tmpdir, "uploads")
            output_dir = os.path.join(tmpdir, "output")
            os.makedirs(upload_dir)
            os.makedirs(output_dir)
            video_path = os.path.join(upload_dir, "source.mp4")
            with open(video_path, "wb") as handle:
                handle.write(b"video")

            job_id = "same-id"
            orphaned_record = {"status": "queued", "created_at": 100.0, "user_id": "user-1"}
            main.UPLOAD_DIR = upload_dir
            main.OUTPUT_DIR = output_dir
            main.clipper_jobs.set(job_id, orphaned_record)

            def create_clips(*args, **kwargs):
                clip_path = os.path.join(output_dir, "clip.mp4")
                with open(clip_path, "wb") as handle:
                    handle.write(b"clip")
                main.clipper_jobs.delete(job_id)
                return ([{"filename": "clip.mp4"}], [])

            with mock.patch.object(main.clipper, "analyze_video", return_value=[{"start": 0, "end": 1}]):
                with mock.patch.object(main.clipper, "create_clips", side_effect=create_clips):
                    with self.assertLogs(main.logger, level="WARNING"):
                        asyncio.run(main.process_video_task(
                            job_id, video_path, None, "user-1", cache_key=None
                        ))

            self.assertIsNone(main.clipper_jobs.get(job_id))
            self.assertFalse(os.path.exists(video_path))
            self.assertFalse(os.path.exists(os.path.join(output_dir, "clip.mp4")))
            self.assertNotEqual(orphaned_record.get("status"), "completed")
            self.assertNotIn("results", orphaned_record)


if __name__ == "__main__":
    unittest.main()
