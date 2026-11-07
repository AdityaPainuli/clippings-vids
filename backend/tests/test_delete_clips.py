import asyncio
import importlib.util
import os
import sys
import types
import unittest
from unittest import mock

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)


def _load_fresh(name, filename, stubs):
    """Import a backend module under a private name with `stubs` in sys.modules
    only during the import, so other suites' stubs don't leak in or out."""
    spec = importlib.util.spec_from_file_location(name, os.path.join(BACKEND_DIR, filename))
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


class _Query:
    """Records a supabase-py table query chain and returns canned rows."""

    def __init__(self, table, log, rows):
        self.table, self.log, self.rows = table, log, rows
        self.calls = []

    def __getattr__(self, method):
        def record(*args):
            self.calls.append((method, args))
            return self
        return record

    def execute(self):
        self.log.append((self.table, self.calls))
        return types.SimpleNamespace(data=self.rows if self.calls[0][0] == "select" else [])


class DeleteJobClipsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        supabase = types.ModuleType("supabase")
        supabase.Client = object
        supabase.create_client = lambda *a, **k: object()
        supabase.ClientOptions = lambda **k: k
        dotenv = types.ModuleType("dotenv")
        dotenv.load_dotenv = lambda: None
        env = {"SUPABASE_URL": "https://example.supabase.co", "SUPABASE_SERVICE_KEY": "k"}
        with mock.patch.dict(os.environ, env):
            cls.sc = _load_fresh("_supabase_client_under_test", "supabase_client.py",
                                 {"supabase": supabase, "dotenv": dotenv})

    def _run(self, rows, known=()):
        log = []
        client = types.SimpleNamespace(table=lambda name: _Query(name, log, rows))
        with mock.patch.object(self.sc, "supabase", client), \
                mock.patch.object(self.sc, "_delete_paths") as delete_paths:
            paths = self.sc.delete_job_clips("user-1", "job-1", known)
        return paths, delete_paths, log

    def test_deletes_storage_objects_and_metadata_rows(self):
        rows = [{"storage_path": "user-1/job-1/a.mp4"}, {"storage_path": "user-1/job-1/b.mp4"}]
        paths, delete_paths, log = self._run(rows)

        self.assertEqual(paths, ["user-1/job-1/a.mp4", "user-1/job-1/b.mp4"])
        delete_paths.assert_called_once_with(paths)
        select, delete = log
        self.assertIn(("eq", ("user_id", "user-1")), select[1])
        self.assertIn(("eq", ("job_id", "job-1")), select[1])
        self.assertEqual(delete[1][0], ("delete", ()))
        self.assertIn(("eq", ("user_id", "user-1")), delete[1])
        self.assertIn(("in_", ("storage_path", paths)), delete[1])

    def test_known_paths_outside_the_users_folder_are_ignored(self):
        paths, _, _ = self._run([], known=["user-1/old-job/a.mp4", "user-2/job/x.mp4"])
        self.assertEqual(paths, ["user-1/old-job/a.mp4"])

    def test_nothing_found_touches_nothing(self):
        paths, delete_paths, log = self._run([])
        self.assertEqual(paths, [])
        delete_paths.assert_not_called()
        self.assertEqual(len(log), 1)  # the select only


class DeleteClipsEndpointTests(unittest.TestCase):
    class _HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    @classmethod
    def setUpClass(cls):
        class _App:
            def include_router(self, router): pass
            def add_middleware(self, *a, **k): pass
            def get(self, *a, **k): return lambda f: f
            def post(self, *a, **k): return lambda f: f
            def delete(self, *a, **k): return lambda f: f

        fastapi = types.ModuleType("fastapi")
        fastapi.FastAPI = _App
        fastapi.HTTPException = cls._HTTPException
        for name in ("UploadFile", "BackgroundTasks", "Request"):
            setattr(fastapi, name, object)
        for name in ("File", "Form", "Depends"):
            setattr(fastapi, name, lambda *a, **k: None)
        security = types.ModuleType("fastapi.security")
        security.HTTPBearer = lambda *a, **k: object()
        security.HTTPAuthorizationCredentials = object
        clipper = types.ModuleType("clipper")
        clipper.CLIP_STYLES = {"auto": "Auto"}
        clipper.CAPTION_PRESETS = {"default": {}}
        captions = types.ModuleType("captions")
        captions.__path__ = []
        captions_api = types.ModuleType("captions.api")
        captions_api.router = object()

        cls.main = _load_fresh("_main_delete_under_test", "main.py", {
            "fastapi": fastapi,
            "fastapi.middleware": types.ModuleType("fastapi.middleware"),
            "fastapi.middleware.cors": mock.MagicMock(),
            "fastapi.security": security,
            "fastapi.responses": mock.MagicMock(),
            "clipper": clipper,
            "supabase_client": mock.MagicMock(),
            "captions": captions,
            "captions.api": captions_api,
        })

    def setUp(self):
        self.main.jobs.clear()
        self.main._clip_cache.clear()

    def _delete(self, job_id, deleted, user="user-1"):
        client = types.ModuleType("supabase_client")
        client.delete_job_clips = mock.Mock(return_value=deleted)
        with mock.patch.dict(sys.modules, {"supabase_client": client}):
            result = asyncio.run(self.main.delete_clips(job_id, user={"user_id": user}))
        return result, client.delete_job_clips

    def test_clips_listed_after_the_job_expired_can_still_be_deleted(self):
        # No in-memory job (restart or JOB_TTL), but clip_metadata still has rows.
        result, delete_job_clips = self._delete("job-1", ["user-1/job-1/a.mp4"])
        self.assertEqual(result, {"deleted_clips": 1})
        delete_job_clips.assert_called_once_with("user-1", "job-1", [])

    def test_unknown_job_with_no_clips_is_404(self):
        with self.assertRaises(self._HTTPException) as ctx:
            self._delete("nope", [])
        self.assertEqual(ctx.exception.status_code, 404)

    def test_someone_elses_job_is_403(self):
        self.main.jobs["job-1"] = {"user_id": "user-2", "results": []}
        with self.assertRaises(self._HTTPException) as ctx:
            self._delete("job-1", ["user-1/job-1/a.mp4"])
        self.assertEqual(ctx.exception.status_code, 403)

    def test_cached_results_pointing_at_deleted_clips_are_evicted(self):
        clip = {"storage_path": "user-1/job-1/a.mp4"}
        self.main.jobs["job-1"] = {"user_id": "user-1", "results": [clip]}
        self.main._clip_cache["same-url"] = [clip]
        self.main._clip_cache["other-url"] = [{"storage_path": "user-1/job-2/b.mp4"}]

        _, delete_job_clips = self._delete("job-1", ["user-1/job-1/a.mp4"])

        delete_job_clips.assert_called_once_with("user-1", "job-1", ["user-1/job-1/a.mp4"])
        self.assertNotIn("same-url", self.main._clip_cache)
        self.assertIn("other-url", self.main._clip_cache)
        self.assertNotIn("job-1", self.main.jobs)


if __name__ == "__main__":
    unittest.main()
