import asyncio
import importlib
import importlib.util
import os
import sys
import threading
import types
import unittest
from concurrent.futures import Future
from unittest import mock

try:
    from tests.test_main_job_race import _install_main_import_stubs
except ImportError:
    from test_main_job_race import _install_main_import_stubs

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)


def _load_clipper():
    stubs = {mod: mock.MagicMock() for mod in (
        "yt_dlp", "google", "google.generativeai", "moviepy", "dotenv")}
    spec = importlib.util.spec_from_file_location(
        "_clipper_render_pool_test", os.path.join(BACKEND_DIR, "clipper.py"))
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


class _FakeExecutor:
    instances = []

    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.submitted = []
        _FakeExecutor.instances.append(self)

    def submit(self, fn, task):
        self.submitted.append(task)
        future = Future()
        future.set_result((task[3], None))
        return future


class SharedRenderPoolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clipper = _load_clipper()

    def setUp(self):
        _FakeExecutor.instances.clear()
        self.pool = mock.patch.object(self.clipper, "_RENDER_POOL", None)
        self.pool.start()
        self.addCleanup(self.pool.stop)

    def _render(self):
        with mock.patch.object(self.clipper, "ProcessPoolExecutor",
                               _FakeExecutor), \
                mock.patch.object(self.clipper, "RENDER_WORKERS", 3), \
                mock.patch.object(self.clipper.subprocess, "run",
                                  return_value=types.SimpleNamespace(
                                      stdout="10.0")), \
                mock.patch.object(self.clipper.os, "makedirs"):
            return self.clipper.create_clips(
                "video.mp4",
                [{"start_time": 0, "end_time": 2},
                 {"start_time": 3, "end_time": 5}],
                "out", captions=False)

    def test_jobs_share_one_bounded_executor(self):
        self._render()
        self._render()

        self.assertEqual(len(_FakeExecutor.instances), 1)
        self.assertEqual(_FakeExecutor.instances[0].max_workers, 3)
        self.assertEqual(len(_FakeExecutor.instances[0].submitted), 4)


class _StateFuture:
    def __init__(self, state="pending"):
        self.state = state

    def running(self):
        return self.state == "running"

    def done(self):
        return self.state == "done"


class RenderStartWatcherTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.clipper = _load_clipper()

    def test_callback_fires_once_when_a_task_starts(self):
        future = _StateFuture()
        called = threading.Event()
        thread = threading.Thread(
            target=self.clipper._watch_render_start,
            args=([future], called.set), daemon=True)
        thread.start()
        self.assertFalse(called.is_set())
        future.state = "running"
        self.assertTrue(called.wait(1))
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive())

    def test_callback_fires_when_all_tasks_finish_before_a_poll(self):
        called = threading.Event()
        self.clipper._watch_render_start(
            [_StateFuture("done")], called.set)
        self.assertTrue(called.is_set())


class ProcessVideoRenderStatusTest(unittest.TestCase):
    def test_job_reports_queued_then_clipping(self):
        _install_main_import_stubs()
        main = importlib.import_module("main")
        main.jobs.clear()
        main._sse_subscribers.clear()
        main.jobs["job-1"] = {
            "status": "analyzing", "user_id": "user-1", "url": ""}
        events = asyncio.Queue()
        main._sse_subscribers["job-1"] = [events]

        def analyze(*args, **kwargs):
            return [{"start_time": 0, "end_time": 2}]

        def create(video, clips, out, captions, style, on_start):
            on_start()
            return [], []

        with mock.patch.object(main.clipper, "analyze_video", analyze), \
                mock.patch.object(main.clipper, "create_clips", create):
            asyncio.run(main.process_video_task(
                "job-1", "video.mp4", None, "user-1"))

        seen = []
        while not events.empty():
            seen.append(events.get_nowait().get("status"))
        self.assertIn("queued", seen)
        self.assertIn("clipping", seen)
        self.assertLess(seen.index("queued"), seen.index("clipping"))

    def test_late_render_callback_does_not_undo_a_later_status(self):
        _install_main_import_stubs()
        main = importlib.import_module("main")
        main.jobs.clear()
        main._sse_subscribers.clear()
        main.jobs["job-1"] = {"status": "uploading", "user_id": "user-1"}
        events = asyncio.Queue()
        main._sse_subscribers["job-1"] = [events]

        main._mark_render_started("job-1", 2)

        self.assertEqual(main.jobs["job-1"]["status"], "uploading")
        self.assertTrue(events.empty())


if __name__ == "__main__":
    unittest.main()
