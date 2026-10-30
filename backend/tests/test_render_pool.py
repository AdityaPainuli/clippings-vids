
import os
import sys
import tempfile
import threading
import unittest
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# Load the backend module without requiring heavy rendering packages.
BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

for module_name in [
    "yt_dlp",
    "google",
    "google.generativeai",
    "moviepy",
    "moviepy.editor",
    "dotenv",
    "whisper",
    "cv2",
]:
    sys.modules.setdefault(module_name, MagicMock())

import clipper  # noqa: E402


def sample_metadata(count=1):
    return [
        {
            "start_time": float(index * 10),
            "end_time": float(index * 10 + 5),
            "description": f"test clip {index}",
            "hook": "",
            "virality_score": 1,
            "clip_type": "test",
        }
        for index in range(count)
    ]


class RenderPoolTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.probe = patch.object(
            clipper.subprocess,
            "run",
            return_value=SimpleNamespace(
                stdout="120",
                stderr="",
                returncode=0,
            ),
        )
        self.probe.start()
        self.addCleanup(self.probe.stop)

        self.semaphore = patch.object(
            clipper,
            "_RENDER_POOL_SEMAPHORE",
            threading.BoundedSemaphore(1),
        )
        self.semaphore.start()
        self.addCleanup(self.semaphore.stop)

    def test_worker_limit_and_start_callback(self):
        class ImmediateExecutor:
            instances = []

            def __init__(self, max_workers):
                self.max_workers = max_workers
                self.submitted = []
                type(self).instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def submit(self, function, task):
                self.submitted.append(task)
                future = Future()
                future.set_result((task[3], None))
                return future

        ImmediateExecutor.instances = []
        callback_counts = []

        with patch.object(
            clipper, "CLIP_RENDER_WORKERS", 2
        ), patch.object(
            clipper, "ProcessPoolExecutor", ImmediateExecutor
        ):
            created, failed = clipper.create_clips(
                "input.mp4",
                sample_metadata(3),
                output_dir=self.temp_dir.name,
                on_start=lambda: callback_counts.append(
                    len(ImmediateExecutor.instances[-1].submitted)
                ),
            )

        self.assertEqual(len(created), 3)
        self.assertEqual(failed, [])
        self.assertEqual(len(ImmediateExecutor.instances), 1)
        self.assertEqual(ImmediateExecutor.instances[0].max_workers, 2)
        self.assertEqual(callback_counts, [3])

    def test_concurrent_jobs_do_not_overlap_pools(self):
        class TrackingSemaphore:
            def __init__(self):
                self.inner = threading.BoundedSemaphore(1)
                self.waiting = threading.Event()

            def __enter__(self):
                if not self.inner.acquire(blocking=False):
                    self.waiting.set()
                    self.inner.acquire()
                return self

            def __exit__(self, *args):
                self.inner.release()

        semaphore = TrackingSemaphore()

        class BlockingExecutor:
            instances = []
            lock = threading.Lock()
            active = 0
            peak_active = 0
            first_submit = threading.Event()
            release_first = threading.Event()

            def __init__(self, max_workers):
                self.max_workers = max_workers
                self.submitted = []
                type(self).instances.append(self)

            def __enter__(self):
                with type(self).lock:
                    type(self).active += 1
                    type(self).peak_active = max(
                        type(self).peak_active, type(self).active
                    )
                return self

            def __exit__(self, *args):
                with type(self).lock:
                    type(self).active -= 1
                return False

            def submit(self, function, task):
                self.submitted.append(task)

                if task[0] == "first.mp4":
                    type(self).first_submit.set()
                    if not type(self).release_first.wait(5):
                        raise TimeoutError("Test release timed out")

                future = Future()
                future.set_result((task[3], None))
                return future

        BlockingExecutor.instances = []
        BlockingExecutor.active = 0
        BlockingExecutor.peak_active = 0
        BlockingExecutor.first_submit = threading.Event()
        BlockingExecutor.release_first = threading.Event()

        errors = []

        def render(video_path):
            try:
                clipper.create_clips(
                    video_path,
                    sample_metadata(1),
                    output_dir=self.temp_dir.name,
                )
            except Exception as exc:
                errors.append(exc)

        with patch.object(
            clipper, "_RENDER_POOL_SEMAPHORE", semaphore
        ), patch.object(
            clipper, "ProcessPoolExecutor", BlockingExecutor
        ):
            first = threading.Thread(target=render, args=("first.mp4",))
            second = threading.Thread(target=render, args=("second.mp4",))

            first.start()
            self.assertTrue(BlockingExecutor.first_submit.wait(3))

            second.start()
            self.assertTrue(semaphore.waiting.wait(3))

            # The second job must wait before creating its pool.
            self.assertEqual(len(BlockingExecutor.instances), 1)

            BlockingExecutor.release_first.set()
            first.join(5)
            second.join(5)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(BlockingExecutor.instances), 2)
        self.assertEqual(BlockingExecutor.peak_active, 1)


if __name__ == "__main__":
    unittest.main()
