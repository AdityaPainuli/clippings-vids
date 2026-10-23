import asyncio
import os
import sys
import threading
import time
import types
import unittest
from unittest import mock

from fastapi import HTTPException
from captions import transcribe
from localapp import assets, server


class TranscriptionReadyTest(unittest.TestCase):
    def test_requires_the_active_model_marker(self):
        with mock.patch.object(assets, "_mlx_available", return_value=False), \
                mock.patch.object(assets, "active_model", return_value="medium"), \
                mock.patch.object(assets, "model_installed",
                                  side_effect=lambda m: m == "small"):
            self.assertFalse(assets.transcription_ready())
        with mock.patch.object(assets, "_mlx_available", return_value=False), \
                mock.patch.object(assets, "active_model", return_value="medium"), \
                mock.patch.object(assets, "model_installed",
                                  side_effect=lambda m: m == "medium"):
            self.assertTrue(assets.transcription_ready())

    def test_mlx_does_not_need_a_cpu_model(self):
        with mock.patch.object(assets, "_mlx_available", return_value=True), \
                mock.patch.object(assets, "model_installed", return_value=False):
            self.assertTrue(assets.transcription_ready())


class TranscribeEndpointGuardTest(unittest.TestCase):
    def test_refuses_before_reading_the_upload(self):
        upload = mock.Mock()
        upload.filename = "clip.mp4"
        with mock.patch.object(assets, "ffmpeg_ready", return_value=True), \
                mock.patch.object(assets, "transcription_ready", return_value=False), \
                mock.patch.object(assets, "active_model", return_value="medium"):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(server.transcribe_endpoint(
                    file=upload, language=None, hinglish=True))
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertIn("medium", ctx.exception.detail)
        upload.read.assert_not_called()


class ModelCacheLockTest(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {"CAPTIONS_CPU_MODEL": "tiny"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def _race(self, loader):
        barrier = threading.Barrier(2)

        def run():
            barrier.wait()
            loader()

        threads = [threading.Thread(target=run) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

    def test_faster_whisper_loads_once(self):
        built = []

        class FakeModel:
            def __init__(self, name, device, compute_type):
                built.append(name)
                time.sleep(0.1)

        fake = types.ModuleType("faster_whisper")
        fake.WhisperModel = FakeModel
        with mock.patch.dict(sys.modules, {"faster_whisper": fake}), \
                mock.patch.object(transcribe, "_fw_model_cache", None):
            self._race(transcribe._faster_whisper_model)
        self.assertEqual(built, ["tiny"])

    def test_openai_whisper_loads_once(self):
        loaded = []

        class FakeLoaded:
            def transcribe(self, *args, **kwargs):
                return {"language": "en", "segments": []}

        def load_model(name):
            loaded.append(name)
            time.sleep(0.1)
            return FakeLoaded()

        fake = types.ModuleType("whisper")
        fake.load_model = load_model
        with mock.patch.dict(sys.modules, {"whisper": fake}), \
                mock.patch.object(transcribe, "_cpu_model_cache", None):
            self._race(lambda: transcribe._transcribe_openai_whisper(
                "unused.wav", None))
        self.assertEqual(loaded, ["tiny"])


if __name__ == "__main__":
    unittest.main()
