import os
import sys
import types
import unittest
from unittest.mock import patch

from captions import transcribe


class FakeWhisperModel:
    calls = []
    error = None

    def __init__(self, name, device, compute_type):
        type(self).calls.append((name, device, compute_type))
        if device == "cuda" and type(self).error is not None:
            raise type(self).error


class FasterWhisperGPUSelectionTests(unittest.TestCase):
    def setUp(self):
        transcribe._fw_model_cache = None
        FakeWhisperModel.calls = []
        FakeWhisperModel.error = None
        self.faster_whisper = types.ModuleType("faster_whisper")
        self.faster_whisper.WhisperModel = FakeWhisperModel
        self.ctranslate2 = types.ModuleType("ctranslate2")
        self.env = {"CAPTIONS_CPU_MODEL": "small"}

    def tearDown(self):
        transcribe._fw_model_cache = None

    def _load(self, cuda_count=0):
        self.ctranslate2.get_cuda_device_count = lambda: cuda_count
        with patch.dict(sys.modules, {
            "faster_whisper": self.faster_whisper,
            "ctranslate2": self.ctranslate2,
        }), patch.dict(os.environ, self.env, clear=False):
            return transcribe._faster_whisper_model()

    def test_no_cuda_device_uses_cpu(self):
        model = self._load(cuda_count=0)

        self.assertIsInstance(model, FakeWhisperModel)
        self.assertEqual(FakeWhisperModel.calls,
                         [("small", "cpu", "int8")])

    def test_cuda_device_uses_float16(self):
        model = self._load(cuda_count=1)

        self.assertIsInstance(model, FakeWhisperModel)
        self.assertEqual(FakeWhisperModel.calls,
                         [("small", "cuda", "float16")])

    def test_cuda_initialization_error_is_not_converted_to_cpu(self):
        FakeWhisperModel.error = RuntimeError("CUDA failed with error initialization error")

        self.ctranslate2.get_cuda_device_count = lambda: 1
        with patch.dict(sys.modules, {
            "faster_whisper": self.faster_whisper,
            "ctranslate2": self.ctranslate2,
        }), patch.dict(os.environ, self.env, clear=False):
            with self.assertRaisesRegex(RuntimeError, "CUDA failed with error initialization error"):
                transcribe._faster_whisper_model()

        self.assertEqual(FakeWhisperModel.calls,
                         [("small", "cuda", "float16")])
        self.assertIsNone(transcribe._fw_model_cache)

    def test_cuda_probe_error_is_not_converted_to_cpu(self):
        def probe():
            raise RuntimeError("CUDA runtime unavailable")

        self.ctranslate2.get_cuda_device_count = probe
        with patch.dict(sys.modules, {
            "faster_whisper": self.faster_whisper,
            "ctranslate2": self.ctranslate2,
        }), patch.dict(os.environ, self.env, clear=False):
            with self.assertRaisesRegex(RuntimeError, "CUDA runtime unavailable"):
                transcribe._faster_whisper_model()

        self.assertEqual(FakeWhisperModel.calls, [])
        self.assertIsNone(transcribe._fw_model_cache)


if __name__ == "__main__":
    unittest.main()
