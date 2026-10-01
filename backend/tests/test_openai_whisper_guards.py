import os
import sys
import types
import unittest
from unittest.mock import patch

from captions import transcribe


class FakeWhisperModel:
    def __init__(self):
        self.kwargs = None

    def transcribe(self, audio, **kwargs):
        self.kwargs = kwargs
        return {
            "language": "en",
            "segments": [
                {
                    "start": 0.0,
                    "end": 1.0,
                    "text": " hello ",
                    "words": [
                        {"start": 0.0, "end": 1.0, "word": " hello "},
                    ],
                }
            ],
        }


class OpenAIWhisperGuardsTests(unittest.TestCase):
    def setUp(self):
        transcribe._cpu_model_cache = None
        self.model = FakeWhisperModel()
        self.whisper_module = types.SimpleNamespace(
            load_model=lambda name: self.model,
        )

    def tearDown(self):
        transcribe._cpu_model_cache = None

    def test_legacy_whisper_uses_loop_and_hallucination_guards(self):
        with patch.dict(sys.modules, {"whisper": self.whisper_module}):
            with patch.dict(os.environ, {"CAPTIONS_CPU_MODEL": "small"}, clear=False):
                result = transcribe._transcribe_openai_whisper("audio.wav", "en")

        self.assertEqual(result["backend"], "whisper:small")
        self.assertEqual(result["words"], [{"start": 0.0, "end": 1.0, "text": "hello"}])
        self.assertFalse(self.model.kwargs["condition_on_previous_text"])
        self.assertEqual(self.model.kwargs["no_speech_threshold"], 0.6)
        self.assertEqual(self.model.kwargs["logprob_threshold"], -1.0)
        self.assertEqual(self.model.kwargs["compression_ratio_threshold"], 2.4)

    def test_legacy_whisper_forwards_language_and_preserves_cpu_decoding(self):
        with patch.dict(sys.modules, {"whisper": self.whisper_module}):
            with patch.dict(os.environ, {"CAPTIONS_CPU_MODEL": "medium"}, clear=False):
                result = transcribe._transcribe_openai_whisper("audio.wav", "hi")

        self.assertEqual(result["backend"], "whisper:medium")
        self.assertEqual(self.model.kwargs["language"], "hi")
        self.assertFalse(self.model.kwargs["fp16"])
        self.assertTrue(self.model.kwargs["word_timestamps"])
        self.assertIsNone(self.model.kwargs["verbose"])


if __name__ == "__main__":
    unittest.main()
