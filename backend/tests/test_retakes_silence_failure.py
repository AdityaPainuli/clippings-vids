import unittest
from unittest.mock import patch

from captions import retakes


class RetakeSilenceFailureTests(unittest.TestCase):
    def _words(self):
        return [
            {"start": 0.0, "end": 0.4, "text": "this"},
            {"start": 0.4, "end": 0.8, "text": "is"},
            {"start": 0.8, "end": 1.2, "text": "the"},
            {"start": 1.2, "end": 1.6, "text": "exact"},
            {"start": 1.6, "end": 2.0, "text": "line"},
            {"start": 2.0, "end": 2.4, "text": "that."},
            {"start": 3.0, "end": 3.4, "text": "this"},
            {"start": 3.4, "end": 3.8, "text": "is"},
            {"start": 3.8, "end": 4.2, "text": "the"},
            {"start": 4.2, "end": 4.6, "text": "exact"},
            {"start": 4.6, "end": 5.0, "text": "line"},
            {"start": 5.0, "end": 5.4, "text": "again."},
        ]

    def test_silence_analysis_failure_is_explicit(self):
        calls = []

        def complete(*args, **kwargs):
            calls.append((args, kwargs))
            return '{"retake": false}'

        with patch.object(
            retakes.tighten,
            "detect_silences_from_audio",
            side_effect=RuntimeError("ffmpeg exited with status 1"),
        ):
            result = retakes.detect(
                self._words(),
                media_path="broken.mp4",
                complete=complete,
            )

        self.assertEqual(result["status"], "silence-error")
        self.assertIn("ffmpeg exited with status 1", result["error"])
        self.assertEqual(result["cuts"], [])
        self.assertEqual(result["groups"], [])
        self.assertEqual(result["asked"], 0)
        self.assertEqual(result["skipped"], 0)
        self.assertEqual(calls, [])

    def test_silence_analysis_failure_does_not_use_punctuation_fallback(self):
        calls = []

        def complete(*args, **kwargs):
            calls.append((args, kwargs))
            return '{"retake": false}'

        with patch.object(
            retakes.tighten,
            "detect_silences_from_audio",
            side_effect=RuntimeError("silencedetect failed"),
        ):
            result = retakes.detect(
                self._words(),
                media_path="broken.mp4",
                complete=complete,
            )

        self.assertEqual(result["status"], "silence-error")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
