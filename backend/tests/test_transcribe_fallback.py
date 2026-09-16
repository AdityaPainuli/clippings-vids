import unittest
from unittest.mock import patch

from captions import transcribe


class TranscriptionFallbackTests(unittest.TestCase):
    def test_runtime_failure_falls_through_to_next_backend(self):
        first = patch.object(
            transcribe, "_transcribe_mlx", side_effect=RuntimeError("mlx crashed")
        )
        second = patch.object(
            transcribe,
            "_transcribe_faster_whisper",
            side_effect=RuntimeError("ctranslate2 crashed"),
        )
        third = patch.object(
            transcribe,
            "_transcribe_openai_whisper",
            return_value={"language": "hi", "backend": "whisper:small", "words": []},
        )

        with first as mlx, second as faster, third as whisper:
            result = transcribe._transcribe_with_fallbacks("audio.wav", "hi")

        self.assertEqual(result["backend"], "whisper:small")
        mlx.assert_called_once_with("audio.wav", "hi")
        faster.assert_called_once_with("audio.wav", "hi")
        whisper.assert_called_once_with("audio.wav", "hi")

    def test_missing_backend_does_not_stop_fallback_chain(self):
        with patch.object(transcribe, "_transcribe_mlx", return_value=None) as mlx, patch.object(
            transcribe,
            "_transcribe_faster_whisper",
            return_value={"language": "hi", "backend": "faster-whisper:small", "words": []},
        ) as faster, patch.object(transcribe, "_transcribe_openai_whisper") as whisper:
            result = transcribe._transcribe_with_fallbacks("audio.wav", "hi")

        self.assertEqual(result["backend"], "faster-whisper:small")
        mlx.assert_called_once_with("audio.wav", "hi")
        faster.assert_called_once_with("audio.wav", "hi")
        whisper.assert_not_called()

    def test_all_backend_failures_raise_a_useful_error(self):
        with patch.object(
            transcribe, "_transcribe_mlx", side_effect=RuntimeError("mlx crashed")
        ) as mlx, patch.object(
            transcribe,
            "_transcribe_faster_whisper",
            side_effect=RuntimeError("ctranslate2 crashed"),
        ) as faster, patch.object(
            transcribe, "_transcribe_openai_whisper", side_effect=ImportError("whisper missing")
        ) as whisper:
            with self.assertRaisesRegex(RuntimeError, "All transcription backends failed") as ctx:
                transcribe._transcribe_with_fallbacks("audio.wav", None)

        message = str(ctx.exception)
        self.assertIn("_transcribe_mlx: mlx crashed", message)
        self.assertIn("_transcribe_faster_whisper: ctranslate2 crashed", message)
        self.assertIn("_transcribe_openai_whisper: whisper missing", message)
        mlx.assert_called_once_with("audio.wav", None)
        faster.assert_called_once_with("audio.wav", None)
        whisper.assert_called_once_with("audio.wav", None)

    def test_no_backend_available_has_distinct_error(self):
        with patch.object(transcribe, "_transcribe_mlx", return_value=None), patch.object(
            transcribe, "_transcribe_faster_whisper", return_value=None
        ), patch.object(transcribe, "_transcribe_openai_whisper", return_value=None):
            with self.assertRaisesRegex(RuntimeError, "No transcription backend is available"):
                transcribe._transcribe_with_fallbacks("audio.wav", None)


if __name__ == "__main__":
    unittest.main()
