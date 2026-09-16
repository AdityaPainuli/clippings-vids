import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from captions import transcribe


class FfmpegAudioExtractionTests(unittest.TestCase):
    def test_timeout_is_reported_as_audio_extraction_error(self):
        with patch.object(
            transcribe.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired(cmd=["ffmpeg"], timeout=transcribe.FFMPEG_TIMEOUT_SECONDS),
        ) as run:
            with self.assertRaisesRegex(RuntimeError, r"Audio extraction timed out after 300 seconds"):
                transcribe._extract_audio("input.mp4", "audio.wav")

        run.assert_called_once_with(
            ["ffmpeg", "-y", "-i", "input.mp4", "-ac", "1", "-ar", "16000", "-vn", "audio.wav"],
            capture_output=True,
            text=True,
            timeout=transcribe.FFMPEG_TIMEOUT_SECONDS,
        )

    def test_nonzero_ffmpeg_exit_remains_an_extraction_error(self):
        completed = subprocess.CompletedProcess(
            args=["ffmpeg"], returncode=1, stdout="", stderr="decoder failed"
        )
        with patch.object(transcribe.subprocess, "run", return_value=completed):
            with self.assertRaisesRegex(RuntimeError, r"Audio extraction failed: decoder failed"):
                transcribe._extract_audio("input.mp4", "audio.wav")


if __name__ == "__main__":
    unittest.main()
