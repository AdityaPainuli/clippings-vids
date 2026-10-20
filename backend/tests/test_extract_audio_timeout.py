import os
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions import transcribe


class FakeRun:
    """Stands in for subprocess.run: ffprobe reports `duration`, ffmpeg
    behaves like a hung process that only a timeout can stop."""

    def __init__(self, duration="42.5", probe_returncode=0, ffmpeg_hangs=True):
        self.duration = duration
        self.probe_returncode = probe_returncode
        self.ffmpeg_hangs = ffmpeg_hangs
        self.ffmpeg_timeout = None

    def __call__(self, cmd, **kwargs):
        if cmd[0] == "ffprobe":
            return subprocess.CompletedProcess(cmd, self.probe_returncode, self.duration, "")
        self.ffmpeg_timeout = kwargs.get("timeout")
        if self.ffmpeg_hangs:
            if self.ffmpeg_timeout is None:
                raise AssertionError("ffmpeg ran with no timeout and would hang forever")
            raise subprocess.TimeoutExpired(cmd, self.ffmpeg_timeout)
        return subprocess.CompletedProcess(cmd, 1, "", "moov atom not found")


class ExtractAudioTimeoutTests(unittest.TestCase):
    def test_hung_ffmpeg_raises_timeout_error(self):
        fake = FakeRun()
        with patch.object(transcribe.subprocess, "run", fake):
            with self.assertRaises(RuntimeError) as ctx:
                transcribe._extract_audio("in.mp4", "out.wav")
        self.assertIn("timed out", str(ctx.exception))
        self.assertIsNotNone(fake.ffmpeg_timeout)

    def test_timeout_scales_with_duration(self):
        fake = FakeRun(duration="600.0")
        with patch.object(transcribe.subprocess, "run", fake):
            with self.assertRaises(RuntimeError):
                transcribe._extract_audio("in.mp4", "out.wav")
        self.assertEqual(fake.ffmpeg_timeout, transcribe._EXTRACT_MIN_TIMEOUT + 600.0)

    def test_unknown_duration_uses_fixed_cap(self):
        for fake in (FakeRun(duration="N/A"), FakeRun(probe_returncode=1)):
            with patch.object(transcribe.subprocess, "run", fake):
                with self.assertRaises(RuntimeError):
                    transcribe._extract_audio("in.mp4", "out.wav")
            self.assertEqual(fake.ffmpeg_timeout, transcribe._EXTRACT_FALLBACK_TIMEOUT)

    def test_ordinary_failure_keeps_stderr(self):
        fake = FakeRun(ffmpeg_hangs=False)
        with patch.object(transcribe.subprocess, "run", fake):
            with self.assertRaises(RuntimeError) as ctx:
                transcribe._extract_audio("in.mp4", "out.wav")
        self.assertIn("Audio extraction failed", str(ctx.exception))
        self.assertIn("moov atom not found", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
