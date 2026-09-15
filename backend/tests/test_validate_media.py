"""Tests for the media input validation module."""

import json
import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from validate_media import validate_media_file, MediaValidationError


def _ffprobe_result(streams, fmt=None, returncode=0):
    """Build a mock CompletedProcess matching ffprobe JSON output."""
    probe = {"streams": streams, "format": fmt or {"duration": "10.0", "format_name": "mov,mp4"}}
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=json.dumps(probe), stderr=""
    )


class TestValidateMediaFile(unittest.TestCase):

    def test_missing_file_raises(self):
        with self.assertRaises(MediaValidationError) as ctx:
            validate_media_file("/nonexistent/path.mp4")
        self.assertIn("not found", str(ctx.exception).lower())

    def test_none_path_raises(self):
        with self.assertRaises(MediaValidationError):
            validate_media_file(None)

    def test_empty_file_raises(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
            path = f.name
        try:
            with self.assertRaises(MediaValidationError) as ctx:
                validate_media_file(path)
            self.assertIn("empty", str(ctx.exception).lower())
        finally:
            os.unlink(path)

    @patch("validate_media.subprocess.run")
    def test_valid_video_returns_info(self, mock_run):
        mock_run.return_value = _ffprobe_result([
            {"codec_type": "video", "codec_name": "h264"},
            {"codec_type": "audio", "codec_name": "aac"},
        ])
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
            f.write(b"\x00" * 100)
            path = f.name
        try:
            info = validate_media_file(path)
            self.assertTrue(info["has_video"])
            self.assertTrue(info["has_audio"])
            self.assertEqual(info["video_codec"], "h264")
            self.assertEqual(info["audio_codec"], "aac")
            self.assertEqual(info["duration"], 10.0)
        finally:
            os.unlink(path)

    @patch("validate_media.subprocess.run")
    def test_no_streams_raises(self, mock_run):
        mock_run.return_value = _ffprobe_result([])
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
            f.write(b"\x00" * 100)
            path = f.name
        try:
            with self.assertRaises(MediaValidationError) as ctx:
                validate_media_file(path)
            self.assertIn("no media streams", str(ctx.exception).lower())
        finally:
            os.unlink(path)

    @patch("validate_media.subprocess.run")
    def test_unsupported_video_codec_raises(self, mock_run):
        mock_run.return_value = _ffprobe_result([
            {"codec_type": "video", "codec_name": "rawvideo"},
        ])
        with tempfile.NamedTemporaryFile(delete=False, suffix=".avi") as f:
            f.write(b"\x00" * 100)
            path = f.name
        try:
            with self.assertRaises(MediaValidationError) as ctx:
                validate_media_file(path)
            self.assertIn("unsupported video codec", str(ctx.exception).lower())
        finally:
            os.unlink(path)

    @patch("validate_media.subprocess.run")
    def test_ffprobe_failure_raises(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="error"
        )
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"not a media file")
            path = f.name
        try:
            with self.assertRaises(MediaValidationError) as ctx:
                validate_media_file(path)
            self.assertIn("not a recognised", str(ctx.exception).lower())
        finally:
            os.unlink(path)

    @patch("validate_media.subprocess.run")
    def test_require_audio_without_audio_raises(self, mock_run):
        mock_run.return_value = _ffprobe_result([
            {"codec_type": "video", "codec_name": "h264"},
        ])
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
            f.write(b"\x00" * 100)
            path = f.name
        try:
            with self.assertRaises(MediaValidationError):
                validate_media_file(path, require_audio=True)
        finally:
            os.unlink(path)

    @patch("validate_media.subprocess.run", side_effect=FileNotFoundError)
    def test_ffprobe_not_installed_raises(self, mock_run):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
            f.write(b"\x00" * 100)
            path = f.name
        try:
            with self.assertRaises(MediaValidationError) as ctx:
                validate_media_file(path)
            self.assertIn("ffprobe", str(ctx.exception).lower())
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
