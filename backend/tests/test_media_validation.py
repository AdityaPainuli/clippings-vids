import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from media_validation import MediaValidationError, validate_media_file


class MediaValidationTests(unittest.TestCase):
    def test_missing_file_is_rejected_before_probe(self):
        with patch("media_validation.subprocess.run") as run:
            with self.assertRaisesRegex(MediaValidationError, "does not exist"):
                validate_media_file("/missing/video.mp4")
        run.assert_not_called()

    def test_empty_file_is_rejected_before_probe(self):
        with tempfile.NamedTemporaryFile() as media:
            with patch("media_validation.subprocess.run") as run:
                with self.assertRaisesRegex(MediaValidationError, "empty"):
                    validate_media_file(media.name)
        run.assert_not_called()

    def test_corrupted_or_unsupported_file_has_clear_error(self):
        with tempfile.NamedTemporaryFile() as media:
            media.write(b"not media")
            media.flush()
            failure = subprocess.CalledProcessError(
                1, ["ffprobe"], stderr="Invalid data found when processing input"
            )
            with patch("media_validation.subprocess.run", side_effect=failure):
                with self.assertRaisesRegex(
                    MediaValidationError, "unsupported or corrupted"
                ):
                    validate_media_file(media.name)

    def test_file_without_required_video_stream_is_rejected(self):
        metadata = {
            "format": {"format_name": "mp3"},
            "streams": [{"codec_type": "audio"}],
        }
        with tempfile.NamedTemporaryFile() as media:
            media.write(b"audio")
            media.flush()
            with patch(
                "media_validation.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    ["ffprobe"], 0, stdout=json.dumps(metadata), stderr=""
                ),
            ):
                with self.assertRaisesRegex(MediaValidationError, "video stream"):
                    validate_media_file(media.name)

    def test_valid_video_is_accepted_independent_of_extension(self):
        metadata = {
            "format": {"format_name": "matroska,webm"},
            "streams": [
                {"codec_type": "video"},
                {"codec_type": "audio"},
            ],
        }
        with tempfile.NamedTemporaryFile(suffix=".txt") as media:
            media.write(b"real media bytes")
            media.flush()
            with patch(
                "media_validation.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    ["ffprobe"], 0, stdout=json.dumps(metadata), stderr=""
                ),
            ) as run:
                result = validate_media_file(media.name)

        self.assertEqual(result, metadata)
        self.assertIn("format=format_name,format_long_name:stream=codec_type", run.call_args.args[0])

    def test_missing_ffprobe_has_clear_error(self):
        with tempfile.NamedTemporaryFile() as media:
            media.write(b"media")
            media.flush()
            with patch("media_validation.subprocess.run", side_effect=FileNotFoundError):
                with self.assertRaisesRegex(MediaValidationError, "ffprobe not found"):
                    validate_media_file(media.name)

    def test_non_object_probe_metadata_is_rejected(self):
        with tempfile.NamedTemporaryFile() as media:
            media.write(b"media")
            media.flush()
            with patch(
                "media_validation.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    ["ffprobe"], 0, stdout="[]", stderr=""
                ),
            ):
                with self.assertRaisesRegex(MediaValidationError, "invalid media metadata"):
                    validate_media_file(media.name)


if __name__ == "__main__":
    unittest.main()
