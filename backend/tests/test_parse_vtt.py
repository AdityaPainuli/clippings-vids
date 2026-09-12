import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock heavy third-party imports that clipper.py pulls in at module level
# so we can test _parse_vtt_to_text in isolation.
_MOCKED_MODULES = [
    "yt_dlp", "google", "google.generativeai", "moviepy",
    "moviepy.editor", "dotenv", "whisper", "cv2",
]
for mod in _MOCKED_MODULES:
    sys.modules.setdefault(mod, mock.MagicMock())

from clipper import _parse_vtt_to_text  # noqa: E402

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")


class ParseVttSingleLineTest(unittest.TestCase):
    def test_single_line_cue(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "single_line.vtt"))
        self.assertEqual(
            result,
            "[00:00:01] This is a single line cue.\n"
            "[00:00:05] Another single line cue.",
        )

    def test_single_line_cue_preserves_timestamp(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "single_line.vtt"))
        self.assertIn("[00:00:01]", result)
        self.assertIn("[00:00:05]", result)


class ParseVttMultiLineTest(unittest.TestCase):
    def test_two_line_cue(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "two_line.vtt"))
        self.assertEqual(
            result,
            "[00:00:01] This is the first line of the cue and this is the second line.",
        )

    def test_three_plus_line_cue(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "three_plus_line.vtt"))
        self.assertEqual(
            result,
            "[00:00:01] This is line one this is line two "
            "and this is line three even a fourth line here.",
        )


class ParseVttMixedTest(unittest.TestCase):
    def test_mixed_single_and_multi_line(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "mixed_cues.vtt"))
        expected = (
            "[00:00:01] Single line cue one.\n"
            "[00:00:05] First line of multi-line cue second line of multi-line cue.\n"
            "[00:00:09] Single line cue two.\n"
            "[00:00:13] Three line cue."
        )
        self.assertEqual(result, expected)

    def test_mixed_cue_count(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "mixed_cues.vtt"))
        self.assertEqual(len(result.split("\n")), 4)


class ParseVttNumberedIdentifierTest(unittest.TestCase):
    def test_numbered_identifiers_skipped(self):
        result = _parse_vtt_to_text(os.path.join(FIXTURES_DIR, "numbered_identifiers.vtt"))
        self.assertEqual(
            result,
            "[00:00:01] First cue with identifier.\n"
            "[00:00:05] Second cue also with identifier.",
        )


class ParseVttStressTest(unittest.TestCase):
    """Adversarial edge cases not designed around the fix implementation."""

    def _write_vtt(self, content, binary=False):
        mode = "wb" if binary else "w"
        f = tempfile.NamedTemporaryFile(mode=mode, suffix=".vtt", delete=False)
        f.write(content)
        f.close()
        return f.name

    def test_single_cue_no_trailing_blank(self):
        path = self._write_vtt(
            "WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nHello.\n"
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(result, "[00:00:01] Hello.")
        finally:
            os.unlink(path)

    def test_consecutive_timestamps_no_text_between(self):
        path = self._write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "00:00:05.000 --> 00:00:08.000\n"
            "Only this cue has text.\n\n"
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(result, "[00:00:05] Only this cue has text.")
        finally:
            os.unlink(path)

    def test_cue_with_only_html_tags(self):
        path = self._write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "<b></b>\n\n"
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertIsNone(result)
        finally:
            os.unlink(path)

    def test_timestamps_with_hours(self):
        path = self._write_vtt(
            "WEBVTT\n\n"
            "01:23:45.000 --> 01:24:00.000\n"
            "Line one\nline two.\n\n"
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(result, "[01:23:45] Line one line two.")
        finally:
            os.unlink(path)

    def test_multiple_blank_lines_between_cues(self):
        path = self._write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Cue one.\n\n\n\n"
            "00:00:05.000 --> 00:00:08.000\n"
            "Cue two.\n\n"
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(
                result,
                "[00:00:01] Cue one.\n[00:00:05] Cue two.",
            )
        finally:
            os.unlink(path)

    def test_timestamp_only_no_text(self):
        path = self._write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n\n"
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertIsNone(result)
        finally:
            os.unlink(path)

    def test_ten_cues_all_parsed(self):
        cues = "\n\n".join(
            f"00:00:{i:02d}.000 --> 00:00:{i+1:02d}.000\nCue {i} text."
            for i in range(10)
        )
        path = self._write_vtt(f"WEBVTT\n\n{cues}\n\n")
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(len(result.split("\n")), 10)
            for i in range(10):
                self.assertIn(f"[00:00:{i:02d}] Cue {i} text.", result)
        finally:
            os.unlink(path)

    def test_mixed_line_endings(self):
        path = self._write_vtt(
            b"WEBVTT\r\n\r\n"
            b"00:00:01.000 --> 00:00:04.000\r\n"
            b"Line one\n"
            b"Line two\r\n"
            b"Line three.\n\n",
            binary=True,
        )
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(
                result,
                "[00:00:01] Line one Line two Line three.",
            )
        finally:
            os.unlink(path)


class ParseVttEdgeCasesTest(unittest.TestCase):
    def test_nonexistent_file_returns_none(self):
        self.assertIsNone(_parse_vtt_to_text("/nonexistent/path.vtt"))

    def test_none_path_returns_none(self):
        self.assertIsNone(_parse_vtt_to_text(None))

    def test_empty_file_returns_none(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vtt", delete=False) as f:
            f.write("")
            path = f.name
        try:
            self.assertIsNone(_parse_vtt_to_text(path))
        finally:
            os.unlink(path)

    def test_webvtt_only_returns_none(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vtt", delete=False) as f:
            f.write("WEBVTT\n\n")
            path = f.name
        try:
            self.assertIsNone(_parse_vtt_to_text(path))
        finally:
            os.unlink(path)

    def test_html_tags_stripped(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vtt", delete=False) as f:
            f.write(
                "WEBVTT\n\n"
                "00:00:01.000 --> 00:00:04.000\n"
                "<b>Bold text</b> and <i>italic</i>.\n\n"
            )
            path = f.name
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(result, "[00:00:01] Bold text and italic.")
        finally:
            os.unlink(path)

    def test_trailing_blank_lines(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".vtt", delete=False) as f:
            f.write(
                "WEBVTT\n\n"
                "00:00:01.000 --> 00:00:04.000\n"
                "Cue text.\n\n\n\n"
            )
            path = f.name
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(result, "[00:00:01] Cue text.")
        finally:
            os.unlink(path)

    def test_crlf_line_endings(self):
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".vtt", delete=False) as f:
            f.write(
                b"WEBVTT\r\n\r\n"
                b"00:00:01.000 --> 00:00:04.000\r\n"
                b"First line\r\n"
                b"second line.\r\n\r\n"
            )
            path = f.name
        try:
            result = _parse_vtt_to_text(path)
            self.assertEqual(
                result,
                "[00:00:01] First line second line.",
            )
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
