"""
Tests for clipper._parse_vtt_to_text.

clipper.py imports several heavy, optional dependencies (yt_dlp,
google.generativeai, moviepy) at module level that aren't needed to exercise
this pure function and aren't installed in every dev/CI environment. They're
stubbed out in sys.modules before the import below so this file runs under
plain `python -m unittest` with no extra installs.
"""

import os
import sys
import types
import unittest

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def _stub(name):
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)


_stub("yt_dlp")
sys.modules["yt_dlp"].YoutubeDL = object

_stub("google")
_stub("google.generativeai")
sys.modules["google.generativeai"].configure = lambda **kw: None
sys.modules["google.generativeai"].GenerativeModel = object
sys.modules["google"].generativeai = sys.modules["google.generativeai"]

_stub("moviepy")
sys.modules["moviepy"].VideoFileClip = object

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clipper import _parse_vtt_to_text  # noqa: E402


def _write(tmp_path, name, content):
    path = os.path.join(tmp_path, name)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(content)
    return path


class ParseVttTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    # ── Missing / empty input ────────────────────────────────────────────

    def test_none_path_returns_none(self):
        self.assertIsNone(_parse_vtt_to_text(None))

    def test_nonexistent_path_returns_none(self):
        self.assertIsNone(_parse_vtt_to_text("/no/such/file.vtt"))

    def test_webvtt_header_only_returns_none(self):
        path = _write(self.tmp_path, "header_only.vtt", "WEBVTT\n")
        self.assertIsNone(_parse_vtt_to_text(path))

    # ── Baseline single-line behavior (must not regress) ─────────────────

    def test_single_line_cue(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Hello there.\n"
        )
        path = _write(self.tmp_path, "single.vtt", content)
        self.assertEqual(_parse_vtt_to_text(path), "[00:00:01] Hello there.")

    def test_html_tags_are_stripped(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "<b>Bold</b> text\n"
        )
        path = _write(self.tmp_path, "html.vtt", content)
        self.assertEqual(_parse_vtt_to_text(path), "[00:00:01] Bold text")

    # ── Multi-line cues (#30 / #31) ───────────────────────────────────────

    def test_two_line_cue_is_joined(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "This is the first line of the cue\n"
            "and this is the second.\n"
        )
        path = _write(self.tmp_path, "two_line.vtt", content)
        self.assertEqual(
            _parse_vtt_to_text(path),
            "[00:00:01] This is the first line of the cue and this is the second.",
        )

    def test_three_plus_line_cue_is_joined(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:06.000\n"
            "one\ntwo\nthree\nfour\n"
        )
        path = _write(self.tmp_path, "multi.vtt", content)
        self.assertEqual(_parse_vtt_to_text(path), "[00:00:01] one two three four")

    def test_mixed_single_and_multi_line_cues(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Single line.\n\n"
            "00:00:05.000 --> 00:00:09.000\n"
            "Two\nlines here.\n"
        )
        path = _write(self.tmp_path, "mixed.vtt", content)
        self.assertEqual(
            _parse_vtt_to_text(path),
            "[00:00:01] Single line.\n[00:00:05] Two lines here.",
        )

    # ── Cue identifiers — structural detection (#45) ─────────────────────

    def test_numbered_identifier_before_timestamp_is_skipped(self):
        path = os.path.join(FIXTURES, "numbered_identifiers.vtt")
        self.assertEqual(
            _parse_vtt_to_text(path),
            "[00:00:01] This is the first cue.\n[00:00:05] This is the second cue.",
        )

    def test_digit_leading_caption_text_is_preserved(self):
        # The exact repro from issue #45.
        path = os.path.join(FIXTURES, "digit_leading_caption.vtt")
        self.assertEqual(
            _parse_vtt_to_text(path),
            "[00:00:01] 2024 was a big year for us.\n"
            "[00:00:05] 90 percent of creators agree.\n"
            "[00:00:09] Normal line survives.",
        )

    def test_numbered_identifiers_and_digit_leading_captions_together(self):
        path = os.path.join(FIXTURES, "mixed_identifiers_and_digits.vtt")
        self.assertEqual(
            _parse_vtt_to_text(path),
            "[00:00:01] 2024 was a big year for us.\n"
            "[00:00:05] 90 percent of creators agree.\n"
            "[00:00:09] Normal line survives.",
        )

    def test_non_numeric_identifier_is_also_skipped(self):
        # Identifiers aren't always bare integers ("cue-1", hashes, etc.) —
        # position, not content, is what makes a line an identifier.
        content = (
            "WEBVTT\n\n"
            "cue-42\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Real caption text.\n"
        )
        path = _write(self.tmp_path, "named_id.vtt", content)
        self.assertEqual(_parse_vtt_to_text(path), "[00:00:01] Real caption text.")

    # ── Adversarial edge cases ────────────────────────────────────────────

    def test_consecutive_timestamps_with_no_text_between(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:02.000\n"
            "00:00:02.000 --> 00:00:04.000\n"
            "Only this survives.\n"
        )
        path = _write(self.tmp_path, "consecutive_ts.vtt", content)
        self.assertEqual(_parse_vtt_to_text(path), "[00:00:02] Only this survives.")

    def test_timestamp_with_no_text_produces_nothing(self):
        content = "WEBVTT\n\n00:00:01.000 --> 00:00:04.000\n"
        path = _write(self.tmp_path, "empty_cue.vtt", content)
        self.assertIsNone(_parse_vtt_to_text(path))

    def test_cue_with_only_html_tags_produces_nothing(self):
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "<i></i>\n"
        )
        path = _write(self.tmp_path, "tags_only.vtt", content)
        self.assertIsNone(_parse_vtt_to_text(path))

    def test_timestamp_with_hour_component(self):
        content = (
            "WEBVTT\n\n"
            "01:02:03.000 --> 01:02:06.000\n"
            "An hour in.\n"
        )
        path = _write(self.tmp_path, "hours.vtt", content)
        self.assertEqual(_parse_vtt_to_text(path), "[01:02:03] An hour in.")

    def test_multiple_blank_lines_between_cues(self):
        content = (
            "WEBVTT\n\n\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "First.\n\n\n"
            "00:00:05.000 --> 00:00:08.000\n"
            "Second.\n"
        )
        path = _write(self.tmp_path, "extra_blanks.vtt", content)
        self.assertEqual(
            _parse_vtt_to_text(path), "[00:00:01] First.\n[00:00:05] Second."
        )

    def test_crlf_line_endings(self):
        content = (
            "WEBVTT\r\n\r\n"
            "00:00:01.000 --> 00:00:04.000\r\n"
            "2024 was a big year for us.\r\n"
        )
        path = os.path.join(self.tmp_path, "crlf.vtt")
        with open(path, "wb") as f:
            f.write(content.encode("utf-8"))
        self.assertEqual(
            _parse_vtt_to_text(path), "[00:00:01] 2024 was a big year for us."
        )

    def test_no_trailing_blank_line_at_eof(self):
        # No trailing newline/blank after the last cue — the flush at EOF
        # must still emit it.
        content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Last cue, no trailing blank."
        )
        path = _write(self.tmp_path, "no_trailing_blank.vtt", content)
        self.assertEqual(
            _parse_vtt_to_text(path), "[00:00:01] Last cue, no trailing blank."
        )

    def test_ten_cue_stress(self):
        parts = ["WEBVTT\n"]
        for i in range(10):
            start = f"00:00:{i:02d}.000"
            end = f"00:00:{i + 1:02d}.000"
            parts.append(f"\n{i + 1}\n{start} --> {end}\ncue number {i}\n")
        path = _write(self.tmp_path, "stress.vtt", "".join(parts))
        result = _parse_vtt_to_text(path)
        self.assertEqual(len(result.splitlines()), 10)
        for i in range(10):
            self.assertIn(f"cue number {i}", result)


if __name__ == "__main__":
    unittest.main()
