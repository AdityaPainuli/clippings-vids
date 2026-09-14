import os
import tempfile
import pytest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))
from clipper import _parse_vtt_to_text


def _write_vtt(content):
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".vtt", delete=False, encoding="utf-8")
    f.write(content)
    f.close()
    return f.name


class TestVttParser:
    def test_single_line_cue(self):
        path = _write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Hello world\n\n"
        )
        result = _parse_vtt_to_text(path)
        os.unlink(path)
        assert result == "[00:00:01] Hello world"

    def test_multi_line_cue(self):
        path = _write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "This is the first line\n"
            "and this is the second line.\n\n"
        )
        result = _parse_vtt_to_text(path)
        os.unlink(path)
        assert result == "[00:00:01] This is the first line and this is the second line."

    def test_mixed_single_and_multi_line(self):
        path = _write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:03.000\n"
            "Single line\n\n"
            "00:00:04.000 --> 00:00:07.000\n"
            "First line\n"
            "Second line\n"
            "Third line\n\n"
            "00:00:08.000 --> 00:00:10.000\n"
            "Another single\n\n"
        )
        result = _parse_vtt_to_text(path)
        os.unlink(path)
        lines = result.split("\n")
        assert len(lines) == 3
        assert lines[0] == "[00:00:01] Single line"
        assert lines[1] == "[00:00:04] First line Second line Third line"
        assert lines[2] == "[00:00:08] Another single"

    def test_html_tags_stripped(self):
        path = _write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "<b>Bold text</b>\n"
            "<i>Italic text</i>\n\n"
        )
        result = _parse_vtt_to_text(path)
        os.unlink(path)
        assert result == "[00:00:01] Bold text Italic text"

    def test_returns_none_for_missing_file(self):
        assert _parse_vtt_to_text("/nonexistent/file.vtt") is None
        assert _parse_vtt_to_text(None) is None

    def test_cue_at_end_without_trailing_blank(self):
        path = _write_vtt(
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:04.000\n"
            "Line one\n"
            "Line two"
        )
        result = _parse_vtt_to_text(path)
        os.unlink(path)
        assert result == "[00:00:01] Line one Line two"
