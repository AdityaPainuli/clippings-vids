import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from captions import cli

BACKEND_DIR = Path(__file__).resolve().parents[1]


class LoadTranscriptTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def _write(self, name, value):
        path = self.dir / name
        path.write_text(value if isinstance(value, str) else json.dumps(value),
                        encoding="utf-8")
        return str(path)

    def test_valid_transcript_is_returned_unchanged(self):
        data = {"language": "hi", "backend": "test",
                "words": [{"start": 0.0, "end": 0.4, "text": "hello"}]}
        self.assertEqual(cli.load_transcript(self._write("ok.json", data)), data)

    def test_missing_file_names_the_path(self):
        path = str(self.dir / "missing.json")
        with self.assertRaises(cli.TranscriptError) as ctx:
            cli.load_transcript(path)
        self.assertIn("could not read transcript", str(ctx.exception))
        self.assertIn(path, str(ctx.exception))

    def test_invalid_json_reports_line_and_column(self):
        path = self._write("broken.json", '{"words": [}')
        with self.assertRaises(cli.TranscriptError) as ctx:
            cli.load_transcript(path)
        message = str(ctx.exception)
        self.assertIn("not valid JSON", message)
        self.assertIn("line 1", message)
        self.assertIn("column", message)

    def test_top_level_must_be_an_object(self):
        with self.assertRaisesRegex(cli.TranscriptError, "JSON object"):
            cli.load_transcript(self._write("list.json", []))

    def test_words_must_be_a_list(self):
        with self.assertRaisesRegex(cli.TranscriptError, "'words' list"):
            cli.load_transcript(self._write("words.json", {"words": {}}))

    def test_word_shape_errors_name_the_index(self):
        cases = [
            ({"words": ["not a word"]}, "word 0 is not an object"),
            ({"words": [{"start": 0, "end": 1}]}, "word 0 is missing a string 'text'"),
            ({"words": [{"start": "0", "end": 1, "text": "x"}]},
             "word 0 has a non-numeric or non-finite 'start'"),
            ({"words": [{"start": 0, "end": float("nan"), "text": "x"}]},
             "word 0 has a non-numeric or non-finite 'end'"),
            ({"words": [{"start": 2, "end": 1, "text": "x"}]},
             "word 0 starts after it ends"),
        ]
        for payload, expected in cases:
            with self.subTest(expected=expected):
                path = self._write("bad.json", payload)
                with self.assertRaises(cli.TranscriptError) as ctx:
                    cli.load_transcript(path)
                self.assertIn(expected, str(ctx.exception))


class MainTranscriptTest(unittest.TestCase):
    def test_bad_transcript_exits_2_without_a_traceback(self):
        path = os.path.join(tempfile.gettempdir(), "bolcap-missing-transcript.json")
        if os.path.exists(path):
            os.remove(path)
        done = subprocess.run(
            [sys.executable, "-m", "captions.cli", "video.mp4",
             "--transcript", path],
            cwd=BACKEND_DIR, capture_output=True, text=True, timeout=30)
        self.assertEqual(done.returncode, 2)
        self.assertIn("error: could not read transcript", done.stderr)
        self.assertNotIn("Traceback", done.stderr)


if __name__ == "__main__":
    unittest.main()
