import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from captions import render

BACKEND_DIR = Path(__file__).resolve().parents[1]


class EmptySrtExportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def test_empty_word_list_raises_and_creates_nothing(self):
        out = self.dir / "empty.srt"
        with self.assertRaisesRegex(ValueError, "no captions"):
            render.export_srt([], str(out))
        self.assertFalse(out.exists())

    def test_whitespace_words_do_not_overwrite_an_existing_file(self):
        out = self.dir / "existing.srt"
        out.write_text("keep me", encoding="utf-8")
        words = [{"start": 0, "end": 1, "text": "   "}]
        with self.assertRaisesRegex(ValueError, "no captions"):
            render.export_srt(words, str(out))
        self.assertEqual(out.read_text(encoding="utf-8"), "keep me")

    def test_valid_words_still_write_an_srt(self):
        out = self.dir / "ok.srt"
        words = [{"start": 0, "end": 1, "text": "hello"},
                 {"start": 1, "end": 2, "text": "world"}]
        self.assertEqual(render.export_srt(words, str(out)), str(out))
        self.assertIn("hello world", out.read_text(encoding="utf-8"))
        self.assertIn("00:00:00,000 --> 00:00:02,000", out.read_text(encoding="utf-8"))


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is not installed")
class CliEmptySrtTest(unittest.TestCase):
    def test_cli_exits_2_without_reporting_done(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            video = root / "clip.mp4"
            subprocess.run(
                ["ffmpeg", "-f", "lavfi", "-i",
                 "color=c=black:s=320x240:d=1", "-pix_fmt", "yuv420p",
                 "-y", str(video)],
                check=True, capture_output=True)
            transcript = root / "empty.json"
            transcript.write_text(json.dumps({"words": []}), encoding="utf-8")
            out = root / "out.srt"

            done = subprocess.run(
                [sys.executable, "-m", "captions.cli", str(video),
                 "--transcript", str(transcript), "--export", "srt",
                 "-o", str(out)],
                cwd=BACKEND_DIR, capture_output=True, text=True, timeout=60)

            self.assertEqual(done.returncode, 2)
            self.assertIn("error: no captions to export", done.stderr)
            self.assertNotIn("done:", done.stdout)
            self.assertFalse(out.exists())


if __name__ == "__main__":
    unittest.main()
