import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest import mock

import captions as captions_package

from captions import cli, tighten, timeline


VIDEO_INFO = {
    "width": 1920, "height": 1080, "duration": 12.0, "fps": 30.0,
}


class TimelineExportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.video = os.path.join(self.tmp.name, "clip.mp4")
        self.transcript = os.path.join(self.tmp.name, "transcript.json")
        with open(self.transcript, "w", encoding="utf-8") as f:
            json.dump({"words": [{"start": 0.0, "end": 1.0, "text": "hello",
                                  "hinglish": "hello"}]}, f)

    def _run(self, export, cuts):
        argv = ["cli", self.video, "--transcript", self.transcript,
                "--tighten", "--export", export]
        srt = mock.Mock(return_value="captions.srt")
        with mock.patch.object(sys, "argv", argv), \
                mock.patch.dict(sys.modules, {"captions": captions_package}), \
                mock.patch("captions.render.probe_video",
                           return_value=dict(VIDEO_INFO)), \
                mock.patch("captions.render.probe_audio",
                           return_value={"has_audio": True,
                                         "channels": 2,
                                         "sample_rate": 48000}), \
                mock.patch("captions.render.export_srt", srt), \
                mock.patch("captions.render.burn_video") as burn, \
                mock.patch("captions.tighten.detect", return_value=cuts):
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main()
        return code, out.getvalue(), burn, srt

    def test_edl_with_no_cuts_exports_the_whole_clip(self):
        with mock.patch("captions.timeline.export_edl",
                        return_value="clip.edl") as export:
            code, output, burn, srt = self._run("edl", [])
        self.assertIsNone(code)
        self.assertEqual(export.call_args.args[0], [(0.0, 12.0)])
        burn.assert_not_called()
        srt.assert_called_once()
        self.assertIn("done: clip.edl", output)
        self.assertNotIn("Burning captions", output)

    def test_fcpxml_with_no_cuts_exports_the_whole_clip(self):
        with mock.patch("captions.timeline.export_fcpxml",
                        return_value="clip.fcpxml") as export:
            code, output, burn, srt = self._run("fcpxml", [])
        self.assertIsNone(code)
        self.assertEqual(export.call_args.args[0], [(0.0, 12.0)])
        burn.assert_not_called()
        srt.assert_called_once()
        self.assertIn("done: clip.fcpxml", output)
        self.assertNotIn("Burning captions", output)

    def test_edl_still_uses_applied_cuts(self):
        cut = tighten.Cut(4.0, 6.0, "silence", 0.95, auto=True)
        with mock.patch("captions.timeline.export_edl",
                        return_value="clip.edl") as export:
            code, output, burn, srt = self._run("edl", [cut])
        self.assertIsNone(code)
        kept = export.call_args.args[0]
        self.assertEqual(len(kept), 2)
        self.assertEqual(kept[0][0], 0.0)
        self.assertEqual(kept[-1][1], 12.0)
        self.assertFalse(any(start < 5.0 < end for start, end in kept
                             if start <= 4.0 and end >= 6.0))
        burn.assert_not_called()
        self.assertIn("done: clip.edl", output)


if __name__ == "__main__":
    unittest.main()
