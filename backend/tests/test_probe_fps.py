import json
import types
import unittest
from unittest import mock

from captions import render


class ProbeFrameRateTest(unittest.TestCase):
    def _probe(self, stream):
        data = {
            "streams": [{"width": 1080, "height": 1920, **stream}],
            "format": {"duration": "2.0"},
        }
        run = mock.Mock(return_value=types.SimpleNamespace(
            returncode=0, stdout=json.dumps(data), stderr=""))
        with mock.patch.object(render.subprocess, "run", run):
            info = render.probe_video("video.mp4")
        return info, run

    def test_valid_fractional_rate(self):
        info, _ = self._probe({"r_frame_rate": "30000/1001"})
        self.assertEqual(info["fps"], 29.97)
        self.assertEqual(info["width"], 1080)
        self.assertEqual(info["duration"], 2.0)

    def test_zero_over_zero_falls_back_to_average(self):
        info, _ = self._probe({"r_frame_rate": "0/0",
                               "avg_frame_rate": "25/1"})
        self.assertEqual(info["fps"], 25.0)

    def test_missing_instant_rate_uses_average(self):
        info, _ = self._probe({"avg_frame_rate": "24/1"})
        self.assertEqual(info["fps"], 24.0)

    def test_unusable_rates_use_the_default(self):
        cases = [
            {"r_frame_rate": "N/A"},
            {"r_frame_rate": "0/0"},
            {"r_frame_rate": "25/0"},
            {"r_frame_rate": "abc/1"},
            {"r_frame_rate": "1/2/3"},
            {"r_frame_rate": "nan/1"},
            {"r_frame_rate": "1e308/1e-308"},
            {},
        ]
        for stream in cases:
            with self.subTest(stream=stream):
                info, _ = self._probe(stream)
                self.assertEqual(info["fps"], 30.0)

    def test_probe_requests_both_rate_fields(self):
        _, run = self._probe({"r_frame_rate": "25/1"})
        command = run.call_args.args[0]
        self.assertIn("stream=width,height,r_frame_rate,avg_frame_rate",
                      command)


if __name__ == "__main__":
    unittest.main()
