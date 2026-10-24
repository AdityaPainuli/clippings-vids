import json
import types
import unittest
from unittest import mock

from captions import render

_MISSING = object()


class ProbeVideoFieldsTest(unittest.TestCase):
    def _probe(self, stream, fmt=None):
        fields = {"width": 1080, "height": 1920, "r_frame_rate": "25/1"}
        for key, value in stream.items():
            if value is _MISSING:
                fields.pop(key, None)
            else:
                fields[key] = value
        data = {"streams": [fields]}
        if fmt is not None:
            data["format"] = fmt
        run = mock.Mock(return_value=types.SimpleNamespace(
            returncode=0, stdout=json.dumps(data), stderr=""))
        with mock.patch.object(render.subprocess, "run", run):
            return render.probe_video("video.mp4")

    def test_valid_fields_are_returned(self):
        info = self._probe({}, {"duration": "2.5"})
        self.assertEqual(info["width"], 1080)
        self.assertEqual(info["height"], 1920)
        self.assertEqual(info["duration"], 2.5)

    def test_missing_or_invalid_width_is_named(self):
        for value in (_MISSING, "N/A", 0, -1, True):
            with self.subTest(value=value):
                with self.assertRaisesRegex(RuntimeError, "width"):
                    self._probe({"width": value}, {"duration": "1.0"})

    def test_missing_or_invalid_height_is_named(self):
        for value in (_MISSING, "N/A", 0, -1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(RuntimeError, "height"):
                    self._probe({"height": value}, {"duration": "1.0"})

    def test_missing_or_invalid_duration_is_named(self):
        for fmt in (None, {}, {"duration": "N/A"}, {"duration": 0},
                    {"duration": -1}, {"duration": "nan"}, {"duration": "inf"}):
            with self.subTest(fmt=fmt):
                with self.assertRaisesRegex(RuntimeError, "duration"):
                    self._probe({}, fmt)


if __name__ == "__main__":
    unittest.main()
