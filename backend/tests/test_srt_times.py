import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.render import _ass_time_srt  # noqa: E402


class SrtTimeTest(unittest.TestCase):
    def test_float_error_does_not_drop_millisecond(self):
        self.assertEqual(_ass_time_srt(2.3), "00:00:02,300")

    def test_zero(self):
        self.assertEqual(_ass_time_srt(0.0), "00:00:00,000")

    def test_rounding_carries_into_next_minute(self):
        self.assertEqual(_ass_time_srt(59.9996), "00:01:00,000")

    def test_hours(self):
        self.assertEqual(_ass_time_srt(3600.5), "01:00:00,500")


if __name__ == "__main__":
    unittest.main()
