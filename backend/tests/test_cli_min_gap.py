import argparse
import os
import sys
import unittest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from captions.cli import parse_min_gap


class TestParseMinGap(unittest.TestCase):
    def test_valid_positive_finite_values(self):
        self.assertEqual(parse_min_gap("0.4"), 0.4)
        self.assertEqual(parse_min_gap("1"), 1.0)
        self.assertEqual(parse_min_gap("2.5"), 2.5)

    def test_reject_negative_values(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("-1")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("-0.4")

    def test_reject_zero(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("0")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("0.0")

    def test_reject_non_finite_values(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("nan")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("inf")
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("-inf")

    def test_reject_invalid_strings(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            parse_min_gap("not_a_number")


if __name__ == "__main__":
    unittest.main()
