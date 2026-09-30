import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.cli import parse_min_gap


class ParseMinGapTest(unittest.TestCase):
    def test_valid_positive_values(self):
        self.assertEqual(parse_min_gap("0.4"), 0.4)
        self.assertEqual(parse_min_gap("1"), 1.0)
        self.assertEqual(parse_min_gap("2.5"), 2.5)

    def test_zero_is_valid(self):
        self.assertEqual(parse_min_gap("0"), 0.0)

    def test_negative_rejected(self):
        with self.assertRaises(ValueError) as cm:
            parse_min_gap("-1")
        self.assertIn("cannot be negative", str(cm.exception))

    def test_inf_rejected(self):
        with self.assertRaises(ValueError) as cm:
            parse_min_gap("inf")
        self.assertIn("finite", str(cm.exception))

    def test_nan_rejected(self):
        with self.assertRaises(ValueError) as cm:
            parse_min_gap("nan")
        self.assertIn("finite", str(cm.exception))

    def test_non_numeric_rejected(self):
        with self.assertRaises(ValueError) as cm:
            parse_min_gap("abc")
        self.assertIn("could not read a number", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
