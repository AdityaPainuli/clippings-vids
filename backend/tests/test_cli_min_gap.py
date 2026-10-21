
import argparse
import sys
import unittest
from unittest import mock

from captions import cli


class TestMinGapValidation(unittest.TestCase):
    def test_accepts_valid_values(self):
        for value in ("0.4", "1", "0.01"):
            with self.subTest(value=value):
                self.assertGreater(cli.parse_min_gap(value), 0)

    def test_rejects_zero(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.parse_min_gap("0")

    def test_rejects_negative_values(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.parse_min_gap("-1")

    def test_rejects_non_finite_values(self):
        for value in ("nan", "inf", "-inf"):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    cli.parse_min_gap(value)

    def test_rejects_non_numeric_values(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.parse_min_gap("abc")

    def test_argparse_rejects_negative_min_gap_before_processing(self):
        with mock.patch.object(
            sys,
            "argv",
            ["cli", "x.mp4", "--min-gap", "-1"],
        ):
            with self.assertRaises(SystemExit) as context:
                cli.main()

        self.assertEqual(context.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
