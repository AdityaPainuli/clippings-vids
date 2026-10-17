import os
import sys
import unittest
from pydantic import ValidationError

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from captions.styles import CaptionStyle


class TestFontValidation(unittest.TestCase):
    def test_valid_font_names(self):
        style = CaptionStyle(font="Arial Black")
        self.assertEqual(style.font, "Arial Black")

        style2 = CaptionStyle(font="  Inter  ")
        self.assertEqual(style2.font, "Inter")

    def test_reject_font_with_comma(self):
        with self.assertRaises(ValidationError) as ctx:
            CaptionStyle(font="Arial, Bold")
        self.assertIn("commas", str(ctx.exception))

    def test_reject_font_with_newline(self):
        with self.assertRaises(ValidationError) as ctx:
            CaptionStyle(font="Arial\nStyle: Attack")
        self.assertIn("newlines", str(ctx.exception))

    def test_reject_font_with_carriage_return(self):
        with self.assertRaises(ValidationError) as ctx:
            CaptionStyle(font="Arial\rStyle: Attack")
        self.assertIn("newlines", str(ctx.exception))

    def test_reject_empty_font(self):
        with self.assertRaises(ValidationError) as ctx:
            CaptionStyle(font="   ")
        self.assertIn("empty", str(ctx.exception))

    def test_reject_overlong_font_name(self):
        with self.assertRaises(ValidationError) as ctx:
            CaptionStyle(font="A" * 65)
        self.assertTrue(
            "64 characters" in str(ctx.exception) or "string_too_long" in str(ctx.exception)
        )


if __name__ == "__main__":
    unittest.main()
