import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pydantic import ValidationError
from captions.styles import Animation, CaptionStyle


class StyleValidationTest(unittest.TestCase):
    def test_unknown_field_in_caption_style_rejected(self):
        with self.assertRaises(ValidationError) as cm:
            CaptionStyle(fnot_size=90)
        self.assertIn("Extra inputs are not permitted", str(cm.exception))

    def test_unknown_field_in_animation_rejected(self):
        with self.assertRaises(ValidationError) as cm:
            Animation(typo_field="value")
        self.assertIn("Extra inputs are not permitted", str(cm.exception))

    def test_invalid_hex_color_rejected(self):
        with self.assertRaises(ValidationError) as cm:
            CaptionStyle(text_color="not-a-color")
        self.assertIn("Invalid hex color", str(cm.exception))

    def test_out_of_range_font_size_rejected(self):
        with self.assertRaises(ValidationError) as cm:
            CaptionStyle(font_size=500)
        self.assertIn("less than or equal to 200", str(cm.exception))

    def test_valid_style_accepted(self):
        style = CaptionStyle(font_size=80, text_color="#FFFFFF")
        self.assertEqual(style.font_size, 80)
        self.assertEqual(style.text_color, "#FFFFFF")


if __name__ == "__main__":
    unittest.main()
