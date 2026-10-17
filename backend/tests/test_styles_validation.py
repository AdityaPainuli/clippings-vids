import os
import sys
import unittest
from pydantic import ValidationError

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from captions.styles import CaptionStyle, Animation, STYLE_PRESETS


class TestStylesValidation(unittest.TestCase):
    def test_forbid_unknown_fields_in_caption_style(self):
        with self.assertRaises(ValidationError) as ctx:
            CaptionStyle(fnot_size=90)
        self.assertIn("extra_forbidden", str(ctx.exception))

    def test_forbid_unknown_fields_in_animation(self):
        with self.assertRaises(ValidationError) as ctx:
            Animation(unknown_prop=True)
        self.assertIn("extra_forbidden", str(ctx.exception))

    def test_invalid_hex_color(self):
        with self.assertRaises(ValidationError):
            CaptionStyle(text_color="not_a_hex")

    def test_out_of_range_font_size(self):
        with self.assertRaises(ValidationError):
            CaptionStyle(font_size=500)
        with self.assertRaises(ValidationError):
            CaptionStyle(font_size=10)

    def test_valid_presets_exist(self):
        self.assertIn("default", STYLE_PRESETS)
        self.assertIn("bold_impact", STYLE_PRESETS)
        self.assertIn("subtle", STYLE_PRESETS)
        self.assertIn("karaoke", STYLE_PRESETS)

    def test_cli_missing_style_file(self):
        from captions import cli
        import io
        from unittest.mock import patch

        with patch("sys.argv", ["cli.py", "dummy.mp4", "--style", "non_existent_file.json"]), \
             patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            code = cli.main()
            self.assertEqual(code, 2)
            self.assertIn("style file not found", mock_out.getvalue())

    def test_cli_unknown_preset(self):
        from captions import cli
        import io
        from unittest.mock import patch

        with patch("sys.argv", ["cli.py", "dummy.mp4", "--preset", "nonexistent_preset"]), \
             patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            code = cli.main()
            self.assertEqual(code, 2)
            self.assertIn("unknown preset 'nonexistent_preset'", mock_out.getvalue())


if __name__ == "__main__":
    unittest.main()
