import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock heavy third-party dependencies before importing clipper or captions
_MOCKED_MODULES = [
    "yt_dlp", "google", "google.generativeai", "moviepy",
    "moviepy.editor", "dotenv", "whisper", "cv2",
]
for mod in _MOCKED_MODULES:
    sys.modules.setdefault(mod, mock.MagicMock())

import clipper  # noqa: E402
from captions.styles import CaptionStyle, Animation, STYLE_PRESETS  # noqa: E402
from captions.engine import build_ass  # noqa: E402


class KaraokeTimingAndColorTest(unittest.TestCase):
    def setUp(self):
        self.karaoke_style = CaptionStyle(
            animation=Animation(type="karaoke"),
            text_color="#FFFFFF",
            highlight_color="#FF5500",
        )
        self.words_with_pause = [
            {"start": 0.0, "end": 1.0, "text": "Hello"},
            {"start": 1.8, "end": 2.5, "text": "World"},
        ]
        self.continuous_words = [
            {"start": 0.0, "end": 1.0, "text": "Hello"},
            {"start": 1.0, "end": 2.0, "text": "World"},
        ]
        self.short_words = [
            {"start": 0.0, "end": 0.05, "text": "A"},
            {"start": 0.05, "end": 0.50, "text": "test"},
        ]

    def test_karaoke_colors_and_no_hardcoded_red(self):
        """In ASS, \\kf wipes SecondaryColour (base text) to PrimaryColour (highlight).
        Ensure highlight_color is used and hardcoded red (&H000000FF) is not present."""
        ass = build_ass(self.words_with_pause, self.karaoke_style)
        self.assertNotIn("&H000000FF", ass)
        expected_style_def = (
            f"Style: Caption,{self.karaoke_style.font},{self.karaoke_style.font_size},"
            f"{self.karaoke_style.ass_highlight_color},{self.karaoke_style.ass_text_color}"
        )
        self.assertIn(expected_style_def, ass)

    def test_inter_word_pause_handling(self):
        """Word 1 (0.0s-1.0s, 100cs), pause (1.0s-1.8s, 80cs), Word 2 (1.8s-2.5s, 70cs).
        Ensure {\\k<gap>} is emitted on the inter-word space."""
        ass = build_ass(self.words_with_pause, self.karaoke_style)
        expected_event = r"{\kf100}HELLO{\k80} {\kf70}WORLD"
        self.assertIn(expected_event, ass)

    def test_continuous_speech_without_pause(self):
        """Words without pause (gap <= 0) should not emit extra pause tags."""
        ass_continuous = build_ass(self.continuous_words, self.karaoke_style)
        expected_continuous = r"{\kf100}HELLO {\kf100}WORLD"
        self.assertIn(expected_continuous, ass_continuous)

    def test_builtin_karaoke_preset(self):
        """Built-in preset 'karaoke' works out-of-the-box with configured colors and pause tags."""
        preset_style = STYLE_PRESETS["karaoke"]
        ass_preset = build_ass(self.words_with_pause, preset_style)
        self.assertIn(preset_style.ass_highlight_color, ass_preset)
        expected_event = r"{\kf100}HELLO{\k80} {\kf70}WORLD"
        self.assertIn(expected_event, ass_preset)

    def test_non_karaoke_styles_unaffected(self):
        """Non-karaoke presets (like bold_impact) do not contain hardcoded red and generate Dialogue events."""
        pop_style = STYLE_PRESETS["bold_impact"]
        ass_pop = build_ass(self.words_with_pause, pop_style)
        self.assertNotIn("&H000000FF", ass_pop)
        self.assertIn("Dialogue:", ass_pop)

    def test_legacy_clipper_generator_timing_and_colors(self):
        """Legacy clipper generator (_build_ass_header and _words_to_ass_events)
        respects colors, handles pauses, and removes hardcoded red."""
        clipper_header = clipper._build_ass_header("karaoke")
        self.assertNotIn("&H000000FF", clipper_header)

        clipper_preset = clipper.CAPTION_PRESETS["karaoke"]
        expected_clipper_caption = (
            f"Style: Caption,{clipper_preset['font']},{clipper_preset['fontsize']},"
            f"{clipper_preset['highlight']},{clipper_preset['color']}"
        )
        self.assertIn(expected_clipper_caption, clipper_header)

        expected_event = r"{\kf100}HELLO{\k80} {\kf70}WORLD"
        clipper_events = clipper._words_to_ass_events(self.words_with_pause, "karaoke")
        self.assertIn(expected_event, clipper_events)

        expected_continuous = r"{\kf100}HELLO {\kf100}WORLD"
        clipper_cont_events = clipper._words_to_ass_events(self.continuous_words, "karaoke")
        self.assertIn(expected_continuous, clipper_cont_events)

        default_header = clipper._build_ass_header("default")
        self.assertNotIn("&H000000FF", default_header)

    def test_short_word_sub_10cs_duration_no_drift(self):
        """Short-word duration (<10cs) is accurately timed without artificial drift in both generators."""
        expected_short = r"{\kf5}A {\kf45}TEST"
        ass_short = build_ass(self.short_words, self.karaoke_style)
        self.assertIn(expected_short, ass_short)

        clipper_short = clipper._words_to_ass_events(self.short_words, "karaoke")
        self.assertIn(expected_short, clipper_short)


if __name__ == "__main__":
    unittest.main()
