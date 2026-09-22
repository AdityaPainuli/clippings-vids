#!/usr/bin/env python3
"""
Check karaoke caption generation: timing synchronization across inter-word
pauses and proper ASS color configuration.

    python scripts/check_karaoke.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.styles import CaptionStyle, Animation, STYLE_PRESETS  # noqa: E402
from captions.engine import build_ass  # noqa: E402


def main():
    failures = []

    # 1. Karaoke colors: SecondaryColour (base text) to PrimaryColour (highlight)
    karaoke_style = CaptionStyle(
        animation=Animation(type="karaoke"),
        text_color="#FFFFFF",
        highlight_color="#FF5500",
    )
    words = [
        {"start": 0.0, "end": 1.0, "text": "Hello"},
        {"start": 1.8, "end": 2.5, "text": "World"},
    ]
    ass = build_ass(words, karaoke_style)

    if "&H000000FF" in ass:
        failures.append("Hardcoded red (&H000000FF) must not be present in ASS header")

    # In ASS, \kf wipes from SecondaryColour to PrimaryColour.
    # Unspoken text should be text_color, wiping to highlight_color.
    expected_style_def = f"Style: Caption,{karaoke_style.font},{karaoke_style.font_size},{karaoke_style.ass_highlight_color},{karaoke_style.ass_text_color}"
    if expected_style_def not in ass:
        failures.append(f"Style: Caption does not match expected colors:\nExpected prefix: {expected_style_def}")

    # 2. Inter-word pause handling
    # Word 1 (0.0s-1.0s, 100cs), pause (1.0s-1.8s, 80cs), Word 2 (1.8s-2.5s, 70cs)
    expected_event = r"{\kf100}HELLO{\k80} {\kf70}WORLD"
    if expected_event not in ass:
        failures.append(f"Karaoke dialogue missing pause tag:\nExpected: {expected_event}\nGot ASS output:\n{ass}")

    # 3. Continuous speech without pause
    continuous_words = [
        {"start": 0.0, "end": 1.0, "text": "Hello"},
        {"start": 1.0, "end": 2.0, "text": "World"},
    ]
    ass_continuous = build_ass(continuous_words, karaoke_style)
    expected_continuous = r"{\kf100}HELLO {\kf100}WORLD"
    if expected_continuous not in ass_continuous:
        failures.append(f"Continuous karaoke dialogue mismatch:\nExpected: {expected_continuous}\nGot ASS output:\n{ass_continuous}")

    # 4. Built-in preset 'karaoke' works out-of-the-box
    preset_style = STYLE_PRESETS["karaoke"]
    ass_preset = build_ass(words, preset_style)
    if preset_style.ass_highlight_color not in ass_preset:
        failures.append("Preset 'karaoke' highlight_color not found in generated ASS")
    if expected_event not in ass_preset:
        failures.append("Preset 'karaoke' missing inter-word pause tag")

    # 5. Non-karaoke styles remain unaffected
    pop_style = STYLE_PRESETS["bold_impact"]
    ass_pop = build_ass(words, pop_style)
    if "&H000000FF" in ass_pop:
        failures.append("Hardcoded red (&H000000FF) must not be in pop preset")
    if "Dialogue:" not in ass_pop:
        failures.append("Pop preset failed to generate Dialogue events")

    if failures:
        print("FAIL")
        for f in failures:
            print(f"  {f}")
        return 1

    print("PASS — karaoke timing synchronized across pauses, highlight_color respected, "
          "no hardcoded red, non-karaoke styles unaffected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
