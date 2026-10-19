import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from captions.engine import build_ass
from captions.styles import CaptionStyle, Animation

class TestKaraokeColors(unittest.TestCase):
    def test_karaoke_uses_configured_highlight_color(self):
        style = CaptionStyle(
            text_color = "#FFFFFF", #ASS : &H00FFFFFF
            highlight_color = "#FF5500", #ASS : &H000055FF
            animation = Animation(type= "karaoke"))

        ass_output = build_ass([], style)

        caption_line = ""

        for line in ass_output.splitlines():
            if line.startswith("Style: Caption"):
                caption_line = line
                break

        assert style.ass_highlight_color in caption_line, "Highlight color missing!"
        assert style.ass_text_color in caption_line, "Text color missing!"
        assert "&H000000FF" not in caption_line, "Found hardcoded red color!"
        
    def test_non_karaoke_preserves_primary_text_color(self):
        style = CaptionStyle(animation = Animation(type= "none"))

        ass_output = build_ass([], style)

        caption_line = ""

        for line in ass_output.splitlines():
            if line.startswith("Style: Caption"):
                caption_line = line
                break
        
        assert style.ass_text_color in caption_line, "Non-Karaoke caption is not using ass_text_color!"
        
class TestKaraokeTiming(unittest.TestCase):
    def test_karaoke_inter_word_pause_includes_gap(self):
        words = [
            {"start": 0.0, "end": 1.0, "text": "Hello"},
            {"start": 1.8, "end": 2.5, "text": "world"},
        ]

        style = CaptionStyle(animation = Animation(type = "karaoke"))
        ass_output = build_ass(words, style)

        dialogue_line = ""

        for line in ass_output.splitlines():
            if line.startswith("Dialogue:"):
                dialogue_line = line
                break

        assert dialogue_line != "", "Dialogue line was not generated!"
        assert "{\\kf100}HELLO" in dialogue_line, "first word duration tag {\\kf100}HELLO Missing!"
        assert "{\\kf80}" in dialogue_line, "Gap duration tag {\\kf80} Missing!"
        assert "{\\kf70}WORLD" in dialogue_line, "Last word duration tag {\\kf70}WORLD Missing!"

    def test_karaoke_contiguous_words_no_gap(self):
        words = [
            {"start": 0.0, "end": 1.0, "text": "Hello"},
            {"start": 1.0, "end": 1.7, "text": "World"},
        ]

        style = CaptionStyle(animation=Animation(type="karaoke"))
        ass_output = build_ass(words, style)

        dialogue_line = ""
        for line in ass_output.splitlines():
            if line.startswith("Dialogue:"):
                dialogue_line = line
                break

        assert dialogue_line != "", "Dialogue line was not generated!"
        assert "{\\kf100}HELLO {\\kf70}WORLD" in dialogue_line, "First word duration tag {\\kf100}HELLO missing!"
        assert "{\\kf70}WORLD" in dialogue_line, "Second word duration tag {\\kf70}WORLD missing!"
        assert "{\\kf0}" not in dialogue_line, "Found unnecessary zero-length gap tag {\\kf0}!"
