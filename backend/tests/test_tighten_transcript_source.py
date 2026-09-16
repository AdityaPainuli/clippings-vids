import unittest

from captions.tighten import TightenConfig, detect_fillers


class TightenTranscriptSourceTests(unittest.TestCase):
    def setUp(self):
        self.cfg = TightenConfig(
            fillers=True,
            lexical_fillers=True,
            filler_suggest_threshold=0.45,
            min_pause_signal=0.25,
        )

    def _words(self, middle_text, middle_hinglish):
        return [
            {"start": 0.0, "end": 0.40, "text": "pehle", "hinglish": "before"},
            {"start": 1.0, "end": 1.40, "text": middle_text, "hinglish": middle_hinglish},
            {"start": 2.0, "end": 2.40, "text": "baat", "hinglish": "thing"},
        ]

    def test_llm_romanization_cannot_turn_normal_word_into_filler(self):
        words = self._words("मैं", "like")

        cuts = detect_fillers(words, self.cfg)

        self.assertEqual([], cuts)

    def test_original_transcript_still_controls_real_lexical_filler(self):
        words = self._words("मतलब", "meaning")

        cuts = detect_fillers(words, self.cfg)

        self.assertEqual(1, len(cuts))
        self.assertEqual("मतलब", cuts[0].text)
        self.assertEqual("filler", cuts[0].reason)

    def test_latin_transcript_remains_supported(self):
        words = self._words("like", "like")

        cuts = detect_fillers(words, self.cfg)

        self.assertEqual(1, len(cuts))
        self.assertEqual("like", cuts[0].text)

    def test_transcript_source_is_used_when_hinglish_is_paraphrased(self):
        words = self._words("तो", "so")

        cuts = detect_fillers(words, self.cfg)

        self.assertEqual(1, len(cuts))
        self.assertEqual("तो", cuts[0].text)


if __name__ == "__main__":
    unittest.main()
