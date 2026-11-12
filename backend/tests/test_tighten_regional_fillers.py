import unittest

from captions.lang_detect import tag_words
from captions.tighten import LEXICAL_FILLERS_TA, LEXICAL_FILLERS_TE
from captions.tighten import TightenConfig, detect_fillers


def _tag_phrase(phrase, segment_lang):
    # Half-second pauses on both sides let an eligible lexical filler score.
    words = [
        {"text": token, "start": i * 0.75, "end": i * 0.75 + 0.25}
        for i, token in enumerate(phrase.split())
    ]
    return tag_words(words, segment_lang=segment_lang)


class RegionalFillerTest(unittest.TestCase):
    def test_regional_words_reach_the_filler_detector(self):
        for phrase, segment_lang, filler in (
            ("nee poda anga", "ta", "poda"),
            ("idi thelusaa meeru", "te", "thelusaa"),
        ):
            with self.subTest(filler=filler):
                words = _tag_phrase(phrase, segment_lang)
                self.assertEqual(words[1]["lang"], segment_lang)
                cuts = detect_fillers(words, TightenConfig(lexical_fillers=True))
                self.assertEqual([cut.text for cut in cuts], [filler])

    def test_real_words_are_preserved(self):
        for phrase, segment_lang, word in (
            ("idha correct panni", "ta", "correct"),
            ("ye sare log aaye", "hi", "sare"),
        ):
            with self.subTest(word=word):
                words = _tag_phrase(phrase, segment_lang)
                self.assertEqual(words[1]["lang"], "en")
                self.assertEqual(detect_fillers(words, TightenConfig(lexical_fillers=True)), [])

    def test_dead_entries_are_removed(self):
        self.assertNotIn("correct", LEXICAL_FILLERS_TA)
        self.assertTrue({"sare", "therlusaa"}.isdisjoint(LEXICAL_FILLERS_TE))
