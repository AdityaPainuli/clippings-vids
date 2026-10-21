"""
Tests for the reduplication guard in captions/tighten.detect_repeats.

Hindi doubles a word for emphasis ("baar baar", "dheere dheere"), and Tighten
must not treat that as a stutter. The guard used to match only the Latin
spellings in REDUPLICATED, so a Devanagari transcript, or one romanized
with a different spelling, had a real word auto-cut.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.tighten import detect_repeats  # noqa: E402


def _words(pairs):
    """(text, hinglish) pairs spoken back to back, no pause between."""
    words = []
    for i, (text, hinglish) in enumerate(pairs):
        w = {"start": i * 0.32, "end": i * 0.32 + 0.3, "text": text}
        if hinglish is not None:
            w["hinglish"] = hinglish
        words.append(w)
    return words


class ReduplicationGuardTest(unittest.TestCase):
    def test_rule_path_spellings_are_not_cut(self):
        # What the no-API-key romanizer produces for these words
        for text, hinglish in [("बार", "bar"), ("धीरे", "dhire"), ("पास", "pas"),
                               ("साथ", "sath"), ("दूर", "dur"), ("छोटा", "chota")]:
            with self.subTest(word=text):
                words = _words([("वो", "vo"), (text, hinglish), (text, hinglish)])
                self.assertEqual(detect_repeats(words), [])

    def test_devanagari_only_transcript_is_not_cut(self):
        words = _words([("मैंने", None), ("बार", None), ("बार", None), ("कहा", None)])
        self.assertEqual(detect_repeats(words), [])

    def test_flap_matches_in_either_encoding(self):
        precomposed = "\u092c\u095c\u093e"           # ड़ as U+095C
        decomposed = "\u092c\u0921\u093c\u093e"      # ड + nukta
        for text in (precomposed, decomposed):
            with self.subTest(codepoints=[hex(ord(c)) for c in text]):
                words = _words([("ये", None), (text, None), (text, None)])
                self.assertEqual(detect_repeats(words), [])

    def test_real_stutter_is_still_cut(self):
        words = _words([("मैं", "main"), ("मैं", "main"), ("जा", "ja"), ("रहा", "raha")])
        cuts = detect_repeats(words)
        self.assertEqual(len(cuts), 1)
        self.assertTrue(cuts[0].auto)
        self.assertEqual(cuts[0].start, words[0]["start"])

    def test_latin_transcript_unchanged(self):
        self.assertEqual(detect_repeats(_words([("alag", None), ("alag", None)])), [])
        self.assertEqual(len(detect_repeats(_words([("main", None), ("main", None)]))), 1)

    def test_latin_chota_is_not_cut(self):
        # The rule romanizer writes छोटा as "chota"; a Latin-only transcript
        # with that spelling has no Devanagari text to fall back on
        self.assertEqual(detect_repeats(_words([("chota", None), ("chota", None)])), [])


if __name__ == "__main__":
    unittest.main()
