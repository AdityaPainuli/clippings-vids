"""
Tests for the rule-based romanizer in captions/romanize.py — the path the
free desktop app uses when no GOOGLE_API_KEY is set.

The cases are a table here rather than a JSON file under fixtures/, because
scripts/eval_tighten.py loads every fixtures/*.json as a Tighten transcript.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.romanize import _rule_romanize  # noqa: E402

# Written ा at the end is a real vowel and must survive
FINAL_AA = {
    "छोटा": "chota", "बड़ा": "bada", "अच्छा": "achha",
    "बेटा": "beta", "पैसा": "paisa", "ज़्यादा": "zyada",
}

# Inherent schwa between a vowel-consonant and a consonant-vowel is dropped
MEDIAL_SCHWA = {
    "सबसे": "sabse", "मतलब": "matlab", "समझे": "samjhe",
    "बचपन": "bachpan", "अदरक": "adrak",
}

# Medial schwa dropped and final ā kept, in the same word
BOTH = {
    "कमरा": "kamra", "लड़का": "ladka", "अपना": "apna",
    "करना": "karna", "समझना": "samajhna",
}

# Already right on main; must not move
UNCHANGED = {
    "चलो": "chalo", "सच": "sach", "न": "na", "तरह": "tarah",
    "ज़िंदगी": "zindagi", "नमस्ते": "namaste", "नहीं": "nahin", "क्या": "kya",
}

# Pronounced schwa: before a word-final short ि (a Sanskrit word) and at a
# prefix boundary (अ + समान)
RETAINED_SCHWA = {
    "प्रगति": "pragati", "अनुमति": "anumati", "सेनापति": "senapati",
    "असमान": "asaman", "असमर्थ": "asamarth", "असहाय": "asahay",
    "नासमझ": "nasamajh",
}


class RuleRomanizeTest(unittest.TestCase):
    def assertRomanizes(self, cases):
        for devanagari, expected in cases.items():
            with self.subTest(word=devanagari):
                self.assertEqual(_rule_romanize(devanagari), expected)

    def test_final_long_aa_is_kept(self):
        self.assertRomanizes(FINAL_AA)

    def test_medial_schwa_is_dropped(self):
        self.assertRomanizes(MEDIAL_SCHWA)

    def test_medial_and_final_in_one_word(self):
        self.assertRomanizes(BOTH)

    def test_existing_output_unchanged(self):
        self.assertRomanizes(UNCHANGED)

    def test_pronounced_schwa_is_kept(self):
        self.assertRomanizes(RETAINED_SCHWA)

    def test_kept_schwa_does_not_block_other_deletions(self):
        self.assertRomanizes({"लखपति": "lakhpati", "असमानता": "asamanta"})

    def test_join_controls_do_not_split_a_word(self):
        # वक्त plain, then with ZWJ (U+200D) and ZWNJ (U+200C) after the virama
        self.assertRomanizes({"\u0935\u0915\u094d\u0924": "vakt",
                              "\u0935\u0915\u094d\u200d\u0924": "vakt",
                              "\u0935\u0915\u094d\u200c\u0924": "vakt"})

    def test_trailing_punctuation_survives(self):
        self.assertRomanizes({"सच?": "sach?", "छोटा,": "chota,"})

    def test_hyphenated_reduplication_drops_both_schwas(self):
        self.assertRomanizes({"अलग-अलग": "alag-alag"})

    def test_flap_is_d_in_either_encoding(self):
        # ड़ as one code point (U+095C) and as ड + nukta
        self.assertRomanizes({"\u092c\u095c\u093e": "bada",
                              "\u092c\u0921\u093c\u093e": "bada"})


if __name__ == "__main__":
    unittest.main()
