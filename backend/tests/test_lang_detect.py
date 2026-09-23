"""
Tests for captions/lang_detect.py — word-level language tagging.

Covers:
  * Devanagari script → "hi"
  * Tamil/Telugu native scripts → "ta" / "te"
  * Latin-script Hindi words (Hinglish) → "hi"
  * Latin-script Tamil words (Tanglish) → "ta"
  * Latin-script Telugu words (Tenglish) → "te"
  * Latin-script English words → "en"
  * Segment-level hint inheritance
  * Mid-utterance language switches
  * Backward compatibility: pre-tagged words are left unchanged
  * Words without a lang tag inherit the segment_lang hint
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.lang_detect import tag_words


def _words(*pairs):
    """Build a minimal word list from (text, start) pairs."""
    return [
        {"start": float(i), "end": float(i) + 0.5, "text": t}
        for i, t in enumerate(pairs)
    ]


def _langs(words):
    return [w["lang"] for w in words]


class ScriptDetectionTest(unittest.TestCase):
    """Script-based detection — no ambiguity."""

    def test_devanagari_is_hi(self):
        result = tag_words(_words("मतलब", "यानी", "तो"))
        self.assertEqual(_langs(result), ["hi", "hi", "hi"])

    def test_tamil_script_is_ta(self):
        result = tag_words(_words("வணக்கம்", "இது"))
        self.assertEqual(_langs(result), ["ta", "ta"])

    def test_telugu_script_is_te(self):
        result = tag_words(_words("నమస్తే", "ఇది"))
        self.assertEqual(_langs(result), ["te", "te"])

    def test_mixed_script_in_one_transcript(self):
        words = _words("hello", "मतलब", "world")
        result = tag_words(words, segment_lang="hi")
        # "hello"/"world" → "en" (not in Hindi lexicon, segment hint can pull it)
        # "मतलब" → "hi" (Devanagari)
        self.assertEqual(result[1]["lang"], "hi")

    def test_devanagari_with_latin_chunk(self):
        words = _words("और", "bahut", "मेहनत")
        result = tag_words(words, segment_lang="hi")
        self.assertEqual(result[0]["lang"], "hi")   # Devanagari
        self.assertEqual(result[1]["lang"], "hi")   # Hindi Latin lexicon hit
        self.assertEqual(result[2]["lang"], "hi")   # Devanagari


class LatinDisambiguationTest(unittest.TestCase):
    """Latin-script disambiguation: Hindi vs Tamil vs Telugu vs English."""

    def test_hindi_lexical_words_tagged_hi(self):
        result = tag_words(_words("matlab", "yaani", "toh", "na", "bas"))
        self.assertTrue(all(w["lang"] == "hi" for w in result))

    def test_tamil_lexical_words_tagged_ta(self):
        result = tag_words(_words("naan", "romba", "sari", "machan"))
        self.assertTrue(all(w["lang"] == "ta" for w in result))

    def test_telugu_lexical_words_tagged_te(self):
        result = tag_words(_words("nenu", "ante", "emo", "chestunna"))
        self.assertTrue(all(w["lang"] == "te" for w in result))

    def test_clear_english_words_tagged_en(self):
        result = tag_words(_words("machine", "learning", "pipeline", "API"))
        self.assertTrue(all(w["lang"] == "en" for w in result))

    def test_english_override_words_stay_en(self):
        # Words in _ENGLISH_OVERRIDE should always be "en" regardless of
        # segment hint when the hint is not a regional language.
        result = tag_words(_words("so", "and", "the", "is"), segment_lang="en")
        self.assertTrue(all(w["lang"] == "en" for w in result))

    def test_segment_hint_pulls_known_regional_words(self):
        # A word in the Hindi lexicon gets tagged 'hi' regardless of hint.
        words = _words("avishkar", "matlab")
        result = tag_words(words, segment_lang="hi")
        # "avishkar" is not in any lexicon → falls back to "en"
        self.assertEqual(result[0]["lang"], "en")
        # "matlab" IS in the Hindi lexicon → "hi"
        self.assertEqual(result[1]["lang"], "hi")


class MidUtteranceSwitchTest(unittest.TestCase):
    """Mid-utterance language switches are tagged word by word."""

    def test_hinglish_switch_en_noun_inside_hi_sentence(self):
        """English technical noun inside a Hindi sentence should be "en"."""
        words = _words("hamare", "backend", "mein", "teen", "APIs", "hain")
        result = tag_words(words, segment_lang="hi")
        self.assertEqual(result[0]["lang"], "hi")   # hamare → Hindi lexicon
        self.assertEqual(result[1]["lang"], "en")   # backend → English
        self.assertEqual(result[2]["lang"], "hi")   # mein → Hindi lexicon
        self.assertEqual(result[4]["lang"], "en")   # APIs → English

    def test_tanglish_switch_en_noun_inside_ta_sentence(self):
        """English words inside a Tamil sentence should be "en"."""
        words = _words("antha", "project", "konjam", "difficult", "irukkum")
        result = tag_words(words, segment_lang="ta")
        self.assertEqual(result[0]["lang"], "ta")   # antha → Tamil segment
        self.assertEqual(result[1]["lang"], "en")   # project → English
        self.assertEqual(result[2]["lang"], "ta")   # konjam → Tamil lexicon
        self.assertEqual(result[3]["lang"], "en")   # difficult → English
        self.assertEqual(result[4]["lang"], "ta")   # irukkum → Tamil segment

    def test_tenglish_switch_en_noun_inside_te_sentence(self):
        """English words inside a Telugu sentence should be "en"."""
        words = _words("mana", "team", "oka", "feature", "build", "chestundi")
        result = tag_words(words, segment_lang="te")
        self.assertEqual(result[0]["lang"], "te")   # mana → Telugu lexicon
        self.assertEqual(result[1]["lang"], "en")   # team → English
        self.assertEqual(result[3]["lang"], "en")   # feature → English
        self.assertEqual(result[5]["lang"], "te")   # chestundi → Telugu segment


class BackwardCompatibilityTest(unittest.TestCase):
    """Pre-tagged words must be left unchanged; untagged words get a tag."""

    def test_pre_tagged_words_are_unchanged(self):
        words = [
            {"start": 0.0, "end": 0.5, "text": "matlab", "lang": "en"},  # wrong but pre-tagged
            {"start": 0.5, "end": 1.0, "text": "hamare"},  # in Hindi lexicon → "hi"
        ]
        result = tag_words(words, segment_lang="hi")
        self.assertEqual(result[0]["lang"], "en")   # left unchanged
        self.assertEqual(result[1]["lang"], "hi")   # newly tagged via lexicon

    def test_no_segment_lang_defaults_to_en(self):
        words = _words("unknown_word_xyz")
        result = tag_words(words)
        self.assertEqual(result[0]["lang"], "en")

    def test_empty_word_list_returns_empty(self):
        self.assertEqual(tag_words([]), [])

    def test_existing_fields_are_preserved(self):
        words = [{"start": 0.0, "end": 0.5, "text": "matlab", "hinglish": "matlab"}]
        result = tag_words(words, segment_lang="hi")
        self.assertEqual(result[0]["hinglish"], "matlab")
        self.assertIn("lang", result[0])

    def test_all_pre_tagged_returns_as_is(self):
        words = [
            {"start": 0.0, "end": 0.5, "text": "naan", "lang": "ta"},
            {"start": 0.5, "end": 1.0, "text": "main", "lang": "hi"},
        ]
        result = tag_words(words)
        self.assertEqual(result[0]["lang"], "ta")
        self.assertEqual(result[1]["lang"], "hi")


class MonolingualRegressionTest(unittest.TestCase):
    """Monolingual transcripts must produce identical tag distributions."""

    def test_monolingual_english_all_en(self):
        words = _words("this", "is", "a", "machine", "learning", "project")
        result = tag_words(words, segment_lang="en")
        # All should be tagged "en"
        self.assertTrue(all(w["lang"] == "en" for w in result))

    def test_monolingual_hindi_devanagari_all_hi(self):
        words = _words("यह", "एक", "परियोजना", "है")
        result = tag_words(words, segment_lang="hi")
        self.assertTrue(all(w["lang"] == "hi" for w in result))

    def test_monolingual_hinglish_latin_predominantly_hi(self):
        words = _words("matlab", "achha", "bahut", "aur", "kaam")
        result = tag_words(words, segment_lang="hi")
        # All these are in the Hindi Latin lexicon
        self.assertTrue(all(w["lang"] == "hi" for w in result))


class DetectionEdgeCasesAndIntegrationTest(unittest.TestCase):
    """Punctuation stripping, technical words, fixture roundtrips, and transcription integration."""

    def test_punctuation_attached_tokens(self):
        words = _words("project,", "hain.", "API!", "toh...")
        result = tag_words(words, segment_lang="hi")
        self.assertEqual(result[0]["lang"], "en")   # project, should be en despite hi segment
        self.assertEqual(result[1]["lang"], "hi")   # hain. should be hi
        self.assertEqual(result[2]["lang"], "en")   # API! should be en
        self.assertEqual(result[3]["lang"], "hi")   # toh... should be hi

    def test_technical_words_stay_english(self):
        words_hi = _words("handle", "server", "architecture", "log")
        res_hi = tag_words(words_hi, segment_lang="hi")
        self.assertTrue(all(w["lang"] == "en" for w in res_hi))

        words_te = _words("build", "pipeline", "service", "bro")
        res_te = tag_words(words_te, segment_lang="te")
        self.assertTrue(all(w["lang"] == "en" for w in res_te))

        # Utterance test cases cited in review:
        s1 = _words("I", "need", "to", "do", "the", "log", "analysis")
        res_s1 = tag_words(s1, segment_lang="hi")
        self.assertEqual([w["lang"] for w in res_s1], ["en"] * 7)

        s2 = _words("Hi", "bro", "can", "you", "handle", "the", "sound")
        res_s2 = tag_words(s2, segment_lang="ta")
        self.assertEqual([w["lang"] for w in res_s2], ["en"] * 7)

    def test_fixtures_untagged_roundtrip(self):
        import json
        fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
        cases = [
            ("hinglish_codemixed.json", "hi"),
            ("tanglish_codemixed.json", "ta"),
            ("tenglish_codemixed.json", "te"),
        ]
        for fname, slang in cases:
            path = os.path.join(fixtures_dir, fname)
            with open(path, encoding="utf-8") as f:
                fx = json.load(f)
            expected = [w["lang"] for w in fx["words"]]
            stripped = [{k: v for k, v in w.items() if k != "lang"} for w in fx["words"]]
            tagged = tag_words(stripped, segment_lang=slang)
            actual = [w["lang"] for w in tagged]
            self.assertEqual(
                actual, expected,
                f"Tagging mismatch on untagged words in {fname}"
            )

    def test_transcribe_video_integrates_tag_words(self):
        from unittest.mock import patch
        from captions import transcribe

        fake_result = {
            "language": "hi",
            "backend": "whisper:test",
            "words": [
                {"start": 0.0, "end": 0.5, "text": "matlab"},
                {"start": 0.5, "end": 1.0, "text": "project,"},
            ],
        }
        with patch.object(transcribe, "_extract_audio"), \
             patch.object(transcribe, "_transcribe_mlx", return_value=fake_result):
            out = transcribe.transcribe_video("fake.mp4")
            self.assertIn("lang", out["words"][0])
            self.assertEqual(out["words"][0]["lang"], "hi")
            self.assertEqual(out["words"][1]["lang"], "en")


if __name__ == "__main__":
    unittest.main()
