import json
import os
import sys
import types
import unittest
from unittest.mock import patch

from captions import romanize


class RomanizeValidationTests(unittest.TestCase):
    def test_accepts_natural_hinglish_spelling(self):
        words = ["मैं", "अच्छा"]
        with patch.object(romanize, "_rule_romanize", side_effect=["main", "achchh"]):
            self.assertTrue(romanize._valid_llm_output(words, ["main", "achha"]))

    def test_rejects_semantically_unrelated_translation(self):
        words = ["मैं", "अच्छा"]
        with patch.object(romanize, "_rule_romanize", side_effect=["main", "achchh"]):
            self.assertFalse(romanize._valid_llm_output(words, ["what", "good"]))

    def test_rejects_short_translated_token(self):
        with patch.object(romanize, "_rule_romanize", return_value="hain"):
            self.assertFalse(romanize._valid_llm_output(["हैं"], ["he"]))

    def test_rejects_changed_latin_input(self):
        self.assertFalse(romanize._valid_llm_output(["hello"], ["hi"]))

    def test_rejects_multitoken_output(self):
        with patch.object(romanize, "_rule_romanize", return_value="main"):
            self.assertFalse(romanize._valid_llm_output(["मैं"], ["main ji"]))

    def test_llm_chunk_falls_back_on_invalid_semantics(self):
        class FakeResponse:
            text = json.dumps(["what", "good"])

        class FakeModel:
            def generate_content(self, prompt):
                return FakeResponse()

        fake_genai = types.SimpleNamespace(GenerativeModel=lambda **_: FakeModel())
        with patch.dict(sys.modules, {"google.generativeai": fake_genai}), \
                patch.dict(os.environ, {"GOOGLE_API_KEY": "test-key"}), \
                patch.object(romanize, "_rule_romanize", side_effect=["main", "achchh"]):
            self.assertIsNone(romanize._llm_romanize_chunk(["मैं", "अच्छा"]))


if __name__ == "__main__":
    unittest.main()
