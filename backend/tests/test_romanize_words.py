import unittest
from unittest.mock import patch

from captions import romanize


class RomanizeWordsTests(unittest.TestCase):
    def test_preserves_existing_hinglish_value(self):
        words = [
            {
                "start": 0,
                "end": 1,
                "text": "क्या",
                "hinglish": "kyaa",
            }
        ]

        with patch.object(
            romanize,
            "_llm_romanize_chunk",
            return_value=None,
        ):
            result = romanize.romanize_words(words)

        self.assertEqual(result, words)

    def test_preserves_existing_hinglish_while_romanizing_other_words(self):
        words = [
            {
                "start": 0,
                "end": 1,
                "text": "क्या",
                "hinglish": "kyaa",
            },
            {
                "start": 1,
                "end": 2,
                "text": "मैं",
                "lang": "hi",
            },
        ]
        with patch.object(
            romanize,
            "_llm_romanize_chunk",
            return_value=None,
        ), patch.object(
            romanize,
            "_rule_romanize",
            return_value="main",
        ):
            result = romanize.romanize_words(words)
        self.assertEqual(result[0]["hinglish"], "kyaa")
        self.assertEqual(result[1]["hinglish"], "main")


if __name__ == "__main__":
    unittest.main()
