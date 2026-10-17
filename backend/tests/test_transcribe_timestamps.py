import os
import sys
import unittest
import math

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from captions.transcribe import validate_word_timestamps, _words_from_result


class TestTranscribeTimestamps(unittest.TestCase):
    def test_valid_timestamps(self):
        words = [
            {"start": 0.0, "end": 0.5, "text": "hello"},
            {"start": 0.5, "end": 1.0, "text": "world"},
            {"start": 1.2, "end": 1.8, "text": "test"},
        ]
        validated = validate_word_timestamps(words)
        self.assertEqual(len(validated), 3)
        self.assertEqual(validated[0]["text"], "hello")

    def test_reject_nan_timestamp(self):
        words = [{"start": float("nan"), "end": 1.0, "text": "bad"}]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("non-finite", str(ctx.exception))

    def test_reject_inf_timestamp(self):
        words = [{"start": 0.0, "end": float("inf"), "text": "bad"}]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("non-finite", str(ctx.exception))

    def test_reject_negative_start(self):
        words = [{"start": -0.1, "end": 0.5, "text": "bad"}]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("negative", str(ctx.exception))

    def test_reject_negative_end(self):
        words = [{"start": 0.0, "end": -0.5, "text": "bad"}]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("negative", str(ctx.exception))

    def test_reject_start_greater_than_end(self):
        words = [{"start": 2.0, "end": 1.5, "text": "bad"}]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("reversed", str(ctx.exception))

    def test_reject_decreasing_start(self):
        words = [
            {"start": 1.0, "end": 2.0, "text": "first"},
            {"start": 0.5, "end": 2.5, "text": "second"},
        ]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("non-monotonic start", str(ctx.exception))

    def test_reject_decreasing_end(self):
        words = [
            {"start": 1.0, "end": 3.0, "text": "first"},
            {"start": 1.5, "end": 2.0, "text": "second"},
        ]
        with self.assertRaises(ValueError) as ctx:
            validate_word_timestamps(words)
        self.assertIn("non-monotonic end", str(ctx.exception))

    def test_words_from_result_word_level(self):
        result = {
            "segments": [
                {
                    "words": [
                        {"start": 0.0, "end": 0.5, "word": "word1"},
                        {"start": 0.5, "end": 1.0, "word": "word2"},
                    ]
                }
            ]
        }
        words = _words_from_result(result)
        self.assertEqual(len(words), 2)
        self.assertEqual(words[0]["text"], "word1")

    def test_words_from_result_sentence_fallback(self):
        result = {
            "segments": [
                {"start": 0.0, "end": 1.5, "text": "Sentence one."},
                {"start": 1.5, "end": 3.0, "text": "Sentence two."},
            ]
        }
        words = _words_from_result(result)
        self.assertEqual(len(words), 2)
        self.assertEqual(words[0]["text"], "Sentence one.")
        self.assertEqual(words[1]["text"], "Sentence two.")

    def test_words_from_result_rejects_malformed_sentence_fallback(self):
        result = {
            "segments": [
                {"start": 2.0, "end": 1.0, "text": "Sentence inverted."},
            ]
        }
        with self.assertRaises(ValueError):
            _words_from_result(result)


if __name__ == "__main__":
    unittest.main()
