import unittest

from captions import transcribe


class ASRTimestampValidationTests(unittest.TestCase):
    def _result(self, words, segments=None):
        return {
            "segments": segments if segments is not None else [
                {"start": 0.0, "end": 2.0, "text": "fallback", "words": words}
            ]
        }

    def test_accepts_finite_non_negative_ordered_word_timestamps(self):
        result = transcribe._words_from_result(self._result([
            {"start": 0.0, "end": 0.4, "word": "one"},
            {"start": 0.4, "end": 0.9, "word": "two"},
            {"start": 1.0, "end": 1.2, "word": "three"},
        ]))

        self.assertEqual(result, [
            {"start": 0.0, "end": 0.4, "text": "one"},
            {"start": 0.4, "end": 0.9, "text": "two"},
            {"start": 1.0, "end": 1.2, "text": "three"},
        ])

    def test_rejects_non_finite_timestamp(self):
        for value in [float("nan"), float("inf"), float("-inf")]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "timestamps must be finite"):
                    transcribe._words_from_result(self._result([
                        {"start": 0.0, "end": value, "word": "one"},
                    ]))

    def test_rejects_negative_timestamp(self):
        with self.assertRaisesRegex(ValueError, "timestamps must be non-negative"):
            transcribe._words_from_result(self._result([
                {"start": -0.1, "end": 0.4, "word": "one"},
            ]))

    def test_rejects_reversed_word_timestamp(self):
        with self.assertRaisesRegex(ValueError, "start must not exceed end"):
            transcribe._words_from_result(self._result([
                {"start": 0.8, "end": 0.4, "word": "one"},
            ]))

    def test_rejects_non_ordered_word_start(self):
        with self.assertRaisesRegex(ValueError, "timestamps must be ordered"):
            transcribe._words_from_result(self._result([
                {"start": 0.5, "end": 0.8, "word": "one"},
                {"start": 0.4, "end": 0.9, "word": "two"},
            ]))

    def test_rejects_non_ordered_word_end(self):
        with self.assertRaisesRegex(ValueError, "timestamps must be ordered"):
            transcribe._words_from_result(self._result([
                {"start": 0.0, "end": 0.9, "word": "one"},
                {"start": 1.0, "end": 0.8, "word": "two"},
            ]))

    def test_validates_sentence_fallback_timestamps(self):
        with self.assertRaisesRegex(ValueError, "timestamps must be non-negative"):
            transcribe._words_from_result(self._result(
                [],
                segments=[{"start": -1.0, "end": 2.0, "text": "fallback", "words": []}],
            ))

    def test_rejects_malformed_word_timestamp(self):
        with self.assertRaises(ValueError):
            transcribe._words_from_result(self._result([
                {"start": "bad", "end": 0.4, "word": "one"},
            ]))


if __name__ == "__main__":
    unittest.main()
