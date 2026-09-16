import unittest

from captions.retakes import MAX_WORDS, Phrase, find_candidates


class RetakeMaxPhraseLengthTests(unittest.TestCase):
    def _phrase(self, start, words):
        tokens = tuple(words)
        return Phrase(
            start=start,
            end=start + 12.0,
            i0=0,
            i1=len(tokens),
            text=" ".join(tokens),
            tokens=tokens,
        )

    def test_long_valid_retakes_are_not_dropped_at_previous_limit(self):
        self.assertGreater(MAX_WORDS, 40)

        words = [f"word{i}" for i in range(41)]
        first = self._phrase(0.0, words)
        second = self._phrase(10.0, words)

        groups = find_candidates([first, second])

        self.assertEqual(groups, [[first, second]])

    def test_pathologically_long_phrase_still_respects_word_limit(self):
        words = [f"word{i}" for i in range(MAX_WORDS + 1)]
        phrase = self._phrase(0.0, words)
        later = self._phrase(10.0, words)

        self.assertEqual(find_candidates([phrase, later]), [])


if __name__ == "__main__":
    unittest.main()
