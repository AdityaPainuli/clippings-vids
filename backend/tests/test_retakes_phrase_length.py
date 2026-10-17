import os
import sys
import unittest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from captions.retakes import Phrase, find_candidates, MAX_WORDS


class TestRetakesPhraseLength(unittest.TestCase):
    def test_max_words_is_finite_and_increased(self):
        self.assertGreater(MAX_WORDS, 40)
        self.assertEqual(MAX_WORDS, 80)

    def test_phrase_above_40_words_is_accepted_as_candidate(self):
        # 50 words repeated twice
        tokens1 = tuple([f"word{i}" for i in range(50)])
        tokens2 = tuple([f"word{i}" for i in range(50)])

        p1 = Phrase(start=0.0, end=15.0, i0=0, i1=50, text=" ".join(tokens1), tokens=tokens1)
        p2 = Phrase(start=16.0, end=31.0, i0=50, i1=100, text=" ".join(tokens2), tokens=tokens2)

        candidates = find_candidates([p1, p2])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(len(candidates[0]), 2)
        self.assertEqual(candidates[0][0], p1)
        self.assertEqual(candidates[0][1], p2)

    def test_phrase_above_new_max_words_is_excluded(self):
        # 85 words (exceeds MAX_WORDS = 80)
        tokens1 = tuple([f"word{i}" for i in range(85)])
        tokens2 = tuple([f"word{i}" for i in range(85)])

        p1 = Phrase(start=0.0, end=25.0, i0=0, i1=85, text=" ".join(tokens1), tokens=tokens1)
        p2 = Phrase(start=26.0, end=51.0, i0=85, i1=170, text=" ".join(tokens2), tokens=tokens2)

        candidates = find_candidates([p1, p2])
        self.assertEqual(len(candidates), 0)

    def test_phrase_below_min_words_is_still_excluded(self):
        # 4 words (below MIN_WORDS = 6)
        tokens1 = ("word0", "word1", "word2", "word3")
        tokens2 = ("word0", "word1", "word2", "word3")

        p1 = Phrase(start=0.0, end=1.5, i0=0, i1=4, text=" ".join(tokens1), tokens=tokens1)
        p2 = Phrase(start=2.0, end=3.5, i0=4, i1=8, text=" ".join(tokens2), tokens=tokens2)

        candidates = find_candidates([p1, p2])
        self.assertEqual(len(candidates), 0)


if __name__ == "__main__":
    unittest.main()
