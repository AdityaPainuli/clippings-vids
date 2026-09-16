import unittest

from captions.retakes import Phrase, find_candidates, similarity


class RetakeStopwordSimilarityTests(unittest.TestCase):
    def _phrase(self, start, tokens):
        tokens = tuple(tokens)
        return Phrase(
            start=start,
            end=start + 4.0,
            i0=0,
            i1=len(tokens),
            text=" ".join(tokens),
            tokens=tokens,
        )

    def test_stopword_heavy_overlap_does_not_create_candidate(self):
        a = self._phrase(0.0, [
            "i", "think", "that", "the", "thing", "is", "in", "the", "right", "way"
        ])
        b = self._phrase(10.0, [
            "i", "think", "that", "the", "answer", "is", "on", "the", "wrong", "side"
        ])

        self.assertEqual(similarity(a, b), 0.0)
        self.assertEqual(find_candidates([a, b]), [])

    def test_shared_content_words_still_create_candidate(self):
        a = self._phrase(0.0, [
            "so", "the", "compound", "interest", "is", "really", "important"
        ])
        b = self._phrase(10.0, [
            "and", "compound", "interest", "is", "very", "important", "here"
        ])

        self.assertGreaterEqual(similarity(a, b), 0.35)
        self.assertEqual(find_candidates([a, b]), [[a, b]])


if __name__ == "__main__":
    unittest.main()
