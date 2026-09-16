import unittest

from captions.retakes import Phrase, find_candidates, similarity


class RetakeCandidateGroupingTests(unittest.TestCase):
    @staticmethod
    def phrase(start: float, tokens: tuple[str, ...]) -> Phrase:
        return Phrase(
            start=start,
            end=start + 10.0,
            i0=0,
            i1=len(tokens),
            text=" ".join(tokens),
            tokens=tokens,
        )

    def test_candidate_group_requires_pairwise_compatibility(self):
        first = self.phrase(
            0.0,
            ("alpha", "beta", "gamma", "delta", "foo", "bar", "baz", "qux"),
        )
        bridge = self.phrase(
            10.1,
            ("alpha", "beta", "gamma", "delta", "corge", "grault", "garply", "waldo"),
        )
        last = self.phrase(
            20.2,
            ("alpha", "beta", "corge", "grault", "xyzzy", "plugh", "thud", "fred"),
        )

        self.assertGreaterEqual(similarity(first, bridge), 0.35)
        self.assertGreaterEqual(similarity(bridge, last), 0.35)
        self.assertLess(similarity(first, last), 0.35)

        groups = find_candidates([first, bridge, last])

        self.assertEqual(groups, [[first, bridge]])
        self.assertNotIn(last, groups[0])

    def test_three_mutually_similar_attempts_stay_together(self):
        first = self.phrase(
            0.0,
            ("the", "quick", "brown", "fox", "jumps", "over", "the", "dog"),
        )
        second = self.phrase(
            10.1,
            ("the", "quick", "brown", "fox", "jumps", "over", "a", "dog"),
        )
        third = self.phrase(
            20.2,
            ("the", "quick", "brown", "fox", "leaps", "over", "a", "dog"),
        )

        groups = find_candidates([first, second, third])

        self.assertEqual(groups, [[first, second, third]])


if __name__ == "__main__":
    unittest.main()
