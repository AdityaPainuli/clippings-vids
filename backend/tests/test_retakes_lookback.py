import unittest

from captions.retakes import Phrase, find_candidates, similarity


class RetakeCandidateLookbackTests(unittest.TestCase):
    @staticmethod
    def phrase(start: float, tokens: tuple[str, ...]) -> Phrase:
        return Phrase(
            start=start,
            end=start + 2.0,
            i0=0,
            i1=len(tokens),
            text=" ".join(tokens),
            tokens=tokens,
        )

    def test_matching_phrase_beyond_eight_candidates_is_found(self):
        first = self.phrase(
            0.0,
            ("record", "the", "same", "line", "one", "more"),
        )
        middle = [
            self.phrase(
                2.5 + index * 2.5,
                (
                    f"unique{index}a",
                    f"unique{index}b",
                    f"unique{index}c",
                    f"unique{index}d",
                    f"unique{index}e",
                    f"unique{index}f",
                ),
            )
            for index in range(9)
        ]
        last = self.phrase(
            25.0,
            ("record", "the", "same", "line", "one", "more"),
        )

        self.assertGreaterEqual(similarity(first, last), 0.35)
        self.assertEqual(
            find_candidates([first, *middle, last]),
            [[first, last]],
        )

    def test_matching_phrase_outside_time_window_is_not_grouped(self):
        first = self.phrase(
            0.0,
            ("record", "the", "same", "line", "one", "more"),
        )
        last = self.phrase(
            47.1,
            ("record", "the", "same", "line", "one", "more"),
        )

        self.assertGreaterEqual(similarity(first, last), 0.35)
        self.assertEqual(find_candidates([first, last]), [])


if __name__ == "__main__":
    unittest.main()
