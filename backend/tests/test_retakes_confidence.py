import unittest
from unittest.mock import patch

from captions import retakes


class RetakeConfidenceTests(unittest.TestCase):
    def setUp(self):
        self.group = [
            retakes.Phrase(0.0, 2.0, 0, 6, "first attempt", ("first", "attempt")),
            retakes.Phrase(3.0, 5.0, 6, 12, "second attempt", ("second", "attempt")),
        ]
        self.words = [{"start": 0.0, "end": 0.5, "text": "hello"}]

    def _detect_with_confidence(self, confidence):
        verdict = {
            "retake": True,
            "keep": 1,
            "confidence": confidence,
            "reason": "second attempt is clearer",
        }
        with patch.object(retakes, "find_candidates", return_value=[self.group]), \
                patch.object(retakes, "_ask", return_value=verdict):
            return retakes.detect(self.words, silences=[], complete=lambda *args, **kwargs: "unused")

    def test_non_finite_confidence_becomes_zero(self):
        for confidence in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(confidence=confidence):
                result = self._detect_with_confidence(confidence)
                self.assertEqual(result["cuts"][0].confidence, 0.0)
                self.assertEqual(result["groups"][0]["confidence"], 0.0)

    def test_non_numeric_confidence_still_defaults_to_zero(self):
        result = self._detect_with_confidence("not-a-number")
        self.assertEqual(result["cuts"][0].confidence, 0.0)
        self.assertEqual(result["groups"][0]["confidence"], 0.0)

    def test_finite_confidence_is_clamped_as_before(self):
        low = self._detect_with_confidence(-0.5)
        high = self._detect_with_confidence(1.5)
        self.assertEqual(low["cuts"][0].confidence, 0.0)
        self.assertEqual(high["cuts"][0].confidence, 1.0)


if __name__ == "__main__":
    unittest.main()
