import os
import sys
import unittest
import math

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

# Restore real captions package in sys.modules if stubbed by a prior test suite
if "captions" in sys.modules and (not hasattr(sys.modules["captions"], "__file__") or not sys.modules["captions"].__file__):
    del sys.modules["captions"]
    for mod in list(sys.modules.keys()):
        if mod.startswith("captions."):
            del sys.modules[mod]

from captions import retakes


class TestRetakesConfidence(unittest.TestCase):
    def setUp(self):
        if "captions" in sys.modules and (not hasattr(sys.modules["captions"], "__file__") or not sys.modules["captions"].__file__):
            del sys.modules["captions"]
            for mod in list(sys.modules.keys()):
                if mod.startswith("captions."):
                    del sys.modules[mod]
        import captions.retakes
        self.retakes = captions.retakes

    def _run_detect_with_confidence(self, conf_val):
        # Words creating two phrases that will be identified as candidates
        words = [
            {"text": "so", "start": 0.0, "end": 0.5},
            {"text": "the", "start": 0.5, "end": 1.0},
            {"text": "thing", "start": 1.0, "end": 1.5},
            {"text": "about", "start": 1.5, "end": 2.0},
            {"text": "compound", "start": 2.0, "end": 2.5},
            {"text": "interest.", "start": 2.5, "end": 3.0},
            # Pause between phrases
            {"text": "so", "start": 4.0, "end": 4.5},
            {"text": "the", "start": 4.5, "end": 5.0},
            {"text": "thing", "start": 5.0, "end": 5.5},
            {"text": "about", "start": 5.5, "end": 6.0},
            {"text": "compound", "start": 6.0, "end": 6.5},
            {"text": "interest,", "start": 6.5, "end": 7.0},
            {"text": "right?", "start": 7.0, "end": 7.5},
        ]

        def fake_complete(prompt, system=None, max_tokens=None):
            import json
            return json.dumps({
                "retake": True,
                "keep": 1,
                "confidence": conf_val,
                "reason": "second take was better",
            })

        res = self.retakes.detect(words, silences=[(3.0, 4.0)], complete=fake_complete)
        self.assertEqual(len(res["cuts"]), 1)
        return res["cuts"][0].confidence

    def test_finite_confidence(self):
        conf = self._run_detect_with_confidence(0.85)
        self.assertEqual(conf, 0.85)

    def test_clamped_high_confidence(self):
        conf = self._run_detect_with_confidence(1.5)
        self.assertEqual(conf, 1.0)

    def test_clamped_low_confidence(self):
        conf = self._run_detect_with_confidence(-0.3)
        self.assertEqual(conf, 0.0)

    def test_nan_confidence_fallback(self):
        conf = self._run_detect_with_confidence(float("nan"))
        self.assertEqual(conf, 0.0)
        self.assertTrue(math.isfinite(conf))

    def test_inf_confidence_fallback(self):
        conf = self._run_detect_with_confidence(float("inf"))
        self.assertEqual(conf, 0.0)
        self.assertTrue(math.isfinite(conf))

    def test_neginf_confidence_fallback(self):
        conf = self._run_detect_with_confidence(float("-inf"))
        self.assertEqual(conf, 0.0)
        self.assertTrue(math.isfinite(conf))

    def test_string_nan_confidence_fallback(self):
        conf = self._run_detect_with_confidence("NaN")
        self.assertEqual(conf, 0.0)
        self.assertTrue(math.isfinite(conf))

    def test_string_inf_confidence_fallback(self):
        conf = self._run_detect_with_confidence("Infinity")
        self.assertEqual(conf, 0.0)
        self.assertTrue(math.isfinite(conf))


if __name__ == "__main__":
    unittest.main()
