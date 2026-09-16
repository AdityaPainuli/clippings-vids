import unittest
from unittest.mock import patch

from captions import retakes
from captions import llm


class RetakeRetryTests(unittest.TestCase):
    def _group(self):
        return [
            retakes.Phrase(0.0, 2.0, 0, 6, "this is the same line again", ("this", "is", "the", "same", "line", "again")),
            retakes.Phrase(3.0, 5.0, 6, 12, "this is the same line again", ("this", "is", "the", "same", "line", "again")),
        ]

    def test_retryable_failure_is_retried_and_can_succeed(self):
        group = self._group()
        transient = llm.LLMError("service unavailable", retryable=True)

        with patch.object(retakes, "_ask", side_effect=[transient, {"retake": False}]) as ask, \
                patch.object(retakes.time, "sleep") as sleep:
            result = retakes._ask_with_retry(group, lambda *args, **kwargs: None)

        self.assertEqual(result, {"retake": False})
        self.assertEqual(ask.call_count, 2)
        sleep.assert_called_once_with(retakes.TRANSIENT_RETRY_DELAY)

    def test_retryable_failure_after_retry_is_raised(self):
        group = self._group()
        transient = llm.LLMError("service unavailable", retryable=True)

        with patch.object(retakes, "_ask", side_effect=transient) as ask, \
                patch.object(retakes.time, "sleep") as sleep:
            with self.assertRaises(llm.LLMError):
                retakes._ask_with_retry(group, lambda *args, **kwargs: None)

        self.assertEqual(ask.call_count, 2)
        sleep.assert_called_once_with(retakes.TRANSIENT_RETRY_DELAY)

    def test_non_retryable_failure_is_not_retried(self):
        group = self._group()
        permanent = llm.LLMError("invalid api key")

        with patch.object(retakes, "_ask", side_effect=permanent) as ask, \
                patch.object(retakes.time, "sleep") as sleep:
            with self.assertRaises(llm.LLMError):
                retakes._ask_with_retry(group, lambda *args, **kwargs: None)

        ask.assert_called_once()
        sleep.assert_not_called()

    def test_detection_continues_after_exhausted_transient_group(self):
        groups = [self._group(), self._group()]
        transient = llm.LLMError("temporary outage", retryable=True)
        verdict = {"retake": True, "keep": 1, "confidence": 0.9, "reason": "same line"}

        with patch.object(retakes, "split_phrases", return_value=[]), \
                patch.object(retakes, "find_candidates", return_value=groups), \
                patch.object(retakes, "_ask_with_retry", side_effect=[transient, verdict]):
            result = retakes.detect(
                [{"start": 0.0, "end": 1.0, "text": "unused"}],
                silences=[],
                complete=lambda *args, **kwargs: None,
            )

        self.assertEqual(result["asked"], 2)
        self.assertEqual(len(result["groups"]), 1)
        self.assertEqual(result["groups"][0]["keep"], 1)
        self.assertEqual(result["status"], "model-error")
        self.assertIn("continued with the remaining candidate groups", result["error"])
        self.assertEqual(len(result["cuts"]), 1)


if __name__ == "__main__":
    unittest.main()
