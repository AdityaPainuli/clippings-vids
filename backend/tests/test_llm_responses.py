import unittest
from unittest.mock import Mock

from captions import llm


class TestGeminiResponses(unittest.TestCase):
    def _response(self, payload):
        response = Mock()
        response.status_code = 200
        response.json.return_value = payload
        response.text = ""
        return response

    def test_empty_candidates_raise_llm_error(self):
        response = self._response({"candidates": []})
        requests = Mock()
        requests.post.return_value = response

        with self.assertRaisesRegex(llm.LLMError, "Gemini returned no candidates"):
            llm._gemini(requests, "prompt", "", 100)

    def test_prompt_block_is_reported(self):
        response = self._response({
            "promptFeedback": {
                "blockReason": "SAFETY",
                "blockReasonMessage": "Request blocked by safety filters.",
            }
        })
        requests = Mock()
        requests.post.return_value = response

        with self.assertRaisesRegex(llm.LLMError, "Gemini blocked the request \(SAFETY\)"):
            llm._gemini(requests, "prompt", "", 100)

    def test_candidate_safety_finish_reason_is_reported(self):
        response = self._response({
            "candidates": [{
                "finishReason": "SAFETY",
                "content": {"parts": []},
            }]
        })
        requests = Mock()
        requests.post.return_value = response

        with self.assertRaisesRegex(llm.LLMError, "Gemini blocked the response \(SAFETY\)"):
            llm._gemini(requests, "prompt", "", 100)

    def test_empty_text_candidate_is_not_treated_as_success(self):
        response = self._response({
            "candidates": [{
                "finishReason": "STOP",
                "content": {"parts": []},
            }]
        })
        requests = Mock()
        requests.post.return_value = response

        with self.assertRaisesRegex(llm.LLMError, "Gemini returned an empty response"):
            llm._gemini(requests, "prompt", "", 100)

    def test_valid_candidate_still_returns_text(self):
        response = self._response({
            "candidates": [{
                "finishReason": "STOP",
                "content": {"parts": [{"text": "[\"ok\"]"}]},
            }]
        })
        requests = Mock()
        requests.post.return_value = response

        self.assertEqual(llm._gemini(requests, "prompt", "", 100), '["ok"]')


if __name__ == "__main__":
    unittest.main()
