
import unittest
from unittest.mock import Mock, patch

import requests

from captions import llm, romanize


class LocalLLMTests(unittest.TestCase):
    def setUp(self):
        llm._LOCAL_AVAILABILITY_CACHE = None
        llm._LOCAL_AVAILABILITY_CHECKED_AT = 0.0

    def tearDown(self):
        llm._LOCAL_AVAILABILITY_CACHE = None
        llm._LOCAL_AVAILABILITY_CHECKED_AT = 0.0

    def test_local_endpoint_available(self):
        response = Mock()
        response.status_code = 200

        with patch.dict("os.environ", {"BOLCAP_LLM_PROVIDER": "openai-compatible"}), \
                patch("requests.get", return_value=response) as mock_get:
            self.assertEqual(llm.provider(), "openai-compatible")

        mock_get.assert_called_once()

    def test_refused_local_connection_falls_back_to_rules(self):
        with patch.dict("os.environ", {
            "BOLCAP_LLM_PROVIDER": "openai-compatible",
        }), patch("requests.get", side_effect=requests.exceptions.ConnectionError), \
                patch.object(llm, "complete", return_value=None), \
                patch.object(romanize, "_rule_romanize", side_effect=["main", "achchh"]) as mock_rules:
            result = romanize.romanize_words([
                {"start": 0, "end": 1, "text": "मैं", "lang": "hi"},
                {"start": 1, "end": 2, "text": "अच्छा", "lang": "hi"},
            ])

        self.assertEqual(
            [word["hinglish"] for word in result],
            ["main", "achchh"],
        )
        mock_rules.assert_any_call("मैं")
        mock_rules.assert_any_call("अच्छा")


if __name__ == "__main__":
    unittest.main()
