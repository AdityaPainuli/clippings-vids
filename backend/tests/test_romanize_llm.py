import json
import os
import unittest
from unittest.mock import patch

import requests

from captions import llm, romanize


class RomanizeLLMTests(unittest.TestCase):
    def test_configured_providers_romanize_without_an_sdk(self):
        words = [
            {"text": "API", "lang": "en", "start": 0, "end": 1},
            {"text": "अच्छा", "lang": "hi", "start": 1, "end": 2},
            {"text": "मराठी", "lang": "mr", "start": 2, "end": 3},
            {"text": "मैं", "hinglish": "main", "start": 3, "end": 4},
        ]
        reply = '```json\n["achchha"]\n```'
        for provider, response, header, source_prompt, url in (
            ("gemini", {"candidates": [{"content": {"parts": [{"text": reply}]}}]},
             "x-goog-api-key", lambda body: body["contents"][0]["parts"][0]["text"],
             "https://generativelanguage.googleapis.com/v1beta/models/test-model:generateContent"),
            ("anthropic", {"content": [{"type": "text", "text": reply}]},
             "x-api-key", lambda body: body["messages"][0]["content"], llm.ANTHROPIC_URL),
        ):
            with self.subTest(provider=provider), patch.dict(os.environ, {
                "GOOGLE_API_KEY": "gemini-key", "ANTHROPIC_API_KEY": "anthropic-key",
                llm.PREFERRED_ENV: provider,
            }, clear=True), patch.object(llm, "GEMINI_MODEL", "test-model"), \
                    patch("requests.post") as post:
                post.return_value.status_code = 200
                post.return_value.json.return_value = response
                result = romanize.romanize_words(words)

                post.assert_called_once()
                self.assertEqual(post.call_args.args, (url,))
                kwargs = post.call_args.kwargs
                self.assertEqual(kwargs["timeout"], llm.TIMEOUT)
                self.assertEqual(kwargs["headers"][header], f"{provider}-key")
                prompt = source_prompt(json.loads(kwargs["data"]))
                self.assertEqual(json.loads(prompt.rsplit("\n", 1)[1]), ["अच्छा"])
                self.assertEqual(result, [
                    {**word, "hinglish": rendered}
                    for word, rendered in zip(words, ("API", "achchha", "मराठी", "main"))
                ])

    def test_no_key_uses_rules_without_http(self):
        with patch.dict(os.environ, {}, clear=True), patch("requests.post") as post:
            result = romanize.romanize_words([{"text": "मैं"}])

        post.assert_not_called()
        self.assertEqual(result, [{"text": "मैं", "hinglish": "main"}])

    def test_timeout_uses_rules(self):
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "fake-key"}, clear=True), \
                patch("requests.post", side_effect=requests.Timeout) as post:
            result = romanize.romanize_words([{"text": "मैं"}])

        post.assert_called_once()
        self.assertEqual(result, [{"text": "मैं", "hinglish": "main"}])

    def test_invalid_chunk_does_not_discard_a_successful_chunk(self):
        words = [{"text": "अच्छा"}] * romanize.CHUNK + [{"text": "मैं"}]
        reply = json.dumps(["achchha"] * romanize.CHUNK)
        with patch.object(llm, "complete", side_effect=[reply, "invalid JSON"]) as complete:
            result = romanize.romanize_words(words)

        self.assertEqual(complete.call_count, 2)
        self.assertEqual([word["hinglish"] for word in result],
                         ["achchha"] * romanize.CHUNK + ["main"])
