import unittest

from captions import llm


class LLMRetryClassificationTests(unittest.TestCase):
    def test_transient_statuses_are_retryable(self):
        for status in (408, 429, 500, 502, 503, 599):
            with self.subTest(status=status):
                self.assertTrue(llm._retryable_status(status))

    def test_auth_and_client_errors_are_not_retryable(self):
        for status in (400, 401, 403, 404, 422):
            with self.subTest(status=status):
                self.assertFalse(llm._retryable_status(status))

    def test_llm_error_preserves_retryability(self):
        self.assertFalse(llm.LLMError("bad key").retryable)
        self.assertTrue(llm.LLMError("temporarily unavailable", retryable=True).retryable)


if __name__ == "__main__":
    unittest.main()
