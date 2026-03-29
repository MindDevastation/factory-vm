from __future__ import annotations

import unittest

from services.publish_runtime.publish_failure_classifier import classify_publish_failure


class _HttpExc(Exception):
    def __init__(self, status_code: int):
        super().__init__(f"http {status_code}")
        self.status_code = status_code


class TestPublishFailureClassifier(unittest.TestCase):
    def test_timeout_is_retriable(self) -> None:
        self.assertEqual(classify_publish_failure(TimeoutError("slow")), ("timeout", "retriable"))

    def test_rate_limit_is_retriable(self) -> None:
        self.assertEqual(classify_publish_failure(_HttpExc(429)), ("rate_limited", "retriable"))

    def test_transient_http_is_retriable(self) -> None:
        self.assertEqual(classify_publish_failure(_HttpExc(503)), ("transient_api_error", "retriable"))

    def test_invalid_configuration_is_terminal(self) -> None:
        self.assertEqual(classify_publish_failure(_HttpExc(401)), ("invalid_configuration", "terminal"))

    def test_rejection_is_terminal(self) -> None:
        self.assertEqual(classify_publish_failure(_HttpExc(409)), ("terminal_publish_rejection", "terminal"))

    def test_default_unknown_is_retriable(self) -> None:
        self.assertEqual(classify_publish_failure(RuntimeError("weird error")), ("unknown_external_error", "retriable"))


if __name__ == "__main__":
    unittest.main()
