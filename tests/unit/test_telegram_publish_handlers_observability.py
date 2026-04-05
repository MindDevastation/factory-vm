from __future__ import annotations

import json
import unittest

from services.bot.handlers import _telegram_reply_payload


class TestTelegramPublishHandlersObservability(unittest.TestCase):
    def test_single_action_payload_contains_required_fields(self) -> None:
        raw = _telegram_reply_payload(
            action="retry",
            target={"job_id": 42},
            result="ok",
            request_id="tg-retry-42-1",
            error_code=None,
        )
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "retry")
        self.assertEqual(payload["target"]["job_id"], 42)
        self.assertEqual(payload["result"], "ok")
        self.assertEqual(payload["request_id"], "tg-retry-42-1")
        self.assertNotIn("error_code", payload)

    def test_bulk_action_payload_contains_required_fields_and_error_code(self) -> None:
        raw = _telegram_reply_payload(
            action="hold",
            target={"job_ids": [1, 2, 3], "count": 3},
            result="failed",
            request_id="tg-bulk-hold-1",
            error_code="E3_BULK_SCOPE_MISMATCH",
        )
        payload = json.loads(raw)
        self.assertEqual(payload["action"], "hold")
        self.assertEqual(payload["target"]["count"], 3)
        self.assertEqual(payload["result"], "failed")
        self.assertEqual(payload["request_id"], "tg-bulk-hold-1")
        self.assertEqual(payload["error_code"], "E3_BULK_SCOPE_MISMATCH")


if __name__ == "__main__":
    unittest.main()
