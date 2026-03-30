from __future__ import annotations

import unittest

from services.bot.telegram_publish_formatting import (
    format_critical_event_message,
    format_next_actions,
    format_publish_reason_label,
    format_publish_state_label,
)


class TestTelegramPublishFormatting(unittest.TestCase):
    def test_known_state_reason_and_actions(self) -> None:
        self.assertEqual(format_publish_state_label("policy_blocked"), "Policy blocked")
        self.assertEqual(format_publish_reason_label("retries_exhausted"), "Retries exhausted")
        self.assertEqual(format_next_actions("manual_handoff_pending"), "acknowledge, mark-completed")

    def test_critical_message_shape(self) -> None:
        msg = format_critical_event_message(
            family="publish failed",
            item={"job_id": 55, "publish_state": "publish_failed_terminal", "publish_reason_code": "terminal_publish_rejection"},
        )
        self.assertIn("publish failed", msg)
        self.assertIn("job_id=55", msg)
        self.assertIn("Publish failed", msg)
        self.assertIn("Terminal publish rejection", msg)


if __name__ == "__main__":
    unittest.main()
