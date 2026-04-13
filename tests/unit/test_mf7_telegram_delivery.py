from __future__ import annotations

import unittest

from services.analytics_center.telegram_delivery import (
    build_telegram_operator_message,
    deliver_telegram_operator_surface,
)


class TestMf7TelegramDelivery(unittest.TestCase):
    def _surface(self) -> dict:
        return {
            "summaries": {"overview": "Analyzer summary", "active_recommendations": 2},
            "alerts": [{"title": "Risk"}],
            "channel_snapshots": [{"deep_link": "/v1/analytics/channels/demo"}],
            "release_video_snapshots": [{"deep_link": "/v1/analytics/releases/42"}],
            "recommendation_summaries": [{"summary": "Do X"}],
            "planning_summaries": [{"scenario": "WEEK"}],
            "linked_actions": [{"label": "Open", "path": "/v1/analytics/overview"}],
        }

    def test_build_operator_message_contains_key_sections(self) -> None:
        msg = build_telegram_operator_message(self._surface())
        self.assertIn("Analyzer Telegram Operator Surface", msg)
        self.assertIn("Overview:", msg)
        self.assertIn("Alerts:", msg)
        self.assertIn("Linked actions:", msg)

    def test_delivery_dry_run_and_live(self) -> None:
        dry = deliver_telegram_operator_surface(
            surface=self._surface(),
            bot_token="",
            chat_id=123,
            dry_run=True,
        )
        self.assertEqual(dry["delivery_mode"], "DRY_RUN")
        self.assertFalse(dry["delivered"])

        def _transport(**_: object) -> dict:
            return {"ok": True, "result": {"message_id": 777}}

        live = deliver_telegram_operator_surface(
            surface=self._surface(),
            bot_token="x",
            chat_id=123,
            dry_run=False,
            transport=_transport,
        )
        self.assertEqual(live["delivery_mode"], "LIVE")
        self.assertTrue(live["delivered"])

    def test_live_delivery_requires_credentials(self) -> None:
        with self.assertRaises(ValueError):
            deliver_telegram_operator_surface(surface=self._surface(), bot_token="", chat_id=123, dry_run=False)
        with self.assertRaises(ValueError):
            deliver_telegram_operator_surface(surface=self._surface(), bot_token="x", chat_id=0, dry_run=False)


if __name__ == "__main__":
    unittest.main()
