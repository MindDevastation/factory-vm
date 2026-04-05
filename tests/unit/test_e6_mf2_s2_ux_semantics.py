from __future__ import annotations

import unittest

from services.factory_api.ux_semantics import (
    action_bar_semantics,
    filter_control_semantics,
    inline_message_semantics,
    readiness_indicator_semantics,
    severity_indicator_semantics,
    status_badge_semantics,
    table_list_semantics,
)


class TestE6Mf2S2UxSemantics(unittest.TestCase):
    def test_status_badge_maps_tones(self) -> None:
        self.assertEqual(status_badge_semantics(status="FAILED")["tone"], "danger")
        self.assertEqual(status_badge_semantics(status="PUBLISHED")["tone"], "success")

    def test_severity_indicator_maps_priority(self) -> None:
        self.assertEqual(severity_indicator_semantics(severity="CRITICAL")["priority_rank"], "P0")
        self.assertEqual(severity_indicator_semantics(severity="LOW")["priority_rank"], "P3")

    def test_readiness_indicator_maps_tones(self) -> None:
        self.assertEqual(readiness_indicator_semantics(readiness="STALE")["tone"], "stale")
        self.assertEqual(readiness_indicator_semantics(readiness="BLOCKED")["tone"], "danger")

    def test_message_action_filter_table_contracts(self) -> None:
        self.assertEqual(inline_message_semantics(level="warning", text="x")["level"], "WARNING")
        self.assertEqual(action_bar_semantics(actions=[{"action": "refresh"}])["count"], 1)
        self.assertEqual(filter_control_semantics(filters=["status"])["filters"], ["status"])
        self.assertEqual(table_list_semantics(variant="list")["variant"], "LIST")


if __name__ == "__main__":
    unittest.main()
