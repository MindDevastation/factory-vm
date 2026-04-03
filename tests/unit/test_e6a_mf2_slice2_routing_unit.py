from __future__ import annotations

import unittest

from services.telegram_inbox import (
    build_notification_dedupe_key,
    build_target_context_summary,
    classify_quiet_noisy,
    validate_classification,
)


class TestE6AMf2Slice2RoutingUnit(unittest.TestCase):
    def test_quiet_noisy_classifier(self) -> None:
        self.assertEqual(classify_quiet_noisy(message_family="CRITICAL_ALERT", severity="CRITICAL"), "IMMEDIATE")
        self.assertEqual(classify_quiet_noisy(message_family="SUMMARY_DIGEST", severity="INFORMATIONAL"), "DIGEST")
        self.assertEqual(classify_quiet_noisy(message_family="UNRESOLVED_FOLLOW_UP", severity="MEDIUM"), "FOLLOW_UP_ONLY")

    def test_dedupe_key_builder_deterministic(self) -> None:
        a = build_notification_dedupe_key(
            message_family="ACTIONABLE_ALERT",
            category="PUBLISH",
            target_entity_type="release",
            target_entity_ref="r1",
            upstream_event_family="publish/manual_handoff",
            upstream_event_ref="e1",
        )
        b = build_notification_dedupe_key(
            message_family="ACTIONABLE_ALERT",
            category="PUBLISH",
            target_entity_type="release",
            target_entity_ref="r1",
            upstream_event_family="publish/manual_handoff",
            upstream_event_ref="e1",
        )
        self.assertEqual(a, b)

    def test_target_context_summary(self) -> None:
        ctx = build_target_context_summary(target_entity_type="release", target_entity_ref="r2", attributes={"channel": "a", "nullable": None})
        self.assertEqual(ctx["summary"], "release:r2")
        self.assertEqual(ctx["attributes"], {"channel": "a"})

    def test_classification_validation(self) -> None:
        out = validate_classification(
            category="HEALTH",
            severity="CRITICAL",
            message_family="CRITICAL_ALERT",
            actionability_class="ESCALATE_ONLY",
            delivery_behavior="IMMEDIATE",
        )
        self.assertEqual(out["category"], "HEALTH")


if __name__ == "__main__":
    unittest.main()
