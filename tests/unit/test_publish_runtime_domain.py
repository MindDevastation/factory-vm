from __future__ import annotations

import unittest

from services.publish_runtime.domain import (
    PUBLISH_DELIVERY_MODE_EFFECTIVE_VALUES,
    PUBLISH_RESOLVED_SCOPE_VALUES,
    PUBLISH_STATE_VALUES,
    PUBLISH_TARGET_VISIBILITY_VALUES,
    normalize_publish_delivery_mode_effective,
    normalize_publish_resolved_scope,
    normalize_publish_state,
    normalize_publish_target_visibility,
    validate_publish_delivery_mode_effective,
    validate_publish_resolved_scope,
    validate_publish_state,
    validate_publish_target_visibility,
)


class TestPublishRuntimeDomain(unittest.TestCase):
    def test_canonical_literal_sets_match_spec(self) -> None:
        self.assertEqual(
            PUBLISH_STATE_VALUES,
            (
                "private_uploaded",
                "policy_blocked",
                "waiting_for_schedule",
                "ready_to_publish",
                "publish_in_progress",
                "retry_pending",
                "manual_handoff_pending",
                "manual_handoff_acknowledged",
                "manual_publish_completed",
                "published_public",
                "published_unlisted",
                "publish_failed_terminal",
                "publish_state_drift_detected",
            ),
        )
        self.assertEqual(PUBLISH_TARGET_VISIBILITY_VALUES, ("public", "unlisted"))
        self.assertEqual(PUBLISH_DELIVERY_MODE_EFFECTIVE_VALUES, ("automatic", "manual"))
        self.assertEqual(PUBLISH_RESOLVED_SCOPE_VALUES, ("project", "channel", "item"))

    def test_normalize_helpers_lowercase_and_strip(self) -> None:
        self.assertEqual(normalize_publish_state("  RETRY_PENDING  "), "retry_pending")
        self.assertEqual(normalize_publish_target_visibility("  UNLISTED "), "unlisted")
        self.assertEqual(normalize_publish_delivery_mode_effective(" MANUAL "), "manual")
        self.assertEqual(normalize_publish_resolved_scope(" ITEM "), "item")

    def test_validation_helpers_accept_valid_literals(self) -> None:
        self.assertEqual(validate_publish_state("retry_pending"), "retry_pending")
        self.assertEqual(validate_publish_target_visibility("public"), "public")
        self.assertEqual(validate_publish_delivery_mode_effective("automatic"), "automatic")
        self.assertEqual(validate_publish_resolved_scope("project"), "project")

    def test_validation_helpers_reject_invalid_literals(self) -> None:
        with self.assertRaises(ValueError):
            validate_publish_state("queued")
        with self.assertRaises(ValueError):
            validate_publish_target_visibility("private")
        with self.assertRaises(ValueError):
            validate_publish_delivery_mode_effective("hybrid")
        with self.assertRaises(ValueError):
            validate_publish_resolved_scope("release")


if __name__ == "__main__":
    unittest.main()
