from __future__ import annotations

import unittest

from services.publish_runtime.snapshot import build_publish_runtime_snapshot


class TestPublishRuntimeSnapshot(unittest.TestCase):
    def test_build_snapshot_extracts_canonical_fields(self) -> None:
        row = {
            "id": 9,
            "state": "WAIT_APPROVAL",
            "publish_state": "ready_to_publish",
            "publish_target_visibility": "public",
            "publish_delivery_mode_effective": "automatic",
            "publish_resolved_scope": "channel",
            "publish_reason_code": "x",
            "publish_reason_detail": "details",
            "publish_attempt_count": 2,
            "publish_retry_at": 123.5,
            "publish_last_transition_at": 222.5,
            "publish_hold_active": 1,
        }

        snapshot = build_publish_runtime_snapshot(row)

        self.assertEqual(snapshot.job_id, 9)
        self.assertEqual(snapshot.job_state, "WAIT_APPROVAL")
        self.assertEqual(snapshot.publish_state, "ready_to_publish")
        self.assertEqual(snapshot.publish_target_visibility, "public")
        self.assertEqual(snapshot.publish_delivery_mode_effective, "automatic")
        self.assertEqual(snapshot.publish_resolved_scope, "channel")
        self.assertEqual(snapshot.publish_reason_code, "x")
        self.assertEqual(snapshot.publish_reason_detail, "details")
        self.assertEqual(snapshot.publish_attempt_count, 2)
        self.assertEqual(snapshot.publish_retry_at, 123.5)
        self.assertEqual(snapshot.publish_last_transition_at, 222.5)
        self.assertTrue(snapshot.publish_hold_active)

    def test_build_snapshot_defaults_optional_values(self) -> None:
        snapshot = build_publish_runtime_snapshot({"id": 1, "state": "READY_FOR_RENDER"})

        self.assertEqual(snapshot.publish_attempt_count, 0)
        self.assertIsNone(snapshot.publish_state)
        self.assertIsNone(snapshot.publish_retry_at)
        self.assertFalse(snapshot.publish_hold_active)


if __name__ == "__main__":
    unittest.main()
