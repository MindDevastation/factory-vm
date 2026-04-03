from __future__ import annotations

import unittest

from services.telegram_inbox import assemble_digest, can_transition, require_transition


class TestE6AMf2Slice3LifecycleUnit(unittest.TestCase):
    def test_lifecycle_transition_validation(self) -> None:
        self.assertTrue(can_transition(from_state="ACTIVE", to_state="SUPERSEDED"))
        self.assertTrue(can_transition(from_state="INFORMATIONAL", to_state="RESOLVED"))
        self.assertFalse(can_transition(from_state="RESOLVED", to_state="ACTIVE"))
        self.assertEqual(require_transition(from_state="ACTIVE", to_state="RESOLVED"), "RESOLVED")
        with self.assertRaises(ValueError):
            require_transition(from_state="EXPIRED", to_state="ACTIVE")

    def test_digest_assembly_behavior(self) -> None:
        digest = assemble_digest(
            [
                {"id": 1, "category": "PUBLISH", "severity": "HIGH"},
                {"id": 2, "category": "PUBLISH", "severity": "HIGH"},
                {"id": 3, "category": "HEALTH", "severity": "CRITICAL"},
            ]
        )
        self.assertEqual(digest["total_items"], 3)
        self.assertEqual(digest["block_count"], 2)


if __name__ == "__main__":
    unittest.main()
