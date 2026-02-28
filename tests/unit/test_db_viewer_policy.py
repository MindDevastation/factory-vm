from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from services.db_viewer.policy import (
    DbViewerPolicyError,
    is_privileged,
    load_policy,
    parse_privileged_users,
    save_policy,
    validate_policy_payload,
)


class TestDbViewerPolicy(unittest.TestCase):
    def test_empty_path_read_and_write_behavior(self):
        env = SimpleNamespace(db_viewer_policy_path="", db_viewer_privileged_users="")

        self.assertEqual(
            load_policy(env),
            {"denylist_tables": [], "human_name_overrides": {}},
        )

        with self.assertRaises(DbViewerPolicyError) as ctx:
            save_policy(env, {"denylist_tables": [], "human_name_overrides": {}})
        self.assertEqual(str(ctx.exception), "Policy storage is not configured")

    def test_validation_rejects_duplicates_invalid_identifiers_and_empty_override(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_policy_payload({"denylist_tables": ["channels", "channels"]})

        with self.assertRaisesRegex(ValueError, "invalid identifier"):
            validate_policy_payload({"denylist_tables": ["1bad"]})

        with self.assertRaisesRegex(ValueError, "invalid identifier"):
            validate_policy_payload({"human_name_overrides": {"bad-name": "X"}})

        with self.assertRaisesRegex(ValueError, "non-empty string"):
            validate_policy_payload({"human_name_overrides": {"channels": "  "}})

    def test_atomic_save_and_load_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            policy_path = Path(td) / "nested" / "policy.json"
            env = SimpleNamespace(db_viewer_policy_path=str(policy_path), db_viewer_privileged_users="")
            payload = {
                "denylist_tables": ["channels", "jobs"],
                "human_name_overrides": {"channels": "Channels"},
            }

            saved = save_policy(env, payload)
            self.assertEqual(saved, payload)
            self.assertEqual(load_policy(env), payload)

            on_disk = json.loads(policy_path.read_text(encoding="utf-8"))
            self.assertEqual(on_disk, payload)
            tmp_leftovers = list(policy_path.parent.glob("*.tmp"))
            self.assertEqual(tmp_leftovers, [])

    def test_privileged_users_parsing(self):
        self.assertEqual(parse_privileged_users(""), set())
        self.assertEqual(parse_privileged_users("alice, bob,,carol "), {"alice", "bob", "carol"})

        env = SimpleNamespace(db_viewer_policy_path="", db_viewer_privileged_users="alice,bob")
        self.assertTrue(is_privileged("alice", env))
        self.assertFalse(is_privileged("carol", env))


if __name__ == "__main__":
    unittest.main()
