from __future__ import annotations

import unittest

from services.common import db as dbm
from services.custom_tags import bulk_rules_service, catalog_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsBulkRulesPreview(unittest.TestCase):
    def test_preview_classifies_create_and_invalid(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                catalog_service.create_tag(
                    conn,
                    {"code": "cyber_arena", "label": "Cyber Arena", "category": "VISUAL", "description": None, "is_active": True},
                )
                result = bulk_rules_service.preview_bulk_rules(
                    conn,
                    [
                        {
                            "tag_code": "cyber_arena",
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "false",
                            "priority": 100,
                            "weight": 1.0,
                            "required": True,
                            "stop_after_match": False,
                            "is_active": True,
                            "match_mode": "ALL",
                        },
                        {
                            "tag_code": "cyber_arena",
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "not-json",
                            "priority": 100,
                            "weight": 1.0,
                            "required": True,
                            "stop_after_match": False,
                            "is_active": True,
                            "match_mode": "ALL",
                        },
                    ],
                )
            finally:
                conn.close()

        self.assertFalse(result["can_confirm"])
        self.assertEqual(result["summary"], {"total": 2, "create": 1, "invalid": 1})
        self.assertEqual(result["items"][0]["action"], "CREATE")
        self.assertIn("active:", result["items"][0]["summary"])
        self.assertEqual(result["items"][1]["action"], "INVALID")
        self.assertEqual(result["items"][1]["errors"][0]["code"], "CTA_INVALID_INPUT")

    def test_preview_duplicate_conflict_and_exact_duplicate_are_invalid(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                catalog_service.create_tag(
                    conn,
                    {"code": "cyber_arena", "label": "Cyber Arena", "category": "VISUAL", "description": None, "is_active": True},
                )
                result = bulk_rules_service.preview_bulk_rules(
                    conn,
                    [
                        {
                            "tag_code": "cyber_arena",
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "false",
                            "priority": 100,
                            "weight": 1.0,
                            "required": True,
                            "stop_after_match": False,
                            "is_active": True,
                            "match_mode": "ALL",
                        },
                        {
                            "tag_code": "cyber_arena",
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "false",
                            "priority": 101,
                            "weight": 1.0,
                            "required": True,
                            "stop_after_match": False,
                            "is_active": True,
                            "match_mode": "ALL",
                        },
                        {
                            "tag_code": "cyber_arena",
                            "source_path": "track_features.payload_json.voice_flag",
                            "operator": "equals",
                            "value_json": "false",
                            "priority": 100,
                            "weight": 1.0,
                            "required": True,
                            "stop_after_match": False,
                            "is_active": True,
                            "match_mode": "ALL",
                        },
                    ],
                )
            finally:
                conn.close()

        self.assertFalse(result["can_confirm"])
        self.assertEqual(result["summary"], {"total": 3, "create": 1, "invalid": 2})
        self.assertEqual(result["items"][1]["errors"][0]["code"], "CTA_DUPLICATE_RULE_CONFLICT")
        self.assertEqual(result["items"][2]["errors"][0]["code"], "CTA_DUPLICATE_RULE")


if __name__ == "__main__":
    unittest.main()
