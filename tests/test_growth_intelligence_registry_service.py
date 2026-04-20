from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.growth_intelligence.registry_service import GrowthRegistryService
from tests._helpers import seed_minimal_db, temp_env


class TestGrowthIntelligenceRegistryService(unittest.TestCase):
    def test_schema_tables_and_constraints_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                tables = {
                    "growth_knowledge_items",
                    "growth_playbooks",
                    "growth_channel_feature_flags",
                    "growth_bootstrap_import_runs",
                    "growth_bootstrap_import_run_items",
                }
                for table in tables:
                    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone()
                    self.assertIsNotNone(row)

                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO growth_knowledge_items(
                            code,title,description,source_type,source_name,source_trust,impact_confidence,
                            applicable_profiles_json,applicable_metrics_json,trigger_conditions_json,
                            action_template,explanation_template,status,source_url,source_class,evidence_note,
                            created_at,updated_at
                        ) VALUES('bad','Bad','d','t','n','Z','High','[]','[]','[]','a','e','ACTIVE','','OFFICIAL','', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                        """
                    )
            finally:
                conn.close()

    def test_bootstrap_import_create_update_repeatable_and_audit(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = GrowthRegistryService(conn)
                payload = {
                    "import_source": "curated",
                    "import_mode": "upsert",
                    "actor": "tester",
                    "items": [
                        {
                            "code": "GI-001",
                            "title": "Item",
                            "description": "desc",
                            "source_type": "doc",
                            "source_name": "internal",
                            "source_trust": "B",
                            "impact_confidence": "Medium",
                            "source_class": "INTERNAL",
                            "action_template": "open",
                            "explanation_template": "because",
                            "status": "ACTIVE",
                        }
                    ],
                }
                first = svc.bootstrap_import(payload)
                self.assertEqual(first["created"], 1)
                self.assertEqual(first["failed"], 0)

                payload["items"][0]["description"] = "desc-updated"
                second = svc.bootstrap_import(payload)
                self.assertEqual(second["updated"], 1)

                third = svc.bootstrap_import(payload)
                self.assertEqual(third["skipped"], 1)

                count = conn.execute("SELECT COUNT(*) AS c FROM growth_knowledge_items WHERE code = 'GI-001'").fetchone()["c"]
                self.assertEqual(int(count), 1)

                runs = conn.execute("SELECT COUNT(*) AS c FROM growth_bootstrap_import_runs").fetchone()["c"]
                self.assertEqual(int(runs), 3)
            finally:
                conn.close()

    def test_service_hardening_duplicate_and_validation_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = GrowthRegistryService(conn)
                item_payload = {
                    "code": "GI-SVC-DUPE-1",
                    "title": "Item",
                    "description": "d",
                    "source_type": "doc",
                    "source_name": "seed",
                    "source_trust": "B",
                    "impact_confidence": "Medium",
                    "source_class": "INTERNAL",
                    "action_template": "a",
                    "explanation_template": "e",
                    "status": "ACTIVE",
                }
                svc.create_knowledge_item(item_payload)
                with self.assertRaisesRegex(ValueError, "already exists"):
                    svc.create_knowledge_item(item_payload)

                with self.assertRaisesRegex(ValueError, "code must be non-empty"):
                    svc.create_knowledge_item({**item_payload, "code": " "})

                playbook_payload = {
                    "code": "PB-SVC-1",
                    "goal_type": "RETENTION",
                    "channel_types_json": ["LONG"],
                    "release_types_json": ["VIDEO"],
                    "activation_rules_json": {"min_items": 1},
                    "output_shape_json": {"kind": "plan"},
                    "trust_policy_json": {"min_trust": "B"},
                }
                svc.create_playbook(playbook_payload)
                with self.assertRaisesRegex(ValueError, "already exists"):
                    svc.create_playbook(playbook_payload)
                with self.assertRaisesRegex(ValueError, "goal_type must be non-empty"):
                    svc.create_playbook(
                        {
                            "code": "PB-SVC-2",
                            "goal_type": " ",
                            "channel_types_json": ["LONG"],
                            "release_types_json": ["VIDEO"],
                            "activation_rules_json": {"min_items": 1},
                            "output_shape_json": {"kind": "plan"},
                            "trust_policy_json": {"min_trust": "B"},
                        }
                    )

                with self.assertRaisesRegex(ValueError, "not found"):
                    svc.set_channel_feature_flags(
                        "missing-channel",
                        {
                            "growth_intelligence_enabled": True,
                            "planning_digest_enabled": False,
                            "planner_handoff_enabled": False,
                            "export_enabled": False,
                            "assisted_planning_enabled": False,
                        },
                    )
            finally:
                conn.close()

    def test_service_patch_playbook_and_provenance_validations(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = GrowthRegistryService(conn)
                base_item = {
                    "code": "GI-SVC-VAL-1",
                    "title": "Item",
                    "description": "d",
                    "source_type": "doc",
                    "source_name": "seed",
                    "source_trust": "B",
                    "impact_confidence": "Medium",
                    "source_class": "INTERNAL",
                    "action_template": "a",
                    "explanation_template": "e",
                    "status": "ACTIVE",
                }
                created = svc.create_knowledge_item(base_item)
                with self.assertRaisesRegex(ValueError, "supersedes_item_id 999999 not found"):
                    svc.update_knowledge_item(int(created["id"]), {"supersedes_item_id": 999999})

                required_fields = (
                    "channel_types_json",
                    "release_types_json",
                    "activation_rules_json",
                    "output_shape_json",
                    "trust_policy_json",
                )
                for field in required_fields:
                    payload = {
                        "code": f"PB-SVC-{field}",
                        "goal_type": "RETENTION",
                        "channel_types_json": ["LONG"],
                        "release_types_json": ["VIDEO"],
                        "activation_rules_json": {"min_items": 1},
                        "output_shape_json": {"kind": "plan"},
                        "trust_policy_json": {"min_trust": "B"},
                    }
                    payload.pop(field)
                    with self.assertRaisesRegex(ValueError, f"{field} is required"):
                        svc.create_playbook(payload)

                with self.assertRaisesRegex(ValueError, "channel_types_json must be a JSON array"):
                    svc.create_playbook(
                        {
                            "code": "PB-SVC-BAD-ARR",
                            "goal_type": "RETENTION",
                            "channel_types_json": {"bad": True},
                            "release_types_json": ["VIDEO"],
                            "activation_rules_json": {"min_items": 1},
                            "output_shape_json": {"kind": "plan"},
                            "trust_policy_json": {"min_trust": "B"},
                        }
                    )
                with self.assertRaisesRegex(ValueError, "activation_rules_json must be a JSON object"):
                    svc.create_playbook(
                        {
                            "code": "PB-SVC-BAD-OBJ",
                            "goal_type": "RETENTION",
                            "channel_types_json": ["LONG"],
                            "release_types_json": ["VIDEO"],
                            "activation_rules_json": ["bad"],
                            "output_shape_json": {"kind": "plan"},
                            "trust_policy_json": {"min_trust": "B"},
                        }
                    )

                official = {**base_item, "code": "GI-OFFICIAL-BAD", "source_class": "OFFICIAL", "source_trust": "A", "source_url": " ", "evidence_note": "ok"}
                with self.assertRaisesRegex(ValueError, "source_url must be non-empty"):
                    svc.create_knowledge_item(official)

                practitioner = {
                    **base_item,
                    "code": "GI-PRACT-BAD",
                    "source_class": "PRACTITIONER",
                    "source_trust": "B",
                    "source_url": "https://example.com",
                    "evidence_note": " ",
                }
                with self.assertRaisesRegex(ValueError, "evidence_note must be non-empty"):
                    svc.create_knowledge_item(practitioner)

                internal = {**base_item, "code": "GI-INTERNAL-OK", "source_url": " ", "evidence_note": " "}
                created_internal = svc.create_knowledge_item(internal)
                self.assertEqual(created_internal["source_class"], "INTERNAL")
            finally:
                conn.close()

    def test_bootstrap_import_records_failed_item_for_invalid_official_provenance(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = GrowthRegistryService(conn)
                result = svc.bootstrap_import(
                    {
                        "import_source": "curated",
                        "import_mode": "upsert",
                        "actor": "tester",
                        "items": [
                            {
                                "code": "GI-BOOT-BAD-PROV",
                                "title": "Bad Official",
                                "description": "d",
                                "source_type": "doc",
                                "source_name": "official-doc",
                                "source_trust": "A",
                                "impact_confidence": "Medium",
                                "source_class": "OFFICIAL",
                                "source_url": " ",
                                "evidence_note": "ok",
                                "action_template": "a",
                                "explanation_template": "e",
                                "status": "ACTIVE",
                            }
                        ],
                    }
                )
                self.assertEqual(result["failed"], 1)
                self.assertEqual(result["created"], 0)
                self.assertEqual(result["items"][0]["result_status"], "FAILED")

                run_row = conn.execute(
                    "SELECT failed_count, created_count FROM growth_bootstrap_import_runs WHERE id = ?",
                    (result["run_id"],),
                ).fetchone()
                self.assertEqual(int(run_row["failed_count"]), 1)
                self.assertEqual(int(run_row["created_count"]), 0)
            finally:
                conn.close()

    def test_get_channel_feature_flags_missing_channel_vs_existing_without_row(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = GrowthRegistryService(conn)

                with self.assertRaisesRegex(ValueError, "channel missing-channel not found"):
                    svc.get_channel_feature_flags("missing-channel")

                flags = svc.get_channel_feature_flags("darkwood-reverie")
                self.assertEqual(flags["channel_slug"], "darkwood-reverie")
                self.assertEqual(int(flags["growth_intelligence_enabled"]), 0)
                self.assertEqual(int(flags["planning_digest_enabled"]), 0)
                self.assertEqual(int(flags["planner_handoff_enabled"]), 0)
                self.assertEqual(int(flags["export_enabled"]), 0)
                self.assertEqual(int(flags["assisted_planning_enabled"]), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
