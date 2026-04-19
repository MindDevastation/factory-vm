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

                playbook_payload = {"code": "PB-SVC-1", "goal_type": "RETENTION"}
                svc.create_playbook(playbook_payload)
                with self.assertRaisesRegex(ValueError, "already exists"):
                    svc.create_playbook(playbook_payload)
                with self.assertRaisesRegex(ValueError, "goal_type must be non-empty"):
                    svc.create_playbook({"code": "PB-SVC-2", "goal_type": " "})

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


if __name__ == "__main__":
    unittest.main()
