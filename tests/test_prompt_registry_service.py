from __future__ import annotations

import unittest

from services.common import db as dbm
from services.prompt_registry.registry_service import PromptRegistryService
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRegistryService(unittest.TestCase):
    def test_schema_smoke_tables_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                for table in ("prompt_records", "prompt_versions", "prompt_variables", "prompt_audit_events"):
                    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone()
                    self.assertIsNotNone(row)
            finally:
                conn.close()

    def test_record_version_activation_and_audit_foundation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                created = svc.create_record(
                    {
                        "slug": "core-template",
                        "code": "PR-001",
                        "title": "Core Template",
                        "record_type": "prompt_template",
                        "status": "draft",
                        "validation_status": "UNKNOWN",
                    }
                )
                prompt_id = int(created["id"])
                self.assertEqual(created["slug"], "core-template")

                updated = svc.update_record(prompt_id, {"status": "active", "title": "Core Template v2"})
                self.assertEqual(updated["status"], "active")

                v1 = svc.create_version(
                    prompt_id,
                    {
                        "body_text": "hello {{name}}",
                        "status": "draft",
                        "validation_status": "VALID",
                        "variables": [
                            {
                                "name": "name",
                                "safety_class": "operator_only",
                                "required": True,
                                "default_value": "",
                            }
                        ],
                    },
                )
                v2 = svc.create_version(prompt_id, {"body_text": "hello again", "status": "draft"})

                svc.activate_version(int(v1["id"]))
                active1 = svc.get_version(int(v1["id"]))
                self.assertEqual(int(active1["is_active"]), 1)
                svc.activate_version(int(v2["id"]))
                active2 = svc.get_version(int(v2["id"]))
                self.assertEqual(int(active2["is_active"]), 1)
                self.assertEqual(int(svc.get_version(int(v1["id"]))["is_active"]), 0)

                versions = svc.list_versions(prompt_id)
                self.assertEqual(len(versions), 2)
                audit = svc.list_audit_events(prompt_id)
                self.assertGreaterEqual(len(audit), 5)
                self.assertTrue(any(row["event_type"] == "version_activated" for row in audit))

                fetched = svc.get_version(int(v1["id"]))
                self.assertEqual(fetched["variables"][0]["safety_class"], "operator_only")
            finally:
                conn.close()

    def test_validation_and_duplicate_hardening(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                base = {
                    "slug": "dup-template",
                    "code": "PR-DUP-1",
                    "title": "Dup",
                    "record_type": "prompt_template",
                    "status": "draft",
                }
                first = svc.create_record(base)
                with self.assertRaisesRegex(Exception, "already exists"):
                    svc.create_record(base)

                with self.assertRaisesRegex(ValueError, "must be non-empty"):
                    svc.create_record({**base, "slug": " ", "code": "PR-DUP-2"})

                svc.update_record(int(first["id"]), {"status": "archived"})
                with self.assertRaisesRegex(ValueError, "invalid lifecycle transition"):
                    svc.update_record(int(first["id"]), {"status": "active"})

                with self.assertRaisesRegex(ValueError, "body_text must be non-empty"):
                    svc.create_version(int(first["id"]), {"body_text": " "})
            finally:
                conn.close()
