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
                    },
                    actor="tester",
                )
                prompt_id = int(created["id"])
                self.assertEqual(created["slug"], "core-template")

                with self.assertRaisesRegex(Exception, "active record requires active_version_id"):
                    svc.update_record(prompt_id, {"status": "active", "title": "Core Template v2"}, actor="tester")

                updated = svc.update_record(prompt_id, {"title": "Core Template v2"}, actor="tester")
                self.assertEqual(updated["status"], "draft")

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
                    actor="tester",
                )
                v2 = svc.create_version(prompt_id, {"body_text": "hello again", "status": "draft"}, actor="tester")

                svc.activate_version(int(v1["id"]), actor="tester")
                active1 = svc.get_version(int(v1["id"]))
                self.assertEqual(int(active1["is_active"]), 1)
                svc.activate_version(int(v2["id"]), actor="tester")
                active2 = svc.get_version(int(v2["id"]))
                self.assertEqual(int(active2["is_active"]), 1)
                self.assertEqual(int(svc.get_version(int(v1["id"]))["is_active"]), 0)
                record_after_activation = svc.get_record(prompt_id)
                self.assertEqual(record_after_activation["status"], "active")

                versions = svc.list_versions(prompt_id)
                self.assertEqual(len(versions), 2)
                self.assertTrue(all("render_fingerprint" in item for item in versions))
                audit = svc.list_audit_events(prompt_id)
                self.assertGreaterEqual(len(audit), 5)
                self.assertTrue(any(row["event_type"] == "version_activated" for row in audit))
                self.assertTrue(all(row["actor"] == "tester" for row in audit))

                fetched = svc.get_version(int(v1["id"]))
                self.assertEqual(fetched["variables"][0]["safety_class"], "operator_only")
                self.assertIsInstance(fetched["render_fingerprint"], str)
            finally:
                conn.close()

    def test_render_fingerprint_is_deterministic_for_identical_payloads(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                created = svc.create_record(
                    {
                        "slug": "fingerprint-template",
                        "code": "PR-FINGERPRINT-1",
                        "title": "Fingerprint Template",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(created["id"])
                payload = {
                    "body_text": "hello {{name}}",
                    "status": "draft",
                    "variables": [
                        {"name": "name", "safety_class": "operator_only", "required": True, "default_value": ""},
                    ],
                }
                v1 = svc.create_version(prompt_id, payload, actor="tester")
                v2 = svc.create_version(prompt_id, payload, actor="tester")
                self.assertEqual(v1["render_fingerprint"], v2["render_fingerprint"])
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
                first = svc.create_record(base, actor="tester")
                with self.assertRaisesRegex(Exception, "already exists"):
                    svc.create_record(base, actor="tester")

                with self.assertRaisesRegex(ValueError, "must be non-empty"):
                    svc.create_record({**base, "slug": " ", "code": "PR-DUP-2"}, actor="tester")

                svc.update_record(int(first["id"]), {"status": "archived"}, actor="tester")
                with self.assertRaisesRegex(ValueError, "invalid lifecycle transition"):
                    svc.update_record(int(first["id"]), {"status": "active"}, actor="tester")

                with self.assertRaisesRegex(ValueError, "body_text must be non-empty"):
                    svc.create_version(int(first["id"]), {"body_text": " "}, actor="tester")
            finally:
                conn.close()

    def test_create_version_atomic_on_invalid_variables_and_duplicates(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                created = svc.create_record(
                    {
                        "slug": "atomic-template",
                        "code": "PR-ATOMIC-1",
                        "title": "Atomic Template",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(created["id"])
                before_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )
                with self.assertRaisesRegex(Exception, "variable must be an object"):
                    svc.create_version(prompt_id, {"body_text": "x", "variables": ["not-an-object"]}, actor="tester")
                after_invalid_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )
                self.assertEqual(after_invalid_count, before_count)

                with self.assertRaisesRegex(Exception, "duplicate variable name"):
                    svc.create_version(
                        prompt_id,
                        {
                            "body_text": "x {{a}} {{a}}",
                            "variables": [
                                {"name": "a", "safety_class": "standard"},
                                {"name": "a", "safety_class": "standard"},
                            ],
                        },
                        actor="tester",
                    )
                after_dupe_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )
                self.assertEqual(after_dupe_count, before_count)

                with self.assertRaisesRegex(Exception, "active version cannot be created with is_active=0"):
                    svc.create_version(prompt_id, {"body_text": "x", "status": "active"}, actor="tester")
            finally:
                conn.close()
