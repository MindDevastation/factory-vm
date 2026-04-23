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
                for table in ("prompt_records", "prompt_versions", "prompt_variables", "prompt_audit_events", "prompt_bindings"):
                    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone()
                    self.assertIsNotNone(row)
            finally:
                conn.close()

    def test_binding_create_list_update_and_resolution_order(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                records = [
                    svc.create_record(
                        {
                            "slug": "global-template",
                            "code": "PR-BIND-GLOBAL",
                            "title": "Global",
                            "record_type": "prompt_template",
                            "status": "draft",
                        },
                        actor="tester",
                    ),
                    svc.create_record(
                        {
                            "slug": "workflow-template",
                            "code": "PR-BIND-WORKFLOW",
                            "title": "Workflow",
                            "record_type": "prompt_template",
                            "status": "draft",
                        },
                        actor="tester",
                    ),
                    svc.create_record(
                        {
                            "slug": "channel-template",
                            "code": "PR-BIND-CHANNEL",
                            "title": "Channel",
                            "record_type": "prompt_template",
                            "status": "draft",
                        },
                        actor="tester",
                    ),
                    svc.create_record(
                        {
                            "slug": "item-template",
                            "code": "PR-BIND-ITEM",
                            "title": "Item",
                            "record_type": "prompt_template",
                            "status": "draft",
                        },
                        actor="tester",
                    ),
                ]
                global_binding = svc.create_binding(
                    {"prompt_id": int(records[0]["id"]), "binding_scope": "global", "binding_status": "active"},
                    actor="tester",
                )
                workflow_binding = svc.create_binding(
                    {
                        "prompt_id": int(records[1]["id"]),
                        "binding_scope": "workflow",
                        "workflow_slug": "wf-a",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                channel_binding = svc.create_binding(
                    {
                        "prompt_id": int(records[2]["id"]),
                        "binding_scope": "channel",
                        "channel_slug": "ch-a",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                item_binding = svc.create_binding(
                    {
                        "prompt_id": int(records[3]["id"]),
                        "binding_scope": "item",
                        "item_type": "release",
                        "item_ref": "r-001",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                listed = svc.list_bindings()
                self.assertGreaterEqual(len(listed), 4)
                self.assertEqual(int(global_binding["prompt_id"]), int(records[0]["id"]))

                result = svc.resolve_effective_prompt(
                    {"workflow_slug": "wf-a", "channel_slug": "ch-a", "item_type": "release", "item_ref": "r-001"}
                )
                self.assertEqual(result["resolution_status"], "matched")
                self.assertEqual(result["winner_binding"]["binding_scope"], "item")
                self.assertEqual(result["winner_prompt"]["slug"], "item-template")
                self.assertTrue(any(item["reason"].startswith("ignored: lower priority") for item in result["evaluated_candidates"]))

                deactivated_item = svc.update_binding_status(int(item_binding["id"]), {"binding_status": "inactive"}, actor="tester")
                self.assertEqual(deactivated_item["binding_status"], "inactive")
                channel_result = svc.resolve_effective_prompt(
                    {"workflow_slug": "wf-a", "channel_slug": "ch-a", "item_type": "release", "item_ref": "r-001"}
                )
                self.assertEqual(channel_result["winner_binding"]["binding_scope"], "channel")
                workflow_result = svc.resolve_effective_prompt({"workflow_slug": "wf-a"})
                self.assertEqual(workflow_result["winner_binding"]["binding_scope"], "workflow")
                miss_result = svc.resolve_effective_prompt({"workflow_slug": "wf-z"})
                self.assertEqual(miss_result["winner_binding"]["binding_scope"], "global")
                self.assertIn("resolution_order", miss_result)
                self.assertIn("evaluated_candidates", miss_result)

                global_only = svc.resolve_effective_prompt({})
                self.assertEqual(global_only["winner_binding"]["binding_scope"], "global")
                svc.update_binding_status(int(global_binding["id"]), {"binding_status": "inactive"}, actor="tester")
                miss_only = svc.resolve_effective_prompt({})
                self.assertEqual(miss_only["resolution_status"], "miss")
                self.assertIsNone(miss_only["winner_binding"])
                self.assertIsNone(miss_only["winner_prompt"])
            finally:
                conn.close()

    def test_binding_scope_validation_and_duplicate_hardening(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                record = svc.create_record(
                    {
                        "slug": "bind-validate-template",
                        "code": "PR-BIND-VALIDATE",
                        "title": "Binding Validate",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(record["id"])
                with self.assertRaisesRegex(Exception, "global binding_scope cannot include"):
                    svc.create_binding(
                        {"prompt_id": prompt_id, "binding_scope": "global", "channel_slug": "ch-a"},
                        actor="tester",
                    )
                with self.assertRaisesRegex(Exception, "workflow binding_scope requires workflow_slug"):
                    svc.create_binding({"prompt_id": prompt_id, "binding_scope": "workflow"}, actor="tester")
                with self.assertRaisesRegex(Exception, "channel binding_scope requires channel_slug"):
                    svc.create_binding({"prompt_id": prompt_id, "binding_scope": "channel"}, actor="tester")
                with self.assertRaisesRegex(Exception, "item binding_scope requires item_type and item_ref"):
                    svc.create_binding({"prompt_id": prompt_id, "binding_scope": "item", "item_type": "release"}, actor="tester")

                active_one = svc.create_binding(
                    {
                        "prompt_id": prompt_id,
                        "binding_scope": "channel",
                        "channel_slug": "ch-a",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                with self.assertRaisesRegex(Exception, "duplicate active binding"):
                    svc.create_binding(
                        {
                            "prompt_id": prompt_id,
                            "binding_scope": "channel",
                            "channel_slug": "ch-a",
                            "binding_status": "active",
                        },
                        actor="tester",
                    )
                svc.update_binding_status(int(active_one["id"]), {"binding_status": "inactive"}, actor="tester")
                second = svc.create_binding(
                    {
                        "prompt_id": prompt_id,
                        "binding_scope": "channel",
                        "channel_slug": "ch-a",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                self.assertEqual(second["binding_status"], "active")
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
