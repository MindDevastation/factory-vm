from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from services.common import db as dbm
from services.prompt_registry.registry_service import PromptRegistryService
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRegistryService(unittest.TestCase):
    def test_schema_smoke_tables_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                for table in (
                    "prompt_records",
                    "prompt_versions",
                    "prompt_variables",
                    "prompt_audit_events",
                    "prompt_bindings",
                    "prompt_linked_actions",
                    "prompt_linked_action_execution_requests",
                    "prompt_usage_events",
                ):
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
                self.assertTrue(all("reason_code" in item for item in result["evaluated_candidates"]))
                self.assertEqual(
                    [item["evaluated_order"] for item in result["evaluated_candidates"]],
                    list(range(1, len(result["evaluated_candidates"]) + 1)),
                )

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

    def test_binding_list_filters_and_resolve_tie_break_diagnostics(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec_a = svc.create_record(
                    {
                        "slug": "scope-a",
                        "code": "PR-SCOPE-A",
                        "title": "Scope A",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                rec_b = svc.create_record(
                    {
                        "slug": "scope-b",
                        "code": "PR-SCOPE-B",
                        "title": "Scope B",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                first = svc.create_binding(
                    {
                        "prompt_id": int(rec_a["id"]),
                        "binding_scope": "channel",
                        "channel_slug": "ch-tie",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                second = svc.create_binding(
                    {
                        "prompt_id": int(rec_b["id"]),
                        "binding_scope": "channel",
                        "channel_slug": "ch-tie",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                inactive = svc.create_binding(
                    {
                        "prompt_id": int(rec_a["id"]),
                        "binding_scope": "workflow",
                        "workflow_slug": "wf-z",
                        "binding_status": "inactive",
                    },
                    actor="tester",
                )

                by_scope = svc.list_bindings(binding_scope="channel")
                self.assertTrue(all(item["binding_scope"] == "channel" for item in by_scope))
                by_status = svc.list_bindings(binding_status="inactive")
                self.assertEqual({int(item["id"]) for item in by_status}, {int(inactive["id"])})
                composed = svc.list_bindings(prompt_id=int(rec_a["id"]), binding_scope="channel", binding_status="active")
                self.assertEqual({int(item["id"]) for item in composed}, {int(first["id"])})

                resolved = svc.resolve_effective_prompt({"channel_slug": "ch-tie"})
                self.assertEqual(resolved["winner_binding"]["binding_scope"], "channel")
                self.assertEqual(int(resolved["winner_binding"]["binding_id"]), int(second["id"]))
                loser = [item for item in resolved["evaluated_candidates"] if int(item["binding_id"]) == int(first["id"])][0]
                self.assertEqual(loser["reason_code"], "IGNORED_SAME_SCOPE_OLDER_BINDING")
                self.assertIn("tie_break_note", loser)

                with self.assertRaisesRegex(Exception, "item_type and item_ref must be provided together"):
                    svc.resolve_effective_prompt({"item_type": "release"})
                with self.assertRaisesRegex(Exception, "item_type and item_ref must be provided together"):
                    svc.resolve_effective_prompt({"item_ref": "rel-10"})
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

    def test_export_import_foundation_roundtrip_and_privacy(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                record = svc.create_record(
                    {
                        "slug": "mf5-export",
                        "code": "PR-MF5-EXP",
                        "title": "MF5 Export",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(record["id"])
                version = svc.create_version(
                    prompt_id,
                    {
                        "body_text": "Hi {{name}}",
                        "status": "draft",
                        "variables": [
                            {"name": "name", "safety_class": "standard", "required": True, "default_value": "Guest"},
                        ],
                    },
                    actor="tester",
                )
                svc.create_binding(
                    {
                        "prompt_id": prompt_id,
                        "binding_scope": "workflow",
                        "workflow_slug": "wf-mf5",
                        "binding_status": "active",
                    },
                    actor="tester",
                )
                svc.preview_version(int(version["id"]), {"variables": {"name": "Alice"}})

                exported = svc.export_registry(prompt_id=prompt_id)
                self.assertEqual(exported["schema_version"], "prompt_registry_export_v1")
                self.assertIn("records", exported)
                self.assertIn("versions", exported)
                self.assertIn("variables", exported)
                self.assertIn("bindings", exported)
                self.assertNotIn("usage_events_summary", exported)
                self.assertEqual([item["slug"] for item in exported["records"]], ["mf5-export"])
                self.assertEqual(exported["versions"][0]["prompt_slug"], "mf5-export")
                self.assertEqual(exported["variables"][0]["default_value"], "Guest")
                self.assertNotIn("used_variables", str(exported))
                self.assertNotIn("diagnostics", str(exported))

                preview = svc.preview_import(exported, mode="merge_only")
                self.assertEqual(preview["import_status"], "OK")
                before_records = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"])
                dry_run = svc.confirm_import(exported, mode="merge_only", dry_run=True, actor="tester")
                self.assertEqual(dry_run["import_status"], "OK")
                after_dry_run_records = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"])
                self.assertEqual(before_records, after_dry_run_records)

                with temp_env() as (_td2, env2):
                    seed_minimal_db(env2)
                    conn2 = dbm.connect(env2)
                    try:
                        svc2 = PromptRegistryService(conn2)
                        applied = svc2.confirm_import(exported, mode="merge_only", dry_run=False, actor="tester")
                        self.assertEqual(applied["import_status"], "OK")
                        self.assertEqual(int(conn2.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"]), 1)
                        self.assertEqual(int(conn2.execute("SELECT COUNT(*) AS c FROM prompt_versions").fetchone()["c"]), 1)
                        self.assertEqual(int(conn2.execute("SELECT COUNT(*) AS c FROM prompt_bindings").fetchone()["c"]), 1)
                    finally:
                        conn2.close()
            finally:
                conn.close()

    def test_export_redacts_default_value_for_secret_and_operator_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                record = svc.create_record(
                    {
                        "slug": "mf5-export-redact",
                        "code": "PR-MF5-RED",
                        "title": "MF5 Redact",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                svc.create_version(
                    int(record["id"]),
                    {
                        "body_text": "Hi {{role}} {{token}} {{name}}",
                        "status": "draft",
                        "variables": [
                            {"name": "role", "safety_class": "operator_only", "required": False, "default_value": "operator"},
                            {"name": "token", "safety_class": "secret", "required": False, "default_value": "s3cr3t"},
                            {"name": "name", "safety_class": "standard", "required": False, "default_value": "Guest"},
                        ],
                    },
                    actor="tester",
                )

                exported = svc.export_registry(prompt_id=int(record["id"]))
                by_name = {str(item["name"]): item for item in exported["variables"]}
                self.assertEqual(by_name["role"]["default_value"], "")
                self.assertEqual(by_name["token"]["default_value"], "")
                self.assertEqual(by_name["name"]["default_value"], "Guest")
            finally:
                conn.close()

    def test_import_preview_rejects_invalid_schema_and_duplicates(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                invalid_schema = {
                    "schema_version": "bad_schema",
                    "records": [],
                    "versions": [],
                    "variables": [],
                    "bindings": [],
                }
                with self.assertRaisesRegex(Exception, "invalid schema_version"):
                    svc.preview_import(invalid_schema, mode="merge_only")

                duplicate_records = {
                    "schema_version": "prompt_registry_export_v1",
                    "records": [
                        {
                            "slug": "dup",
                            "code": "PR-DUP-A",
                            "title": "A",
                            "record_type": "prompt_template",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        },
                        {
                            "slug": "dup",
                            "code": "PR-DUP-B",
                            "title": "B",
                            "record_type": "prompt_template",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        },
                    ],
                    "versions": [],
                    "variables": [],
                    "bindings": [],
                }
                preview = svc.preview_import(duplicate_records, mode="merge_only")
                self.assertEqual(preview["import_status"], "INVALID")
                self.assertTrue(any("duplicate record slug" in item for item in preview["summary"]["validation_errors"]))
            finally:
                conn.close()

    def test_confirm_import_atomic_rollback_and_non_destructive(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                existing = svc.create_record(
                    {
                        "slug": "existing-keep",
                        "code": "PR-KEEP-1",
                        "title": "Keep",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                payload = {
                    "schema_version": "prompt_registry_export_v1",
                    "records": [
                        {
                            "slug": "existing-keep",
                            "code": "PR-KEEP-1",
                            "title": "Keep Updated",
                            "record_type": "prompt_template",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        },
                        {
                            "slug": "new-record",
                            "code": "PR-NEW-1",
                            "title": "New",
                            "record_type": "prompt_template",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        },
                    ],
                    "versions": [
                        {
                            "prompt_slug": "new-record",
                            "version_number": 1,
                            "body_text": "Body {{name}}",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        }
                    ],
                    "variables": [
                        {
                            "prompt_slug": "new-record",
                            "version_number": 1,
                            "name": "name",
                            "safety_class": "standard",
                            "required": True,
                            "default_value": "",
                            "description": "",
                        }
                    ],
                    "bindings": [
                        {
                            "prompt_slug": "new-record",
                            "binding_scope": "channel",
                            "channel_slug": "ch-new",
                            "binding_status": "active",
                        }
                    ],
                }
                ok = svc.confirm_import(payload, mode="merge_only", dry_run=False, actor="tester")
                self.assertEqual(ok["import_status"], "OK")
                self.assertEqual(int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"]), 2)
                self.assertEqual(
                    int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records WHERE slug = 'existing-keep'").fetchone()["c"]),
                    1,
                )

                before = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"])
                rollback_payload = {
                    **payload,
                    "versions": payload["versions"] + [dict(payload["versions"][0])],
                }
                preview = svc.confirm_import(rollback_payload, mode="merge_only", dry_run=False, actor="tester")
                self.assertEqual(preview["import_status"], "INVALID")
                after = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"])
                self.assertEqual(before, after)
                self.assertEqual(int(existing["id"]), int(svc.get_record(int(existing["id"]))["id"]))
            finally:
                conn.close()

    def test_confirm_import_rolls_back_after_mid_apply_failure(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                payload = {
                    "schema_version": "prompt_registry_export_v1",
                    "records": [
                        {
                            "slug": "tx-record",
                            "code": "PR-TX-1",
                            "title": "Tx Record",
                            "record_type": "prompt_template",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        }
                    ],
                    "versions": [
                        {
                            "prompt_slug": "tx-record",
                            "version_number": 1,
                            "body_text": "Body {{name}}",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        }
                    ],
                    "variables": [
                        {
                            "prompt_slug": "tx-record",
                            "version_number": 1,
                            "name": "name",
                            "safety_class": "standard",
                            "required": True,
                            "default_value": "",
                            "description": "",
                        }
                    ],
                    "bindings": [
                        {
                            "prompt_slug": "tx-record",
                            "binding_scope": "workflow",
                            "workflow_slug": "wf-tx",
                            "binding_status": "active",
                        }
                    ],
                }
                self.assertEqual(svc.preview_import(payload, mode="merge_only")["import_status"], "OK")
                with patch.object(svc, "create_binding", side_effect=RuntimeError("forced binding failure")):
                    with self.assertRaisesRegex(RuntimeError, "forced binding failure"):
                        svc.confirm_import(payload, mode="merge_only", dry_run=False, actor="tester")
                self.assertEqual(int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"]), 0)
                self.assertEqual(int(conn.execute("SELECT COUNT(*) AS c FROM prompt_versions").fetchone()["c"]), 0)
                self.assertEqual(int(conn.execute("SELECT COUNT(*) AS c FROM prompt_variables").fetchone()["c"]), 0)
                self.assertEqual(int(conn.execute("SELECT COUNT(*) AS c FROM prompt_bindings").fetchone()["c"]), 0)
            finally:
                conn.close()

    def test_confirm_import_sets_render_fingerprint_from_imported_variables(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                payload = {
                    "schema_version": "prompt_registry_export_v1",
                    "records": [
                        {
                            "slug": "fp-record",
                            "code": "PR-FP-1",
                            "title": "Fingerprint",
                            "record_type": "prompt_template",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        }
                    ],
                    "versions": [
                        {
                            "prompt_slug": "fp-record",
                            "version_number": 1,
                            "body_text": "Hello {{name}} {{token}}",
                            "status": "draft",
                            "validation_status": "UNKNOWN",
                        }
                    ],
                    "variables": [
                        {
                            "prompt_slug": "fp-record",
                            "version_number": 1,
                            "name": "token",
                            "safety_class": "secret",
                            "required": False,
                            "default_value": "tok-1",
                            "description": "",
                        },
                        {
                            "prompt_slug": "fp-record",
                            "version_number": 1,
                            "name": "name",
                            "safety_class": "standard",
                            "required": True,
                            "default_value": "Guest",
                            "description": "",
                        },
                    ],
                    "bindings": [],
                }
                self.assertEqual(svc.confirm_import(payload, mode="merge_only", dry_run=False, actor="tester")["import_status"], "OK")
                imported_version = conn.execute(
                    """
                    SELECT pv.id,pv.body_text,pv.render_fingerprint
                    FROM prompt_versions pv
                    JOIN prompt_records pr ON pr.id = pv.prompt_id
                    WHERE pr.slug = 'fp-record' AND pv.version_no = 1
                    """
                ).fetchone()
                self.assertIsNotNone(imported_version)
                imported_variables = list(
                    conn.execute(
                        """
                        SELECT name,safety_class,required,default_value,description
                        FROM prompt_variables
                        WHERE prompt_version_id = ?
                        ORDER BY name ASC
                        """,
                        (int(imported_version["id"]),),
                    ).fetchall()
                )
                expected_fingerprint = svc._build_render_fingerprint(str(imported_version["body_text"]), imported_variables)
                self.assertEqual(imported_version["render_fingerprint"], expected_fingerprint)
            finally:
                conn.close()

    def test_preview_foundation_render_and_diagnostics(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                created = svc.create_record(
                    {
                        "slug": "preview-template",
                        "code": "PR-PREVIEW-1",
                        "title": "Preview Template",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(created["id"])
                version = svc.create_version(
                    prompt_id,
                    {
                        "body_text": "Hello {{name}}. Role={{role}} Secret={{token}} Snippet={{snippet_ref}}.",
                        "variables": [
                            {"name": "name", "safety_class": "standard", "required": True},
                            {"name": "role", "safety_class": "standard", "required": False, "default_value": "operator"},
                            {"name": "token", "safety_class": "secret", "required": False, "default_value": "abc123"},
                        ],
                    },
                    actor="tester",
                )

                ok_preview = svc.preview_version(int(version["id"]), {"variables": {"name": "Alice", "extra": "x"}})
                self.assertEqual(ok_preview["preview_status"], "INVALID")
                self.assertEqual(ok_preview["used_variables"]["name"], "Alice")
                self.assertEqual(ok_preview["used_variables"]["role"], "operator")
                self.assertEqual(ok_preview["used_variables"]["token"], "***MASKED***")
                self.assertIn("token", ok_preview["masked_variables"])
                self.assertIn("role", ok_preview["diagnostics"]["defaults_used"])
                self.assertIn("extra", ok_preview["diagnostics"]["unknown_variables"])
                self.assertIn("snippet_ref", ok_preview["diagnostics"]["unresolved_placeholders"])

                missing_required = svc.preview_version(int(version["id"]), {"variables": {}})
                self.assertEqual(missing_required["preview_status"], "INVALID")
                self.assertIn("name", missing_required["missing_variables"])
                self.assertIn("name", missing_required["diagnostics"]["missing_required"])

                unmasked = svc.preview_version(int(version["id"]), {"variables": {"name": "Alice"}, "mask_sensitive": False})
                self.assertEqual(unmasked["preview_status"], "INVALID")
                self.assertIn("Secret=abc123", unmasked["rendered_text"])
                self.assertEqual(unmasked["masked_variables"], [])
                self.assertEqual(unmasked["used_variables"]["token"], "abc123")
            finally:
                conn.close()


    def test_preview_resolved_prompt_foundation_cases(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                matching = svc.create_record(
                    {
                        "slug": "resolved-preview-template",
                        "code": "PR-RESOLVED-1",
                        "title": "Resolved Preview",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                matching_prompt_id = int(matching["id"])
                matching_version = svc.create_version(
                    matching_prompt_id,
                    {
                        "body_text": "Hello {{name}} Secret={{token}}",
                        "variables": [
                            {"name": "name", "safety_class": "standard", "required": True},
                            {"name": "token", "safety_class": "secret", "required": False, "default_value": "tok-1"},
                        ],
                    },
                    actor="tester",
                )
                svc.activate_version(int(matching_version["id"]), actor="tester")
                svc.create_binding(
                    {
                        "prompt_id": matching_prompt_id,
                        "binding_scope": "workflow",
                        "workflow_slug": "wf-preview",
                        "binding_status": "active",
                    },
                    actor="tester",
                )

                no_active = svc.create_record(
                    {
                        "slug": "resolved-preview-no-active",
                        "code": "PR-RESOLVED-2",
                        "title": "Resolved Preview No Active",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                no_active_prompt_id = int(no_active["id"])
                svc.create_binding(
                    {
                        "prompt_id": no_active_prompt_id,
                        "binding_scope": "channel",
                        "channel_slug": "ch-no-active",
                        "binding_status": "active",
                    },
                    actor="tester",
                )

                matched = svc.preview_resolved_prompt({"workflow_slug": "wf-preview", "variables": {"name": "Alice"}})
                self.assertEqual(matched["overall_status"], "OK")
                self.assertEqual(matched["resolution"]["resolution_status"], "matched")
                self.assertEqual(matched["resolution"]["winner_binding"]["binding_scope"], "workflow")
                self.assertEqual(matched["preview"]["preview_status"], "OK")
                self.assertIn("Alice", matched["preview"]["rendered_text"])
                self.assertIn("***MASKED***", matched["preview"]["rendered_text"])

                missing_required = svc.preview_resolved_prompt({"workflow_slug": "wf-preview", "variables": {}})
                self.assertEqual(missing_required["overall_status"], "INVALID")
                self.assertEqual(missing_required["preview"]["preview_status"], "INVALID")
                self.assertIn("name", missing_required["preview"]["missing_variables"])

                unmasked = svc.preview_resolved_prompt(
                    {"workflow_slug": "wf-preview", "variables": {"name": "Alice"}, "mask_sensitive": False}
                )
                self.assertEqual(unmasked["preview"]["preview_status"], "OK")
                self.assertIn("tok-1", unmasked["preview"]["rendered_text"])
                self.assertEqual(unmasked["preview"]["masked_variables"], [])

                miss = svc.preview_resolved_prompt({"workflow_slug": "wf-miss", "variables": {"name": "Alice"}})
                self.assertEqual(miss["overall_status"], "INVALID")
                self.assertEqual(miss["resolution"]["resolution_status"], "miss")
                self.assertEqual(miss["preview"]["preview_status"], "INVALID")
                self.assertIn("no matching prompt binding", miss["preview"]["diagnostics"]["errors"][0])

                no_active_result = svc.preview_resolved_prompt(
                    {"channel_slug": "ch-no-active", "variables": {"name": "Alice"}}
                )
                self.assertEqual(no_active_result["overall_status"], "INVALID")
                self.assertEqual(no_active_result["resolution"]["resolution_status"], "matched")
                self.assertEqual(no_active_result["preview"]["preview_status"], "INVALID")
                self.assertIn("no active version", no_active_result["preview"]["diagnostics"]["errors"][0])

                usage_events = list(
                    conn.execute("SELECT * FROM prompt_usage_events ORDER BY created_at DESC, id DESC").fetchall()
                )
                self.assertGreaterEqual(len(usage_events), 5)
                self.assertEqual(str(usage_events[0]["event_type"]), "resolved_preview")
                matched_event = [row for row in usage_events if str(row["status"]) == "OK"][0]
                self.assertIsNotNone(matched_event["binding_id"])
                self.assertIsNotNone(matched_event["version_id"])
                self.assertIsNotNone(matched_event["prompt_id"])
                self.assertNotEqual(str(matched_event["render_fingerprint"]), "")
                self.assertNotIn("Alice", str(matched_event["variables_schema_json"]))
                self.assertNotIn("tok-1", str(matched_event["variables_schema_json"]))
            finally:
                conn.close()

    def test_usage_event_write_rules_for_preview_version(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                created = svc.create_record(
                    {
                        "slug": "usage-version-template",
                        "code": "PR-USAGE-VERSION-1",
                        "title": "Usage Version",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                version = svc.create_version(
                    int(created["id"]),
                    {
                        "body_text": "Hi {{name}}",
                        "variables": [{"name": "name", "safety_class": "standard", "required": True}],
                    },
                    actor="tester",
                )

                ok = svc.preview_version(int(version["id"]), {"variables": {"name": "Alice"}})
                self.assertEqual(ok["preview_status"], "OK")
                invalid = svc.preview_version(int(version["id"]), {"variables": {}})
                self.assertEqual(invalid["preview_status"], "INVALID")

                before_error_count = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_usage_events").fetchone()["c"])
                with self.assertRaisesRegex(Exception, "prompt version 999999 not found"):
                    svc.preview_version(999999, {"variables": {"name": "Alice"}})
                with self.assertRaisesRegex(Exception, "variables must be an object"):
                    svc.preview_version(int(version["id"]), {"variables": []})
                after_error_count = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_usage_events").fetchone()["c"])
                self.assertEqual(before_error_count, after_error_count)

                usage_rows = list(
                    conn.execute("SELECT event_type,status FROM prompt_usage_events ORDER BY id ASC").fetchall()
                )
                self.assertEqual([(row["event_type"], row["status"]) for row in usage_rows], [("version_preview", "OK"), ("version_preview", "INVALID")])
            finally:
                conn.close()

    def test_usage_events_list_and_summary(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec = svc.create_record(
                    {
                        "slug": "usage-list-template",
                        "code": "PR-USAGE-LIST-1",
                        "title": "Usage List",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                version = svc.create_version(
                    int(rec["id"]),
                    {
                        "body_text": "Hello {{name}}",
                        "variables": [{"name": "name", "safety_class": "standard", "required": True}],
                    },
                    actor="tester",
                )
                svc.preview_version(int(version["id"]), {"variables": {"name": "A"}})
                svc.preview_version(int(version["id"]), {"variables": {}})

                listed = svc.list_usage_events(
                    prompt_id=int(rec["id"]),
                    version_id=int(version["id"]),
                    event_type="version_preview",
                    status="INVALID",
                    limit=50,
                )
                self.assertEqual(len(listed), 1)
                self.assertEqual(listed[0]["status"], "INVALID")
                self.assertNotIn("context_json", listed[0])
                self.assertNotIn("variables_schema_json", listed[0])
                self.assertNotIn("diagnostics_json", listed[0])

                summary = svc.usage_summary(prompt_id=int(rec["id"]), version_id=int(version["id"]))
                self.assertEqual(summary["total_events"], 2)
                self.assertEqual(summary["by_event_type"]["version_preview"], 2)
                self.assertEqual(summary["by_status"]["OK"], 1)
                self.assertEqual(summary["by_status"]["INVALID"], 1)
                self.assertEqual(summary["prompt_ids"], [int(rec["id"])])
                self.assertEqual(summary["version_ids"], [int(version["id"])])
            finally:
                conn.close()

    def test_linked_actions_create_list_status_and_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec = svc.create_record(
                    {
                        "slug": "linked-actions-a",
                        "code": "PR-LA-A",
                        "title": "Linked Actions A",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(rec["id"])

                created = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "open-usage-page",
                        "action_type": "ui_action",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/usage",
                        "config_json": {"note": "safe"},
                    },
                    "tester",
                )
                self.assertEqual(str(created["action_key"]), "open-usage-page")
                self.assertEqual(str(created["action_status"]), "active")
                self.assertEqual(created["config"], {"note": "safe"})

                listed_all = svc.list_linked_actions(prompt_id, include_inactive=True)
                self.assertEqual(len(listed_all), 1)
                self.assertEqual(int(listed_all[0]["id"]), int(created["id"]))

                with self.assertRaisesRegex(Exception, "action_type must be one of"):
                    svc.create_linked_action(
                        prompt_id,
                        {"action_key": "bad-type", "action_type": "bad", "target_kind": "route", "config_json": {}},
                        "tester",
                    )
                with self.assertRaisesRegex(Exception, "config_json must be an object"):
                    svc.create_linked_action(
                        prompt_id,
                        {"action_key": "bad-json", "action_type": "ui_action", "target_kind": "route", "config_json": []},
                        "tester",
                    )
                with self.assertRaisesRegex(Exception, "must not include secret/token/password-like keys"):
                    svc.create_linked_action(
                        prompt_id,
                        {
                            "action_key": "bad-secret",
                            "action_type": "ui_action",
                            "target_kind": "route",
                            "config_json": {"api_token": "abc"},
                        },
                        "tester",
                    )
                with self.assertRaisesRegex(Exception, "must not include secret/token/password-like keys"):
                    svc.create_linked_action(
                        prompt_id,
                        {
                            "action_key": "bad-secret-nested",
                            "action_type": "ui_action",
                            "target_kind": "route",
                            "config_json": {"meta": {"authToken": "abc"}},
                        },
                        "tester",
                    )
                with self.assertRaisesRegex(Exception, "duplicate active action_key"):
                    svc.create_linked_action(
                        prompt_id,
                        {
                            "action_key": "open-usage-page",
                            "action_type": "api_endpoint",
                            "target_kind": "endpoint",
                            "config_json": {},
                        },
                        "tester",
                    )

                deactivated = svc.update_linked_action_status(int(created["id"]), {"action_status": "inactive"}, "tester")
                self.assertEqual(str(deactivated["action_status"]), "inactive")

                recreated = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "open-usage-page",
                        "action_type": "ui_action",
                        "target_kind": "route",
                        "config_json": {},
                    },
                    "tester",
                )
                self.assertEqual(str(recreated["action_status"]), "active")

                listed_active = svc.list_linked_actions(prompt_id, include_inactive=False)
                self.assertEqual({int(row["id"]) for row in listed_active}, {int(recreated["id"])})

                audit_events = svc.list_audit_events(prompt_id)
                self.assertTrue(any(str(item["event_type"]) == "linked_action_created" for item in audit_events))
                self.assertTrue(any(str(item["event_type"]) == "linked_action_status_updated" for item in audit_events))
            finally:
                conn.close()

    def test_linked_action_preview_diagnostics_and_read_only_behavior(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec = svc.create_record(
                    {
                        "slug": "linked-preview",
                        "code": "PR-LINK-PREVIEW",
                        "title": "Linked Preview",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(rec["id"])

                active = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "active-route",
                        "action_type": "ui_action",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/usage",
                        "config_json": {"note": "safe", "deep": {"count": 2}},
                    },
                    "tester",
                )
                inactive = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "inactive-route",
                        "action_type": "ui_action",
                        "action_status": "inactive",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/usage",
                        "config_json": {"note": "safe"},
                    },
                    "tester",
                )
                missing_ref = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "missing-ref",
                        "action_type": "workflow",
                        "action_status": "active",
                        "target_kind": "workflow",
                        "target_ref": "",
                        "config_json": {},
                    },
                    "tester",
                )
                mismatch = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "mismatch",
                        "action_type": "api_endpoint",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "/ui/mismatch",
                        "config_json": {},
                    },
                    "tester",
                )
                note_ok = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "note-ok",
                        "action_type": "external_note",
                        "action_status": "active",
                        "target_kind": "note",
                        "target_ref": "",
                        "config_json": {"title": "Reminder"},
                    },
                    "tester",
                )

                initial_audit_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_audit_events WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )

                active_preview = svc.preview_linked_action(int(active["id"]))
                self.assertEqual(active_preview["preview_status"], "OK")
                self.assertTrue(active_preview["can_execute_later"])
                self.assertEqual(active_preview["normalized_target"]["kind"], "route")
                self.assertEqual(active_preview["normalized_target"]["ref"], "/ui/prompt-registry/usage")

                inactive_preview = svc.preview_linked_action(int(inactive["id"]))
                self.assertEqual(inactive_preview["preview_status"], "WARNING")
                self.assertFalse(inactive_preview["can_execute_later"])
                self.assertTrue(any(item["code"] == "LINKED_ACTION_INACTIVE" for item in inactive_preview["diagnostics"]))

                missing_ref_preview = svc.preview_linked_action(int(missing_ref["id"]))
                self.assertEqual(missing_ref_preview["preview_status"], "INVALID")
                self.assertFalse(missing_ref_preview["can_execute_later"])
                self.assertTrue(any(item["severity"] == "BLOCKING" for item in missing_ref_preview["diagnostics"]))

                mismatch_preview = svc.preview_linked_action(int(mismatch["id"]))
                self.assertEqual(mismatch_preview["preview_status"], "WARNING")
                self.assertTrue(mismatch_preview["can_execute_later"])
                self.assertTrue(any(item["code"] == "LINKED_ACTION_TARGET_KIND_MISMATCH" for item in mismatch_preview["diagnostics"]))

                note_preview = svc.preview_linked_action(int(note_ok["id"]))
                self.assertEqual(note_preview["preview_status"], "OK")
                self.assertTrue(note_preview["can_execute_later"])
                self.assertTrue(
                    any(item["code"] == "LINKED_ACTION_NOTE_TARGET_REF_OPTIONAL" for item in note_preview["diagnostics"])
                )

                final_audit_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_audit_events WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )
                self.assertEqual(initial_audit_count, final_audit_count)
            finally:
                conn.close()

    def test_linked_action_execution_requests_create_and_filters(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec = svc.create_record(
                    {
                        "slug": "linked-exec",
                        "code": "PR-LINK-EXEC",
                        "title": "Linked Exec",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(rec["id"])
                ok_action = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "ok-action",
                        "action_type": "ui_action",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/usage",
                        "config_json": {"note": "safe"},
                    },
                    "tester",
                )
                inactive_action = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "inactive-action",
                        "action_type": "ui_action",
                        "action_status": "inactive",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/usage",
                        "config_json": {"note": "safe"},
                    },
                    "tester",
                )
                invalid_action = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "invalid-action",
                        "action_type": "workflow",
                        "action_status": "active",
                        "target_kind": "workflow",
                        "target_ref": "",
                        "config_json": {},
                    },
                    "tester",
                )
                preview_only = svc.create_linked_action_execution_request(
                    int(ok_action["id"]),
                    {"confirm_execution": False, "request_context_json": {"reason": "dry-run"}},
                    "tester",
                )
                self.assertEqual(preview_only["request_status"], "preview_only")
                self.assertEqual(preview_only["preview_status"], "OK")
                self.assertTrue(preview_only["can_execute_later"])

                accepted = svc.create_linked_action_execution_request(
                    int(ok_action["id"]),
                    {"confirm_execution": True, "request_context_json": {"reason": "approve"}},
                    "tester",
                )
                self.assertEqual(accepted["request_status"], "accepted")

                inactive_blocked = svc.create_linked_action_execution_request(
                    int(inactive_action["id"]),
                    {"confirm_execution": True, "request_context_json": {"reason": "inactive"}},
                    "tester",
                )
                self.assertEqual(inactive_blocked["request_status"], "blocked")
                self.assertEqual(inactive_blocked["preview_status"], "WARNING")

                invalid_blocked = svc.create_linked_action_execution_request(
                    int(invalid_action["id"]),
                    {"confirm_execution": True, "request_context_json": {"reason": "invalid"}},
                    "tester",
                )
                self.assertEqual(invalid_blocked["request_status"], "blocked")
                self.assertEqual(invalid_blocked["preview_status"], "INVALID")

                with self.assertRaisesRegex(Exception, "must not include secret/token/password-like keys"):
                    svc.create_linked_action_execution_request(
                        int(ok_action["id"]),
                        {"confirm_execution": False, "request_context_json": {"nested": {"apiToken": "x"}}},
                        "tester",
                    )

                by_prompt = svc.list_linked_action_execution_requests(prompt_id=prompt_id, limit=50)
                self.assertGreaterEqual(len(by_prompt), 4)
                by_action = svc.list_linked_action_execution_requests(action_id=int(ok_action["id"]), limit=50)
                self.assertEqual({str(item["request_status"]) for item in by_action}, {"preview_only", "accepted"})
                by_status = svc.list_linked_action_execution_requests(prompt_id=prompt_id, request_status="accepted", limit=50)
                self.assertEqual({str(item["request_status"]) for item in by_status}, {"accepted"})
                by_preview_status = svc.list_linked_action_execution_requests(prompt_id=prompt_id, preview_status="WARNING", limit=50)
                self.assertTrue(by_preview_status)
                self.assertEqual({str(item["preview_status"]) for item in by_preview_status}, {"WARNING"})
                by_actor = svc.list_linked_action_execution_requests(prompt_id=prompt_id, requested_by="tester", limit=50)
                self.assertGreaterEqual(len(by_actor), 4)
                with self.assertRaisesRegex(Exception, "request_status must be one of"):
                    svc.list_linked_action_execution_requests(request_status="bad", limit=50)
                with self.assertRaisesRegex(Exception, "limit must be between 1 and 200"):
                    svc.list_linked_action_execution_requests(limit=0)

                audit_events = svc.list_audit_events(prompt_id)
                self.assertTrue(any(str(item["event_type"]) == "linked_action_execution_requested" for item in audit_events))
            finally:
                conn.close()

    def test_linked_action_execution_requests_table_constraints_smoke(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec = svc.create_record(
                    {
                        "slug": "linked-exec-constraints",
                        "code": "PR-LINK-EXEC-C",
                        "title": "Linked Exec Constraints",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                action = svc.create_linked_action(
                    int(rec["id"]),
                    {
                        "action_key": "constraints-action",
                        "action_type": "ui_action",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/usage",
                        "config_json": {},
                    },
                    "tester",
                )
                now = svc._now_iso()
                with self.assertRaises(Exception):
                    conn.execute(
                        """
                        INSERT INTO prompt_linked_action_execution_requests(
                            action_id,prompt_id,request_status,requested_by,preview_status,can_execute_later,
                            diagnostics_json,request_context_json,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(action["id"]),
                            int(rec["id"]),
                            "bad_status",
                            "tester",
                            "OK",
                            1,
                            "[]",
                            "{}",
                            now,
                            now,
                        ),
                    )
                with self.assertRaises(Exception):
                    conn.execute(
                        """
                        INSERT INTO prompt_linked_action_execution_requests(
                            action_id,prompt_id,request_status,requested_by,preview_status,can_execute_later,
                            diagnostics_json,request_context_json,created_at,updated_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(action["id"]),
                            int(rec["id"]),
                            "preview_only",
                            "tester",
                            "OK",
                            1,
                            "{bad json",
                            "{}",
                            now,
                            now,
                        ),
                    )
            finally:
                conn.close()

    def test_preview_linked_action_dispatch_plan_ready_blocked_unknown_and_read_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = PromptRegistryService(conn)
                rec = svc.create_record(
                    {
                        "slug": "dispatch-preview",
                        "code": "PR-DISPATCH-PREVIEW",
                        "title": "Dispatch Preview",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                    actor="tester",
                )
                prompt_id = int(rec["id"])
                ready_action = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "dispatch-ready",
                        "action_type": "ui_action",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "/ui/prompt-registry/linked-action-requests",
                        "config_json": {"ui_label": "Open", "channel": "ops"},
                    },
                    "tester",
                )
                preview_only_req = svc.create_linked_action_execution_request(
                    int(ready_action["id"]),
                    {"confirm_execution": False, "request_context_json": {"reason": "dry-run", "operator": "qa"}},
                    "tester",
                )
                accepted_req = svc.create_linked_action_execution_request(
                    int(ready_action["id"]),
                    {"confirm_execution": True, "request_context_json": {"reason": "approved", "operator": "alice"}},
                    "tester",
                )

                blocked_action = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "dispatch-blocked",
                        "action_type": "ui_action",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "",
                        "config_json": {"note": "missing target"},
                    },
                    "tester",
                )
                blocked_req = svc.create_linked_action_execution_request(
                    int(blocked_action["id"]),
                    {"confirm_execution": True, "request_context_json": {"reason": "bad-target"}},
                    "tester",
                )

                unknown_action = svc.create_linked_action(
                    prompt_id,
                    {
                        "action_key": "dispatch-unknown",
                        "action_type": "workflow",
                        "action_status": "active",
                        "target_kind": "route",
                        "target_ref": "/v1/example",
                        "config_json": {"note": "kind mismatch"},
                    },
                    "tester",
                )
                unknown_req = svc.create_linked_action_execution_request(
                    int(unknown_action["id"]),
                    {"confirm_execution": True, "request_context_json": {"reason": "mismatch"}},
                    "tester",
                )

                before_audit = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_audit_events").fetchone()["c"])
                before_usage = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_usage_events").fetchone()["c"])
                before_requests = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_linked_action_execution_requests").fetchone()["c"])

                ready_plan = svc.preview_linked_action_dispatch_plan(int(accepted_req["id"]))
                self.assertEqual(ready_plan["dispatch_status"], "READY")
                self.assertEqual(ready_plan["dispatch_kind"], "ui_route")
                self.assertEqual(ready_plan["dispatch_target"], "/ui/prompt-registry/linked-action-requests")
                self.assertEqual(ready_plan["reason_codes"], [])
                self.assertIn("ui_label=Open", ready_plan["safe_config_summary"])

                preview_only_plan = svc.preview_linked_action_dispatch_plan(int(preview_only_req["id"]))
                self.assertEqual(preview_only_plan["dispatch_status"], "BLOCKED")
                self.assertIn("REQUEST_NOT_ACCEPTED", preview_only_plan["reason_codes"])

                blocked_plan = svc.preview_linked_action_dispatch_plan(int(blocked_req["id"]))
                self.assertEqual(blocked_plan["dispatch_status"], "BLOCKED")
                self.assertIn("TARGET_REF_REQUIRED", blocked_plan["reason_codes"])
                self.assertIn("LINKED_ACTION_PREVIEW_INVALID", blocked_plan["reason_codes"])

                unknown_plan = svc.preview_linked_action_dispatch_plan(int(unknown_req["id"]))
                self.assertEqual(unknown_plan["dispatch_status"], "READY")
                self.assertEqual(unknown_plan["dispatch_kind"], "unknown")
                self.assertIn("DISPATCH_KIND_UNKNOWN", unknown_plan["reason_codes"])

                with self.assertRaisesRegex(Exception, "linked action execution request not found"):
                    svc.preview_linked_action_dispatch_plan(999999)

                after_audit = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_audit_events").fetchone()["c"])
                after_usage = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_usage_events").fetchone()["c"])
                after_requests = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_linked_action_execution_requests").fetchone()["c"])
                self.assertEqual(after_audit, before_audit)
                self.assertEqual(after_usage, before_usage)
                self.assertEqual(after_requests, before_requests)
            finally:
                conn.close()
