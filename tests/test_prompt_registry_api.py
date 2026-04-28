from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPromptRegistryApi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_contracts_records_versions_endpoints(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            contracts = client.get("/v1/prompt-registry/contracts", headers=headers)
            self.assertEqual(contracts.status_code, 200)
            self.assertIn("record_type", contracts.json())
            self.assertIn("safety_class", contracts.json())

            bridge_policy = client.get("/v1/prompt-registry/bridge-policy", headers=headers)
            self.assertEqual(bridge_policy.status_code, 200)
            bridge_payload = bridge_policy.json()
            self.assertEqual(bridge_payload["mode"], "bridge_safe_foundation")
            self.assertEqual(bridge_payload["runtime_bridge_execution"], "not_implemented")
            self.assertEqual(bridge_payload["authoritative_surfaces"]["title_templates"], "authoritative")
            self.assertEqual(bridge_payload["authoritative_surfaces"]["description_templates"], "authoritative")
            self.assertEqual(bridge_payload["authoritative_surfaces"]["video_tag_presets"], "authoritative")
            self.assertEqual(bridge_payload["authoritative_surfaces"]["channel_visual_style_templates"], "authoritative")

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-template",
                    "code": "PR-API-1",
                    "title": "API Template",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])

            listed = client.get("/v1/prompt-registry/records", headers=headers)
            self.assertEqual(listed.status_code, 200)
            self.assertGreaterEqual(len(listed.json()["items"]), 1)

            fetched = client.get(f"/v1/prompt-registry/records/{prompt_id}", headers=headers)
            self.assertEqual(fetched.status_code, 200)

            patched = client.patch(f"/v1/prompt-registry/records/{prompt_id}", headers=headers, json={"status": "active"})
            self.assertEqual(patched.status_code, 422)

            version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "body {{x}}",
                    "variables": [{"name": "x", "safety_class": "standard", "required": True}],
                },
            )
            self.assertEqual(version.status_code, 200)
            version_id = int(version.json()["id"])
            self.assertIn("render_fingerprint", version.json())
            self.assertIsInstance(version.json()["render_fingerprint"], str)

            versions = client.get(f"/v1/prompt-registry/records/{prompt_id}/versions", headers=headers)
            self.assertEqual(versions.status_code, 200)
            self.assertEqual(len(versions.json()["items"]), 1)
            self.assertIn("render_fingerprint", versions.json()["items"][0])

            get_version = client.get(f"/v1/prompt-registry/versions/{version_id}", headers=headers)
            self.assertEqual(get_version.status_code, 200)
            self.assertEqual(get_version.json()["variables"][0]["safety_class"], "standard")
            self.assertIn("render_fingerprint", get_version.json())

            audit_response = client.get(f"/v1/prompt-registry/records/{prompt_id}/audit", headers=headers)
            self.assertEqual(audit_response.status_code, 200)
            audit_payload = audit_response.json()
            self.assertEqual(audit_payload["prompt_id"], prompt_id)
            self.assertGreaterEqual(len(audit_payload["items"]), 2)
            audit_ids = [item["id"] for item in audit_payload["items"]]
            self.assertEqual(audit_ids, sorted(audit_ids))
            self.assertTrue(all("payload_json" not in item for item in audit_payload["items"]))

            activated = client.post(f"/v1/prompt-registry/versions/{version_id}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)
            self.assertEqual(int(activated.json()["is_active"]), 1)

            conn = dbm.connect(env)
            try:
                audit_rows = conn.execute(
                    "SELECT event_type,actor FROM prompt_audit_events WHERE prompt_id = ? ORDER BY id ASC",
                    (prompt_id,),
                ).fetchall()
                self.assertTrue(all(row["actor"] == "admin" for row in audit_rows))
                table_names = {
                    row["name"]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?)",
                        (
                            "title_templates",
                            "description_templates",
                            "video_tag_presets",
                            "channel_visual_style_templates",
                        ),
                    ).fetchall()
                }
                self.assertEqual(
                    table_names,
                    {
                        "title_templates",
                        "description_templates",
                        "video_tag_presets",
                        "channel_visual_style_templates",
                    },
                )
            finally:
                conn.close()

    def test_audit_endpoint_returns_404_for_unknown_prompt(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            missing = client.get("/v1/prompt-registry/records/999999/audit", headers=headers)
            self.assertEqual(missing.status_code, 404)

    def test_validation_duplicate_and_lifecycle_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            bad = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={"slug": "", "code": "", "title": "", "record_type": "bad", "status": "bad"},
            )
            self.assertEqual(bad.status_code, 422)

            first = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "dupe-api",
                    "code": "PR-API-DUPE",
                    "title": "Dup",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(first.status_code, 200)
            prompt_id = int(first.json()["id"])

            dupe = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "dupe-api",
                    "code": "PR-API-DUPE",
                    "title": "Dup",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(dupe.status_code, 409)

            arch = client.patch(f"/v1/prompt-registry/records/{prompt_id}", headers=headers, json={"status": "archived"})
            self.assertEqual(arch.status_code, 200)
            invalid_transition = client.patch(
                f"/v1/prompt-registry/records/{prompt_id}", headers=headers, json={"status": "active"}
            )
            self.assertEqual(invalid_transition.status_code, 422)

            invalid_version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={"body_text": "x", "variables": [{"name": "secret", "safety_class": "not_allowed"}]},
            )
            self.assertEqual(invalid_version.status_code, 422)

    def test_version_create_atomicity_and_duplicate_names_api(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-atomic",
                    "code": "PR-API-ATOMIC",
                    "title": "API Atomic",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])

            invalid_payload = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={"body_text": "x", "variables": ["not-an-object"]},
            )
            self.assertEqual(invalid_payload.status_code, 422)

            duplicate_names = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "x {{a}} {{a}}",
                    "variables": [
                        {"name": "a", "safety_class": "standard"},
                        {"name": "a", "safety_class": "standard"},
                    ],
                },
            )
            self.assertEqual(duplicate_names.status_code, 422)

            contradictory_active = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={"body_text": "x", "status": "active"},
            )
            self.assertEqual(contradictory_active.status_code, 422)

            conn = dbm.connect(env)
            try:
                version_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )
                self.assertEqual(version_count, 0)
            finally:
                conn.close()

    def test_bindings_and_resolve_endpoints(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            prompt_ids: dict[str, int] = {}
            for name, code in (
                ("global", "PR-API-BIND-G"),
                ("workflow", "PR-API-BIND-W"),
                ("channel", "PR-API-BIND-C"),
                ("item", "PR-API-BIND-I"),
            ):
                created = client.post(
                    "/v1/prompt-registry/records",
                    headers=headers,
                    json={
                        "slug": f"api-bind-{name}",
                        "code": code,
                        "title": f"Bind {name}",
                        "record_type": "prompt_template",
                        "status": "draft",
                    },
                )
                self.assertEqual(created.status_code, 200)
                prompt_ids[name] = int(created.json()["id"])

            global_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={"prompt_id": prompt_ids["global"], "binding_scope": "global", "binding_status": "active"},
            )
            self.assertEqual(global_binding.status_code, 200)
            channel_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": prompt_ids["channel"],
                    "binding_scope": "channel",
                    "channel_slug": "channel-1",
                    "binding_status": "active",
                },
            )
            self.assertEqual(channel_binding.status_code, 200)
            workflow_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": prompt_ids["workflow"],
                    "binding_scope": "workflow",
                    "workflow_slug": "wf-1",
                    "binding_status": "active",
                },
            )
            self.assertEqual(workflow_binding.status_code, 200)
            item_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": prompt_ids["item"],
                    "binding_scope": "item",
                    "item_type": "release",
                    "item_ref": "rel-1",
                    "binding_status": "active",
                },
            )
            self.assertEqual(item_binding.status_code, 200)

            list_response = client.get("/v1/prompt-registry/bindings", headers=headers)
            self.assertEqual(list_response.status_code, 200)
            self.assertGreaterEqual(len(list_response.json()["items"]), 4)
            filtered_scope = client.get("/v1/prompt-registry/bindings?binding_scope=channel", headers=headers)
            self.assertEqual(filtered_scope.status_code, 200)
            self.assertTrue(all(item["binding_scope"] == "channel" for item in filtered_scope.json()["items"]))
            filtered_composed = client.get(
                f"/v1/prompt-registry/bindings?prompt_id={prompt_ids['channel']}&binding_scope=channel&binding_status=active",
                headers=headers,
            )
            self.assertEqual(filtered_composed.status_code, 200)
            self.assertEqual(len(filtered_composed.json()["items"]), 1)
            invalid_filter = client.get("/v1/prompt-registry/bindings?binding_scope=bad-scope", headers=headers)
            self.assertEqual(invalid_filter.status_code, 422)
            self.assertEqual(invalid_filter.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")
            invalid_prompt_id = client.get("/v1/prompt-registry/bindings?prompt_id=not-an-int", headers=headers)
            self.assertEqual(invalid_prompt_id.status_code, 422)
            self.assertEqual(invalid_prompt_id.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")

            duplicate = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": prompt_ids["item"],
                    "binding_scope": "item",
                    "item_type": "release",
                    "item_ref": "rel-1",
                    "binding_status": "active",
                },
            )
            self.assertEqual(duplicate.status_code, 409)

            invalid_scope = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={"prompt_id": prompt_ids["workflow"], "binding_scope": "workflow", "channel_slug": "bad-mix"},
            )
            self.assertEqual(invalid_scope.status_code, 422)

            resolve = client.post(
                "/v1/prompt-registry/resolve",
                headers=headers,
                json={"workflow_slug": "wf-1", "channel_slug": "channel-1", "item_type": "release", "item_ref": "rel-1"},
            )
            self.assertEqual(resolve.status_code, 200)
            payload = resolve.json()
            self.assertEqual(payload["resolution_status"], "matched")
            self.assertEqual(payload["winner_binding"]["binding_scope"], "item")
            self.assertIn("evaluated_candidates", payload)
            self.assertTrue(any("reason" in candidate for candidate in payload["evaluated_candidates"]))
            self.assertTrue(all("reason_code" in candidate for candidate in payload["evaluated_candidates"]))
            self.assertEqual(
                [candidate["evaluated_order"] for candidate in payload["evaluated_candidates"]],
                list(range(1, len(payload["evaluated_candidates"]) + 1)),
            )

            deactivated = client.patch(
                f"/v1/prompt-registry/bindings/{int(item_binding.json()['id'])}",
                headers=headers,
                json={"binding_status": "inactive"},
            )
            self.assertEqual(deactivated.status_code, 200)
            fallback = client.post(
                "/v1/prompt-registry/resolve",
                headers=headers,
                json={"workflow_slug": "wf-1", "channel_slug": "channel-1", "item_type": "release", "item_ref": "rel-1"},
            )
            self.assertEqual(fallback.status_code, 200)
            self.assertEqual(fallback.json()["winner_binding"]["binding_scope"], "channel")

            client.patch(
                f"/v1/prompt-registry/bindings/{int(channel_binding.json()['id'])}",
                headers=headers,
                json={"binding_status": "inactive"},
            )
            client.patch(
                f"/v1/prompt-registry/bindings/{int(workflow_binding.json()['id'])}",
                headers=headers,
                json={"binding_status": "inactive"},
            )
            client.patch(
                f"/v1/prompt-registry/bindings/{int(global_binding.json()['id'])}",
                headers=headers,
                json={"binding_status": "inactive"},
            )
            miss = client.post("/v1/prompt-registry/resolve", headers=headers, json={})
            self.assertEqual(miss.status_code, 200)
            self.assertEqual(miss.json()["resolution_status"], "miss")
            self.assertIsNone(miss.json()["winner_binding"])

    def test_linked_actions_endpoints(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            created_record = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-linked-actions",
                    "code": "PR-API-LINK",
                    "title": "API Linked Actions",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created_record.status_code, 200)
            prompt_id = int(created_record.json()["id"])

            created_action = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
                headers=headers,
                json={
                    "action_key": "open-audit",
                    "action_type": "ui_action",
                    "action_status": "active",
                    "target_kind": "route",
                    "target_ref": "/ui/prompt-registry/1/audit",
                    "config_json": {"tab": "audit"},
                },
            )
            self.assertEqual(created_action.status_code, 200)
            action_id = int(created_action.json()["id"])
            self.assertEqual(created_action.json()["config"], {"tab": "audit"})

            listed = client.get(f"/v1/prompt-registry/records/{prompt_id}/linked-actions", headers=headers)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(len(listed.json()["items"]), 1)

            filtered = client.get(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions?include_inactive=false",
                headers=headers,
            )
            self.assertEqual(filtered.status_code, 200)
            self.assertEqual(len(filtered.json()["items"]), 1)

            duplicate = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
                headers=headers,
                json={
                    "action_key": "open-audit",
                    "action_type": "api_endpoint",
                    "target_kind": "endpoint",
                    "config_json": {},
                },
            )
            self.assertEqual(duplicate.status_code, 409)

            invalid_enum = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
                headers=headers,
                json={
                    "action_key": "invalid-enum",
                    "action_type": "not-allowed",
                    "target_kind": "route",
                    "config_json": {},
                },
            )
            self.assertEqual(invalid_enum.status_code, 422)

            invalid_json = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
                headers=headers,
                json={
                    "action_key": "invalid-json",
                    "action_type": "ui_action",
                    "target_kind": "route",
                    "config_json": [],
                },
            )
            self.assertEqual(invalid_json.status_code, 422)

            bad_secret = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
                headers=headers,
                json={
                    "action_key": "bad-secret",
                    "action_type": "ui_action",
                    "target_kind": "route",
                    "config_json": {"password_value": "abc"},
                },
            )
            self.assertEqual(bad_secret.status_code, 422)

            bad_secret_nested = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions",
                headers=headers,
                json={
                    "action_key": "bad-secret-nested",
                    "action_type": "ui_action",
                    "target_kind": "route",
                    "config_json": {"meta": {"authToken": "abc"}},
                },
            )
            self.assertEqual(bad_secret_nested.status_code, 422)

            status_update = client.post(
                f"/v1/prompt-registry/linked-actions/{action_id}/status",
                headers=headers,
                json={"action_status": "inactive"},
            )
            self.assertEqual(status_update.status_code, 200)
            self.assertEqual(status_update.json()["action_status"], "inactive")

            active_only = client.get(
                f"/v1/prompt-registry/records/{prompt_id}/linked-actions?include_inactive=false",
                headers=headers,
            )
            self.assertEqual(active_only.status_code, 200)
            self.assertEqual(active_only.json()["items"], [])

    def test_resolve_rejects_partial_item_context_and_explains_same_scope_tie_break(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            first_prompt = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "tie-first",
                    "code": "PR-TIE-FIRST",
                    "title": "Tie First",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            second_prompt = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "tie-second",
                    "code": "PR-TIE-SECOND",
                    "title": "Tie Second",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(first_prompt.status_code, 200)
            self.assertEqual(second_prompt.status_code, 200)
            first_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": int(first_prompt.json()["id"]),
                    "binding_scope": "channel",
                    "channel_slug": "tie-channel",
                    "binding_status": "active",
                },
            )
            second_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": int(second_prompt.json()["id"]),
                    "binding_scope": "channel",
                    "channel_slug": "tie-channel",
                    "binding_status": "active",
                },
            )
            self.assertEqual(first_binding.status_code, 200)
            self.assertEqual(second_binding.status_code, 200)

            only_type = client.post("/v1/prompt-registry/resolve", headers=headers, json={"item_type": "release"})
            self.assertEqual(only_type.status_code, 422)
            self.assertEqual(only_type.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")
            only_ref = client.post("/v1/prompt-registry/resolve", headers=headers, json={"item_ref": "rel-1"})
            self.assertEqual(only_ref.status_code, 422)
            self.assertEqual(only_ref.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")

            resolved = client.post("/v1/prompt-registry/resolve", headers=headers, json={"channel_slug": "tie-channel"})
            self.assertEqual(resolved.status_code, 200)
            payload = resolved.json()
            self.assertEqual(int(payload["winner_binding"]["binding_id"]), int(second_binding.json()["id"]))
            older = [item for item in payload["evaluated_candidates"] if int(item["binding_id"]) == int(first_binding.json()["id"])][0]
            self.assertEqual(older["reason_code"], "IGNORED_SAME_SCOPE_OLDER_BINDING")
            self.assertIn("tie_break_note", older)

    def test_preview_endpoint_foundation_cases(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-preview",
                    "code": "PR-API-PREVIEW",
                    "title": "API Preview",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])
            version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "Hello {{name}}! Role={{role}} Secret={{token}}.",
                    "variables": [
                        {"name": "name", "safety_class": "standard", "required": True},
                        {"name": "role", "safety_class": "standard", "required": False, "default_value": "operator"},
                        {"name": "token", "safety_class": "operator_only", "required": False, "default_value": "t-001"},
                    ],
                },
            )
            self.assertEqual(version.status_code, 200)
            version_id = int(version.json()["id"])

            ok = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {"name": "Alice"}},
            )
            self.assertEqual(ok.status_code, 200)
            ok_payload = ok.json()
            self.assertEqual(ok_payload["preview_status"], "OK")
            self.assertIn("Alice", ok_payload["rendered_text"])
            self.assertIn("operator", ok_payload["rendered_text"])
            self.assertIn("***MASKED***", ok_payload["rendered_text"])
            self.assertEqual(ok_payload["missing_variables"], [])
            self.assertIn("role", ok_payload["diagnostics"]["defaults_used"])
            self.assertIn("token", ok_payload["masked_variables"])

            missing = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {}},
            )
            self.assertEqual(missing.status_code, 200)
            missing_payload = missing.json()
            self.assertEqual(missing_payload["preview_status"], "INVALID")
            self.assertIn("name", missing_payload["missing_variables"])

            unknown = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {"name": "Alice", "unknown": "x"}},
            )
            self.assertEqual(unknown.status_code, 200)
            self.assertIn("unknown", unknown.json()["diagnostics"]["unknown_variables"])

            unmasked = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {"name": "Alice"}, "mask_sensitive": False},
            )
            self.assertEqual(unmasked.status_code, 200)
            self.assertIn("t-001", unmasked.json()["rendered_text"])
            self.assertEqual(unmasked.json()["masked_variables"], [])

            missing_version = client.post(
                "/v1/prompt-registry/versions/999999/preview",
                headers=headers,
                json={"variables": {"name": "Alice"}},
            )
            self.assertEqual(missing_version.status_code, 404)

            malformed = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": "not-an-object"},
            )
            self.assertEqual(malformed.status_code, 422)

            conn = dbm.connect(env)
            try:
                usage_rows = list(
                    conn.execute(
                        "SELECT event_type,status,prompt_id,version_id FROM prompt_usage_events ORDER BY id ASC"
                    ).fetchall()
                )
                self.assertEqual(
                    [(row["event_type"], row["status"]) for row in usage_rows],
                    [
                        ("version_preview", "OK"),
                        ("version_preview", "INVALID"),
                        ("version_preview", "OK"),
                        ("version_preview", "OK"),
                    ],
                )
                self.assertTrue(all(int(row["prompt_id"]) == prompt_id for row in usage_rows))
                self.assertTrue(all(int(row["version_id"]) == version_id for row in usage_rows))
            finally:
                conn.close()


    def test_resolve_preview_endpoint_foundation_cases(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            matched_record = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-resolve-preview-match",
                    "code": "PR-API-RESOLVE-1",
                    "title": "API Resolve Preview Match",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(matched_record.status_code, 200)
            matched_prompt_id = int(matched_record.json()["id"])

            matched_version = client.post(
                f"/v1/prompt-registry/records/{matched_prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "Hello {{name}} Secret={{token}}",
                    "variables": [
                        {"name": "name", "safety_class": "standard", "required": True},
                        {"name": "token", "safety_class": "operator_only", "required": False, "default_value": "api-tok"},
                    ],
                },
            )
            self.assertEqual(matched_version.status_code, 200)
            matched_version_id = int(matched_version.json()["id"])

            activated = client.post(f"/v1/prompt-registry/versions/{matched_version_id}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)

            matched_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": matched_prompt_id,
                    "binding_scope": "workflow",
                    "workflow_slug": "wf-resolve",
                    "binding_status": "active",
                },
            )
            self.assertEqual(matched_binding.status_code, 200)

            no_active_record = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-resolve-preview-no-active",
                    "code": "PR-API-RESOLVE-2",
                    "title": "API Resolve Preview No Active",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(no_active_record.status_code, 200)
            no_active_prompt_id = int(no_active_record.json()["id"])
            no_active_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": no_active_prompt_id,
                    "binding_scope": "channel",
                    "channel_slug": "ch-no-active",
                    "binding_status": "active",
                },
            )
            self.assertEqual(no_active_binding.status_code, 200)

            ok = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"workflow_slug": "wf-resolve", "variables": {"name": "Alice"}},
            )
            self.assertEqual(ok.status_code, 200)
            ok_payload = ok.json()
            self.assertEqual(ok_payload["overall_status"], "OK")
            self.assertEqual(ok_payload["resolution"]["resolution_status"], "matched")
            self.assertIn("winner_binding", ok_payload["resolution"])
            self.assertEqual(ok_payload["preview"]["preview_status"], "OK")
            self.assertIn("Alice", ok_payload["preview"]["rendered_text"])
            self.assertIn("***MASKED***", ok_payload["preview"]["rendered_text"])

            miss = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"workflow_slug": "wf-miss", "variables": {"name": "Alice"}},
            )
            self.assertEqual(miss.status_code, 200)
            miss_payload = miss.json()
            self.assertEqual(miss_payload["overall_status"], "INVALID")
            self.assertEqual(miss_payload["resolution"]["resolution_status"], "miss")
            self.assertEqual(miss_payload["preview"]["preview_status"], "INVALID")

            no_active = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"channel_slug": "ch-no-active", "variables": {"name": "Alice"}},
            )
            self.assertEqual(no_active.status_code, 200)
            no_active_payload = no_active.json()
            self.assertEqual(no_active_payload["overall_status"], "INVALID")
            self.assertEqual(no_active_payload["resolution"]["resolution_status"], "matched")
            self.assertEqual(no_active_payload["preview"]["preview_status"], "INVALID")

            missing_required = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"workflow_slug": "wf-resolve", "variables": {}},
            )
            self.assertEqual(missing_required.status_code, 200)
            self.assertEqual(missing_required.json()["overall_status"], "INVALID")
            self.assertIn("name", missing_required.json()["preview"]["missing_variables"])

            unmasked = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"workflow_slug": "wf-resolve", "variables": {"name": "Alice"}, "mask_sensitive": False},
            )
            self.assertEqual(unmasked.status_code, 200)
            self.assertIn("api-tok", unmasked.json()["preview"]["rendered_text"])
            self.assertEqual(unmasked.json()["preview"]["masked_variables"], [])

            partial_item = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"item_type": "release", "variables": {}},
            )
            self.assertEqual(partial_item.status_code, 422)
            self.assertEqual(partial_item.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")

            malformed_variables = client.post(
                "/v1/prompt-registry/resolve-preview",
                headers=headers,
                json={"workflow_slug": "wf-resolve", "variables": "not-an-object"},
            )
            self.assertEqual(malformed_variables.status_code, 422)
            self.assertEqual(malformed_variables.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")

            conn = dbm.connect(env)
            try:
                usage_rows = list(
                    conn.execute(
                        "SELECT event_type,status,binding_id,context_json,variables_schema_json FROM prompt_usage_events ORDER BY id ASC"
                    ).fetchall()
                )
                self.assertEqual(len(usage_rows), 5)
                self.assertTrue(all(row["event_type"] == "resolved_preview" for row in usage_rows))
                self.assertTrue(any(row["status"] == "OK" for row in usage_rows))
                self.assertTrue(any(row["binding_id"] is not None for row in usage_rows))
                self.assertFalse(any("Alice" in str(row["variables_schema_json"]) for row in usage_rows))
                self.assertFalse(any("api-tok" in str(row["variables_schema_json"]) for row in usage_rows))
                self.assertTrue(any("wf-resolve" in str(row["context_json"]) for row in usage_rows))
            finally:
                conn.close()

    def test_usage_events_and_summary_endpoints(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-usage-events",
                    "code": "PR-API-USAGE-1",
                    "title": "API Usage Events",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])
            version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "Hello {{name}}",
                    "variables": [{"name": "name", "safety_class": "standard", "required": True}],
                },
            )
            self.assertEqual(version.status_code, 200)
            version_id = int(version.json()["id"])

            self.assertEqual(
                client.post(
                    f"/v1/prompt-registry/versions/{version_id}/preview",
                    headers=headers,
                    json={"variables": {"name": "Alice"}},
                ).status_code,
                200,
            )
            self.assertEqual(
                client.post(
                    f"/v1/prompt-registry/versions/{version_id}/preview",
                    headers=headers,
                    json={"variables": {}},
                ).status_code,
                200,
            )

            listed = client.get(
                f"/v1/prompt-registry/usage-events?prompt_id={prompt_id}&version_id={version_id}&event_type=version_preview&status=INVALID&limit=50",
                headers=headers,
            )
            self.assertEqual(listed.status_code, 200)
            listed_payload = listed.json()
            self.assertEqual(len(listed_payload["items"]), 1)
            self.assertEqual(listed_payload["items"][0]["status"], "INVALID")
            self.assertIn("context", listed_payload["items"][0])
            self.assertIn("variables_schema", listed_payload["items"][0])
            self.assertNotIn("context_json", listed_payload["items"][0])
            self.assertNotIn("variables_schema_json", listed_payload["items"][0])
            self.assertNotIn("diagnostics_json", listed_payload["items"][0])

            summary = client.get(
                f"/v1/prompt-registry/usage-summary?prompt_id={prompt_id}&version_id={version_id}&event_type=version_preview",
                headers=headers,
            )
            self.assertEqual(summary.status_code, 200)
            summary_payload = summary.json()
            self.assertEqual(summary_payload["total_events"], 2)
            self.assertEqual(summary_payload["by_event_type"]["version_preview"], 2)
            self.assertEqual(summary_payload["by_status"]["OK"], 1)
            self.assertEqual(summary_payload["by_status"]["INVALID"], 1)
            self.assertEqual(summary_payload["prompt_ids"], [prompt_id])
            self.assertEqual(summary_payload["version_ids"], [version_id])

            invalid_usage_filter = client.get("/v1/prompt-registry/usage-events?event_type=bad", headers=headers)
            self.assertEqual(invalid_usage_filter.status_code, 422)
            self.assertEqual(invalid_usage_filter.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")
            invalid_limit = client.get("/v1/prompt-registry/usage-events?limit=500", headers=headers)
            self.assertEqual(invalid_limit.status_code, 422)
            self.assertEqual(invalid_limit.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")

    def test_export_and_import_endpoints_foundation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-mf5-export",
                    "code": "PR-API-MF5-1",
                    "title": "API MF5 Export",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])
            version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "Body {{name}}",
                    "variables": [{"name": "name", "safety_class": "standard", "required": True, "default_value": "Guest"}],
                },
            )
            self.assertEqual(version.status_code, 200)
            created_binding = client.post(
                "/v1/prompt-registry/bindings",
                headers=headers,
                json={
                    "prompt_id": prompt_id,
                    "binding_scope": "workflow",
                    "workflow_slug": "wf-export",
                    "binding_status": "active",
                },
            )
            self.assertEqual(created_binding.status_code, 200)

            exported = client.get(f"/v1/prompt-registry/export?prompt_id={prompt_id}", headers=headers)
            self.assertEqual(exported.status_code, 200)
            exported_payload = exported.json()
            self.assertEqual(exported_payload["schema_version"], "prompt_registry_export_v1")
            self.assertIn("records", exported_payload)
            self.assertIn("versions", exported_payload)
            self.assertIn("variables", exported_payload)
            self.assertIn("bindings", exported_payload)
            self.assertNotIn("usage_events_summary", exported_payload)

            preview = client.post(
                "/v1/prompt-registry/import/preview",
                headers=headers,
                json={"payload": exported_payload, "mode": "merge_only"},
            )
            self.assertEqual(preview.status_code, 200)
            self.assertIn(preview.json()["import_status"], ("OK", "INVALID"))
            self.assertIn("summary", preview.json())
            conn = dbm.connect(env)
            try:
                before_records = conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"]
            finally:
                conn.close()

            dry_run = client.post(
                "/v1/prompt-registry/import/confirm",
                headers=headers,
                json={"payload": exported_payload, "mode": "merge_only", "dry_run": True},
            )
            self.assertEqual(dry_run.status_code, 200)
            conn = dbm.connect(env)
            try:
                after_records = conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"]
            finally:
                conn.close()
            self.assertEqual(before_records, after_records)

            invalid_schema = client.post(
                "/v1/prompt-registry/import/preview",
                headers=headers,
                json={"payload": {**exported_payload, "schema_version": "bad"}, "mode": "merge_only"},
            )
            self.assertEqual(invalid_schema.status_code, 422)
            self.assertEqual(invalid_schema.json()["error"]["code"], "PROMPT_REGISTRY_VALIDATION_ERROR")

            duplicate_payload = {
                "schema_version": "prompt_registry_export_v1",
                "records": [
                    {
                        "slug": "dup-api",
                        "code": "PR-DUP-1",
                        "title": "Dup",
                        "record_type": "prompt_template",
                        "status": "draft",
                        "validation_status": "UNKNOWN",
                    },
                    {
                        "slug": "dup-api",
                        "code": "PR-DUP-2",
                        "title": "Dup2",
                        "record_type": "prompt_template",
                        "status": "draft",
                        "validation_status": "UNKNOWN",
                    },
                ],
                "versions": [],
                "variables": [],
                "bindings": [],
            }
            duplicate_preview = client.post(
                "/v1/prompt-registry/import/preview",
                headers=headers,
                json={"payload": duplicate_payload, "mode": "merge_only"},
            )
            self.assertEqual(duplicate_preview.status_code, 200)
            self.assertEqual(duplicate_preview.json()["import_status"], "INVALID")
            self.assertTrue(
                any("duplicate record slug" in item for item in duplicate_preview.json()["summary"]["validation_errors"])
            )
