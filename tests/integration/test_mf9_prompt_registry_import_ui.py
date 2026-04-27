from __future__ import annotations

import importlib
import json
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf9PromptRegistryImportUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_exportable_payload(self, client: TestClient, headers: dict[str, str]) -> dict:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={
                "slug": "mf9-import-source",
                "code": "PR-MF9-SOURCE",
                "title": "MF9 Source",
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

        binding = client.post(
            "/v1/prompt-registry/bindings",
            headers=headers,
            json={
                "prompt_id": prompt_id,
                "binding_scope": "workflow",
                "workflow_slug": "wf-mf9",
                "binding_status": "active",
            },
        )
        self.assertEqual(binding.status_code, 200)

        exported = client.get(f"/v1/prompt-registry/export?prompt_id={prompt_id}", headers=headers)
        self.assertEqual(exported.status_code, 200)
        return exported.json()

    def _counts(self, env: Env) -> tuple[int, int, int]:
        conn = dbm.connect(env)
        try:
            records = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_records").fetchone()["c"])
            versions = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_versions").fetchone()["c"])
            bindings = int(conn.execute("SELECT COUNT(*) AS c FROM prompt_bindings").fetchone()["c"])
            return records, versions, bindings
        finally:
            conn.close()

    def test_import_page_auth_and_overview_link(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.get("/ui/prompt-registry/import")
            self.assertEqual(unauthorized.status_code, 401)

            overview = client.get("/ui/prompt-registry", headers=headers)
            self.assertEqual(overview.status_code, 200)
            self.assertIn('href="/ui/prompt-registry/import"', overview.text)

            page = client.get("/ui/prompt-registry/import", headers=headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn('action="/ui/prompt-registry/import/preview"', page.text)
            self.assertIn('action="/ui/prompt-registry/import/confirm"', page.text)
            self.assertIn("merge_only", page.text)
            self.assertIn("Destructive replace/delete is not available", page.text)
            self.assertIn('href="/v1/prompt-registry/export"', page.text)

    def test_import_preview_valid_payload_is_safe_and_non_destructive(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            payload = self._seed_exportable_payload(client, headers)
            before = self._counts(env)

            preview = client.post(
                "/ui/prompt-registry/import/preview",
                headers=headers,
                data={"payload_json": json.dumps(payload)},
            )
            self.assertEqual(preview.status_code, 200)
            self.assertIn("import_status:</strong>", preview.text)
            self.assertIn("records_to_create:</strong>", preview.text)
            self.assertIn("records_to_update:</strong>", preview.text)
            self.assertIn("versions_to_create:</strong>", preview.text)
            self.assertIn("bindings_to_create:</strong>", preview.text)

            after = self._counts(env)
            self.assertEqual(before, after)
            self.assertNotIn("Traceback", preview.text)

    def test_import_preview_malformed_json_safe_error_and_no_traceback(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            bad = client.post(
                "/ui/prompt-registry/import/preview",
                headers=headers,
                data={"payload_json": "{bad json"},
            )
            self.assertEqual(bad.status_code, 200)
            self.assertIn("Import error:</strong> Import payload must be valid JSON", bad.text)
            self.assertNotIn("Traceback", bad.text)

    def test_import_confirm_dry_run_true_non_destructive(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            payload = self._seed_exportable_payload(client, headers)
            before = self._counts(env)

            dry_run = client.post(
                "/ui/prompt-registry/import/confirm",
                headers=headers,
                data={"payload_json": json.dumps(payload), "dry_run": "true"},
            )
            self.assertEqual(dry_run.status_code, 200)
            self.assertIn("import_status:</strong>", dry_run.text)

            after = self._counts(env)
            self.assertEqual(before, after)
            self.assertNotIn("Traceback", dry_run.text)

    def test_import_confirm_dry_run_false_writes_expected_entities(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            source_client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            payload = self._seed_exportable_payload(source_client, headers)

        with temp_env() as (_td_target, target_env):
            seed_minimal_db(target_env)
            target_client = self._client(target_env)
            target_headers = basic_auth_header(target_env.basic_user, target_env.basic_pass)
            before = self._counts(target_env)

            confirmed = target_client.post(
                "/ui/prompt-registry/import/confirm",
                headers=target_headers,
                data={"payload_json": json.dumps(payload), "dry_run": "false"},
            )
            self.assertEqual(confirmed.status_code, 200)
            self.assertIn("Success:</strong> Import applied in merge_only mode", confirmed.text)
            self.assertIn("import_status:</strong> OK", confirmed.text)

            after = self._counts(target_env)
            self.assertEqual(after[0], before[0] + 1)
            self.assertEqual(after[1], before[1] + 1)
            self.assertEqual(after[2], before[2] + 1)

    def test_import_confirm_invalid_payload_safe_error_no_traceback(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            invalid = client.post(
                "/ui/prompt-registry/import/confirm",
                headers=headers,
                data={"payload_json": '{"schema_version":"bad"}', "dry_run": "false"},
            )
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Import error:</strong> Import confirm failed validation", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_import_payload_not_in_query_string(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            marker = "SECRET-MF9-MARKER"

            response = client.post(
                "/ui/prompt-registry/import/preview",
                headers=headers,
                data={"payload_json": '{"schema_version":"prompt_registry_export_v1","records":[],"versions":[],"variables":[],"bindings":[],"note":"SECRET-MF9-MARKER"}'},
            )
            self.assertEqual(response.status_code, 200)
            self.assertNotIn("payload_json", str(response.request.url))
            self.assertNotIn(marker, str(response.request.url))
            self.assertIn(marker, response.text)


if __name__ == "__main__":
    unittest.main()
