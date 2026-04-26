from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf6PromptRegistryUiFoundation(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_prompt(self, client: TestClient, headers: dict[str, str], *, slug: str, code: str, title: str) -> tuple[int, int]:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={
                "slug": slug,
                "code": code,
                "title": title,
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
                "body_text": "Hello {{name}} / {{api_key}}",
                "variables": [
                    {"name": "name", "safety_class": "standard", "required": True},
                    {"name": "api_key", "safety_class": "secret", "required": False, "default_value": "token-123"},
                ],
            },
        )
        self.assertEqual(version.status_code, 200)
        version_id = int(version.json()["id"])

        binding = client.post(
            "/v1/prompt-registry/bindings",
            headers=headers,
            json={
                "prompt_id": prompt_id,
                "binding_scope": "global",
                "binding_status": "active",
            },
        )
        self.assertEqual(binding.status_code, 200)

        preview = client.post(
            f"/v1/prompt-registry/versions/{version_id}/preview",
            headers=headers,
            json={"variables": {"name": "Alice"}, "mask_sensitive": True},
        )
        self.assertEqual(preview.status_code, 200)
        return prompt_id, version_id

    def test_overview_detail_preview_and_auth(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id, version_id = self._seed_prompt(
                client,
                headers,
                slug="mf6-ui",
                code="PR-MF6-UI",
                title="MF6 UI Prompt",
            )

            unauthorized = client.get("/ui/prompt-registry")
            self.assertEqual(unauthorized.status_code, 401)

            overview = client.get("/ui/prompt-registry", headers=headers)
            self.assertEqual(overview.status_code, 200)
            self.assertIn("Prompt Registry", overview.text)
            self.assertIn("MF6 UI Prompt", overview.text)
            self.assertIn("versions=1", overview.text)
            self.assertIn("bindings=1", overview.text)
            self.assertIn("usage_events=1", overview.text)
            self.assertIn('href="/v1/prompt-registry/export"', overview.text)
            self.assertIn("/v1/prompt-registry/import/preview", overview.text)
            self.assertIn("/v1/prompt-registry/import/confirm", overview.text)

            detail = client.get(f"/ui/prompt-registry/{prompt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn(f"Record #{prompt_id}", detail.text)
            self.assertIn("Usage summary", detail.text)
            self.assertIn("Bindings", detail.text)
            self.assertIn("Audit diagnostics", detail.text)
            self.assertIn("***MASKED***", detail.text)

            self.assertIn('method="post"', detail.text)
            self.assertIn(f'action="/ui/prompt-registry/{prompt_id}/preview"', detail.text)
            self.assertNotIn('method="get" action="/ui/prompt-registry/', detail.text)
            self.assertIn("Sensitive variables are always masked in UI preview.", detail.text)
            self.assertNotIn("mask_sensitive", detail.text)

            valid_preview = client.post(
                f"/ui/prompt-registry/{prompt_id}/preview",
                headers=headers,
                data={
                    "version_id": str(version_id),
                    "variables_json": '{"name": "Bob"}',
                },
            )
            self.assertEqual(valid_preview.status_code, 200)
            self.assertIn("preview_status:</strong> OK", valid_preview.text)
            self.assertIn("Hello Bob / ***MASKED***", valid_preview.text)

            invalid_preview = client.post(
                f"/ui/prompt-registry/{prompt_id}/preview",
                headers=headers,
                data={
                    "version_id": str(version_id),
                    "variables_json": "{}",
                },
            )
            self.assertEqual(invalid_preview.status_code, 200)
            self.assertIn("preview_status:</strong> INVALID", invalid_preview.text)
            self.assertIn("missing_required", invalid_preview.text)

            forced_unmask = client.post(
                f"/ui/prompt-registry/{prompt_id}/preview",
                headers=headers,
                data={
                    "version_id": str(version_id),
                    "variables_json": '{"name": "Bob", "api_key": "RAW-SECRET"}',
                    "mask_sensitive": "false",
                },
            )
            self.assertEqual(forced_unmask.status_code, 200)
            self.assertIn("***MASKED***", forced_unmask.text)
            self.assertNotIn("RAW-SECRET", forced_unmask.text)

    def test_cross_prompt_version_id_is_rejected_without_usage_event(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_a_id, _ = self._seed_prompt(
                client,
                headers,
                slug="mf6-ui-a",
                code="PR-MF6-UI-A",
                title="MF6 UI Prompt A",
            )
            prompt_b_id, version_b_id = self._seed_prompt(
                client,
                headers,
                slug="mf6-ui-b",
                code="PR-MF6-UI-B",
                title="MF6 UI Prompt B",
            )

            usage_before = client.get(
                f"/v1/prompt-registry/usage-summary?prompt_id={prompt_b_id}&version_id={version_b_id}&event_type=version_preview",
                headers=headers,
            )
            self.assertEqual(usage_before.status_code, 200)
            before_total = int(usage_before.json()["total_events"])

            rejected = client.post(
                f"/ui/prompt-registry/{prompt_a_id}/preview",
                headers=headers,
                data={
                    "version_id": str(version_b_id),
                    "variables_json": '{"name": "CrossPrompt"}',
                },
            )
            self.assertEqual(rejected.status_code, 200)
            self.assertIn("Preview error:</strong> version_id does not belong to the current prompt", rejected.text)
            self.assertNotIn("MF6 UI Prompt B", rejected.text)
            self.assertNotIn("Hello CrossPrompt", rejected.text)

            usage_after = client.get(
                f"/v1/prompt-registry/usage-summary?prompt_id={prompt_b_id}&version_id={version_b_id}&event_type=version_preview",
                headers=headers,
            )
            self.assertEqual(usage_after.status_code, 200)
            after_total = int(usage_after.json()["total_events"])
            self.assertEqual(after_total, before_total)


if __name__ == "__main__":
    unittest.main()
