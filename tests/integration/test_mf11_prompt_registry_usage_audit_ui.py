from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf11PromptRegistryUsageAuditUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _create_record(self, client: TestClient, headers: dict[str, str], *, slug: str, code: str, title: str) -> int:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={"slug": slug, "code": code, "title": title, "record_type": "prompt_template", "status": "draft"},
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def _create_version(self, client: TestClient, headers: dict[str, str], *, prompt_id: int, variable_name: str = "name") -> int:
        created = client.post(
            f"/v1/prompt-registry/records/{prompt_id}/versions",
            headers=headers,
            json={
                "body_text": f"Hello {{{{{variable_name}}}}}",
                "variables": [{"name": variable_name, "safety_class": "standard", "required": True, "default_value": "Guest"}],
            },
        )
        self.assertEqual(created.status_code, 200)
        return int(created.json()["id"])

    def test_usage_page_requires_auth_and_renders_usage_events(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf11-usage-a", code="PR-MF11-UA", title="MF11 Usage A")
            version_id = self._create_version(client, headers, prompt_id=prompt_id)

            unauthorized = client.get("/ui/prompt-registry/usage")
            self.assertEqual(unauthorized.status_code, 401)

            preview = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {"name": "Operator"}},
            )
            self.assertEqual(preview.status_code, 200)

            rendered = client.get("/ui/prompt-registry/usage", headers=headers)
            self.assertEqual(rendered.status_code, 200)
            self.assertIn("Usage diagnostics", rendered.text)
            self.assertIn("version_preview", rendered.text)
            self.assertIn(">api<", rendered.text)
            self.assertNotIn("Traceback", rendered.text)

    def test_usage_prompt_filter_works_and_invalid_filter_is_safe(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            prompt_a = self._create_record(client, headers, slug="mf11-filter-a", code="PR-MF11-FA", title="MF11 Filter A")
            version_a = self._create_version(client, headers, prompt_id=prompt_a)
            prompt_b = self._create_record(client, headers, slug="mf11-filter-b", code="PR-MF11-FB", title="MF11 Filter B")
            version_b = self._create_version(client, headers, prompt_id=prompt_b)

            self.assertEqual(
                client.post(f"/v1/prompt-registry/versions/{version_a}/preview", headers=headers, json={"variables": {"name": "A"}}).status_code,
                200,
            )
            self.assertEqual(
                client.post(f"/v1/prompt-registry/versions/{version_b}/preview", headers=headers, json={"variables": {"name": "B"}}).status_code,
                200,
            )

            filtered = client.get(f"/ui/prompt-registry/usage?prompt_id={prompt_a}", headers=headers)
            self.assertEqual(filtered.status_code, 200)
            self.assertIn(f">{prompt_a}<", filtered.text)
            self.assertNotIn(f">{prompt_b}<", filtered.text)

            invalid = client.get("/ui/prompt-registry/usage?limit=not-a-number", headers=headers)
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Usage diagnostics error:</strong> limit must be an integer", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_usage_page_does_not_dump_raw_usage_json(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf11-compact", code="PR-MF11-CP", title="MF11 Compact")
            version_id = self._create_version(client, headers, prompt_id=prompt_id)

            preview = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {"name": "Compact"}},
            )
            self.assertEqual(preview.status_code, 200)

            page = client.get("/ui/prompt-registry/usage", headers=headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Context summary", page.text)
            self.assertIn("Diagnostics summary", page.text)
            self.assertNotIn("preview_diagnostics", page.text)
            self.assertNotIn("textarea", page.text)

    def test_detail_contains_usage_and_audit_links(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf11-links", code="PR-MF11-LINKS", title="MF11 Links")

            detail = client.get(f"/ui/prompt-registry/{prompt_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertIn(f'href="/ui/prompt-registry/usage?prompt_id={prompt_id}"', detail.text)
            self.assertIn(f'href="/ui/prompt-registry/{prompt_id}/audit"', detail.text)
            self.assertIn("Usage summary", detail.text)

            overview = client.get("/ui/prompt-registry", headers=headers)
            self.assertEqual(overview.status_code, 200)
            self.assertIn('href="/ui/prompt-registry/usage"', overview.text)

    def test_audit_page_requires_auth_and_renders_events(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_id = self._create_record(client, headers, slug="mf11-audit", code="PR-MF11-AUD", title="MF11 Audit")
            self._create_version(client, headers, prompt_id=prompt_id)

            created_binding = client.post(
                f"/ui/prompt-registry/{prompt_id}/bindings/create",
                headers=headers,
                data={"binding_scope": "global", "binding_status": "active"},
                follow_redirects=False,
            )
            self.assertEqual(created_binding.status_code, 303)

            unauthorized = client.get(f"/ui/prompt-registry/{prompt_id}/audit")
            self.assertEqual(unauthorized.status_code, 401)

            audit = client.get(f"/ui/prompt-registry/{prompt_id}/audit", headers=headers)
            self.assertEqual(audit.status_code, 200)
            self.assertIn("Audit timeline", audit.text)
            self.assertIn("record_created", audit.text)
            self.assertIn("version_created", audit.text)
            self.assertIn("binding_created", audit.text)
            self.assertIn("Payload summary", audit.text)
            self.assertNotIn("Traceback", audit.text)


if __name__ == "__main__":
    unittest.main()
