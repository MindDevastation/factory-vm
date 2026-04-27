from __future__ import annotations

import importlib
import json
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMf10PromptRegistryExportUi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_prompt(self, client: TestClient, headers: dict[str, str], *, slug: str, code: str, title: str) -> tuple[int, int]:
        created = client.post(
            "/v1/prompt-registry/records",
            headers=headers,
            json={"slug": slug, "code": code, "title": title, "record_type": "prompt_template", "status": "draft"},
        )
        self.assertEqual(created.status_code, 200)
        prompt_id = int(created.json()["id"])
        version = client.post(
            f"/v1/prompt-registry/records/{prompt_id}/versions",
            headers=headers,
            json={
                "body_text": f"Body for {slug}: {{name}}",
                "variables": [{"name": "name", "safety_class": "standard", "required": True, "default_value": "Guest"}],
            },
        )
        self.assertEqual(version.status_code, 200)
        version_id = int(version.json()["id"])
        return prompt_id, version_id

    def test_export_page_requires_auth_and_overview_has_export_link(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.get("/ui/prompt-registry/export")
            self.assertEqual(unauthorized.status_code, 401)

            overview = client.get("/ui/prompt-registry", headers=headers)
            self.assertEqual(overview.status_code, 200)
            self.assertIn('href="/ui/prompt-registry/export"', overview.text)

            page = client.get("/ui/prompt-registry/export", headers=headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn('action="/ui/prompt-registry/export"', page.text)
            self.assertIn("include_usage (summary only)", page.text)
            self.assertIn('href="/ui/prompt-registry/import"', page.text)

    def test_export_post_no_filters_renders_counts_and_payload(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            self._seed_prompt(client, headers, slug="mf10-a", code="PR-MF10-A", title="MF10 A")

            rendered = client.post("/ui/prompt-registry/export", headers=headers, data={"include_inactive": "1"})
            self.assertEqual(rendered.status_code, 200)
            self.assertIn("schema_version:</strong> prompt_registry_export_v1", rendered.text)
            self.assertIn("records count:</strong> 1", rendered.text)
            self.assertIn("versions count:</strong> 1", rendered.text)
            self.assertIn("variables count:</strong> 1", rendered.text)
            self.assertIn("bindings count:</strong> 0", rendered.text)
            self.assertIn("usage summary included:</strong> no", rendered.text)
            self.assertIn("&#34;records&#34;: [", rendered.text)
            self.assertNotIn("Traceback", rendered.text)
            self.assertNotIn("usage_events_summary", rendered.text)

    def test_export_post_prompt_id_filters_and_invalid_prompt_id_is_safe(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            prompt_a, _ = self._seed_prompt(client, headers, slug="mf10-filter-a", code="PR-MF10-FA", title="MF10 Filter A")
            self._seed_prompt(client, headers, slug="mf10-filter-b", code="PR-MF10-FB", title="MF10 Filter B")

            filtered = client.post(
                "/ui/prompt-registry/export",
                headers=headers,
                data={"prompt_id": str(prompt_a), "include_inactive": "1"},
            )
            self.assertEqual(filtered.status_code, 200)
            self.assertIn("records count:</strong> 1", filtered.text)
            self.assertIn("mf10-filter-a", filtered.text)
            self.assertNotIn("mf10-filter-b", filtered.text)

            invalid = client.post("/ui/prompt-registry/export", headers=headers, data={"prompt_id": "not-an-int"})
            self.assertEqual(invalid.status_code, 200)
            self.assertIn("Export error:</strong> prompt_id must be an integer", invalid.text)
            self.assertNotIn("Traceback", invalid.text)

    def test_export_download_returns_attachment_json(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            self._seed_prompt(client, headers, slug="mf10-download", code="PR-MF10-DL", title="MF10 Download")

            download = client.post("/ui/prompt-registry/export/download", headers=headers, data={"include_inactive": "1"})
            self.assertEqual(download.status_code, 200)
            self.assertEqual(download.headers["content-disposition"], 'attachment; filename="prompt-registry-export.json"')
            self.assertTrue(download.headers["content-type"].startswith("application/json"))
            self.assertNotIn("prompt_id=", str(download.request.url))
            payload = json.loads(download.text)
            self.assertEqual(payload["schema_version"], "prompt_registry_export_v1")

    def test_include_usage_summary_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            _prompt_id, version_id = self._seed_prompt(client, headers, slug="mf10-usage", code="PR-MF10-USAGE", title="MF10 Usage")
            preview = client.post(
                f"/v1/prompt-registry/versions/{version_id}/preview",
                headers=headers,
                json={"variables": {"name": "Operator"}},
            )
            self.assertEqual(preview.status_code, 200)

            no_usage = client.post("/ui/prompt-registry/export", headers=headers, data={"include_inactive": "1"})
            self.assertEqual(no_usage.status_code, 200)
            self.assertIn("usage summary included:</strong> no", no_usage.text)
            self.assertNotIn("usage_events_summary", no_usage.text)

            with_usage = client.post(
                "/ui/prompt-registry/export",
                headers=headers,
                data={"include_inactive": "1", "include_usage": "1"},
            )
            self.assertEqual(with_usage.status_code, 200)
            self.assertIn("usage summary included:</strong> yes", with_usage.text)
            self.assertIn("&#34;usage_events_summary&#34;", with_usage.text)
            self.assertIn("&#34;total_events&#34;", with_usage.text)
            self.assertIn("&#34;by_event_type&#34;", with_usage.text)
            self.assertIn("&#34;by_status&#34;", with_usage.text)
            self.assertNotIn("&#34;events&#34;", with_usage.text)
            self.assertNotIn("Traceback", with_usage.text)


if __name__ == "__main__":
    unittest.main()
