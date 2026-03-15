from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataTitleTemplateApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_variables_returns_whitelist_catalog(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/metadata/title-templates/variables", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            names = [item["name"] for item in body["variables"]]
            self.assertEqual(
                names,
                [
                    "channel_display_name",
                    "channel_slug",
                    "channel_kind",
                    "release_year",
                    "release_month_number",
                    "release_day_number",
                ],
            )

    def test_preview_full_render(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name}} — {{release_year}}-{{release_month_number}}-{{release_day_number}}",
                    "release_date": "2026-04-09",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "FULL")
            self.assertEqual(body["rendered_title"], "Darkwood Reverie — 2026-04-09")
            self.assertEqual(body["missing_variables"], [])

    def test_preview_partial_render_with_missing_variables(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name}} - {{release_year}}",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "PARTIAL")
            self.assertEqual(body["missing_variables"], ["release_year"])
            self.assertIn("<<missing:release_year>>", body["rendered_title"])

    def test_preview_channel_not_found(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "missing-channel",
                    "template_body": "{{channel_display_name}}",
                },
            )
            self.assertEqual(resp.status_code, 404)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "MTB_CHANNEL_NOT_FOUND")

    def test_preview_invalid_syntax_returns_error_payload(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "ERROR")
            self.assertTrue(any(item["code"] == "MTB_TEMPLATE_SYNTAX" for item in body["validation_errors"]))

    def test_preview_empty_template_body_returns_mtb_title_empty(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "ERROR")
            self.assertTrue(any(item["code"] == "MTB_TITLE_EMPTY" for item in body["validation_errors"]))


if __name__ == "__main__":
    unittest.main()
