from __future__ import annotations

import importlib
import time
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataDescriptionTemplateApi(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _insert_release(self, env, *, channel_slug: str, title: str, planned_at: str | None) -> int:
        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, channel_slug)
            assert channel is not None
            cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(channel["id"]),
                    title,
                    "desc",
                    "[]",
                    planned_at,
                    None,
                    f"meta_{time.time_ns()}",
                    dbm.now_ts(),
                ),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_variables_returns_whitelist_catalog(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/metadata/description-templates/variables", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            names = [item["name"] for item in body["variables"]]
            self.assertEqual(
                names,
                [
                    "channel_display_name",
                    "channel_slug",
                    "channel_kind",
                    "release_title",
                    "release_year",
                    "release_month_number",
                    "release_day_number",
                ],
            )

    def test_preview_full_render_for_channel_only_template(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/description-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name}}\\n\\nKind: {{channel_kind}}",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertTrue(body["syntax_valid"])
            self.assertTrue(body["structurally_valid"])
            self.assertEqual(body["render_status"], "FULL")
            self.assertEqual(body["missing_variables"], [])
            self.assertEqual(body["rendered_description_preview"], "Darkwood Reverie\\n\\nKind: LONG")

    def test_preview_partial_with_explicit_missing_markers(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/description-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name}}\\n\\n{{release_title}}",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "PARTIAL")
            self.assertEqual(body["missing_variables"], ["release_title"])
            self.assertIn("<<missing:release_title>>", body["rendered_description_preview"])

    def test_release_id_present_with_usable_context_returns_full(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            release_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                title="March Night Session",
                planned_at="2026-04-09T03:00:00+00:00",
            )
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/description-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "release_id": release_id,
                    "template_body": "{{release_title}}\\nReleased: {{release_year}}-{{release_month_number}}-{{release_day_number}}",
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "FULL")
            self.assertEqual(body["missing_variables"], [])
            self.assertEqual(body["rendered_description_preview"], "March Night Session\\nReleased: 2026-04-09")

    def test_release_not_found_returns_repo_aligned_error(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/description-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "release_id": 999999,
                    "template_body": "{{release_title}}",
                },
            )
            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "MTD_RELEASE_NOT_FOUND")

    def test_channel_not_found_returns_repo_aligned_error(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/description-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "unknown-channel",
                    "template_body": "{{channel_display_name}}",
                },
            )
            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "MTD_CHANNEL_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
