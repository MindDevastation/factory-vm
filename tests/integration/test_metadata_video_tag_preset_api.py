from __future__ import annotations

import importlib
import time
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataVideoTagPresetApi(unittest.TestCase):
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

            resp = client.get("/v1/metadata/video-tag-presets/variables", headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(
                [item["name"] for item in resp.json()["variables"]],
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

    def test_preview_full_render_for_channel_only_preset(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "preset_body": ["{{channel_display_name}}", "ambient"],
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "FULL")
            self.assertEqual(body["final_normalized_tags"], ["Darkwood Reverie", "ambient"])

    def test_preview_partial_render_with_missing_markers(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "preset_body": ["{{release_title}}", "{{release_year}}", "ambient"],
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "PARTIAL")
            self.assertIn("release_title", body["missing_variables"])
            self.assertIn("<<missing:release_title>>", body["rendered_items_before_normalization"])

    def test_preview_reports_dropped_empty_and_removed_duplicates(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "preset_body": ["ambient", "  ", "ambient"],
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["dropped_empty_items"], ["  "])
            self.assertEqual(body["removed_duplicates"], ["ambient"])

    def test_release_id_with_usable_context_returns_full(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            release_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                title="Night Ritual",
                planned_at="2026-04-09T03:00:00+00:00",
            )
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "release_id": release_id,
                    "preset_body": ["{{release_title}}", "{{release_year}}", "ambient"],
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["render_status"], "FULL")
            self.assertEqual(body["final_normalized_tags"], ["Night Ritual", "2026", "ambient"])

    def test_release_not_found_returns_repo_aligned_error(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "release_id": 999999,
                    "preset_body": ["{{release_title}}"],
                },
            )
            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "MTV_RELEASE_NOT_FOUND")

    def test_channel_not_found_returns_repo_aligned_error(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets/preview",
                headers=headers,
                json={
                    "channel_slug": "missing-channel",
                    "preset_body": ["ambient"],
                },
            )
            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "MTV_CHANNEL_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
