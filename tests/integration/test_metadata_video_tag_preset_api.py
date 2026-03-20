from __future__ import annotations

import importlib
import json
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

    def _create_preset(
        self,
        client: TestClient,
        headers: dict[str, str],
        *,
        channel_slug: str = "darkwood-reverie",
        preset_name: str = "Main tag preset",
        preset_body: list[str] | None = None,
        make_default: bool = False,
    ) -> dict:
        payload = {
            "channel_slug": channel_slug,
            "preset_name": preset_name,
            "preset_body": preset_body or ["{{channel_display_name}}", "{{release_year}}", "ambient"],
            "make_default": make_default,
        }
        resp = client.post("/v1/metadata/video-tag-presets", headers=headers, json=payload)
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

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

    def test_create_valid_preset(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            created = self._create_preset(client, headers, make_default=False)
            self.assertEqual(created["status"], "ACTIVE")
            self.assertFalse(created["is_default"])
            self.assertEqual(created["validation_status"], "VALID")
            self.assertEqual(created["validation_errors"], [])
            self.assertIsNotNone(created["last_validated_at"])

    def test_list_filters_by_channel_status_and_q(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            first = self._create_preset(client, headers, preset_name="Main tag preset")
            second = self._create_preset(client, headers, preset_name="Alt ambient pack")
            self.assertEqual(first["channel_slug"], second["channel_slug"])
            client.post(f"/v1/metadata/video-tag-presets/{second['id']}/archive", headers=headers)

            active = client.get(
                "/v1/metadata/video-tag-presets",
                headers=headers,
                params={"channel_slug": "darkwood-reverie", "status": "active", "q": "Main"},
            )
            self.assertEqual(active.status_code, 200)
            active_ids = [item["id"] for item in active.json()["items"]]
            self.assertIn(first["id"], active_ids)
            self.assertNotIn(second["id"], active_ids)

            archived = client.get(
                "/v1/metadata/video-tag-presets",
                headers=headers,
                params={"channel_slug": "darkwood-reverie", "status": "archived", "q": "ambient"},
            )
            self.assertEqual(archived.status_code, 200)
            archived_ids = [item["id"] for item in archived.json()["items"]]
            self.assertIn(second["id"], archived_ids)

    def test_details_returns_body_and_validation_metadata(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = self._create_preset(client, headers)

            detail = client.get(f"/v1/metadata/video-tag-presets/{created['id']}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            body = detail.json()
            self.assertEqual(body["id"], created["id"])
            self.assertIsInstance(body["preset_body"], list)
            self.assertEqual(body["validation_status"], "VALID")
            self.assertEqual(body["validation_errors"], [])
            self.assertIsNotNone(body["last_validated_at"])

    def test_patch_updates_same_entity_in_place(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = self._create_preset(client, headers, preset_name="Old")

            patched = client.patch(
                f"/v1/metadata/video-tag-presets/{created['id']}",
                headers=headers,
                json={"preset_name": "New", "preset_body": ["{{channel_display_name}}", "drone"]},
            )
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(patched.json()["id"], created["id"])
            self.assertEqual(patched.json()["preset_name"], "New")
            self.assertEqual(patched.json()["preset_body"], ["{{channel_display_name}}", "drone"])

    def test_invalid_preset_save_rejected(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "preset_name": "  ",
                    "preset_body": ["{{unknown_var}}"],
                    "make_default": False,
                },
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTV_PRESET_NAME_REQUIRED")

    def test_create_rejects_oversize_release_title_estimate(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/video-tag-presets",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "preset_name": "Too Long",
                    "preset_body": ["{{release_title}}"],
                    "make_default": False,
                },
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTV_TAG_ITEM_TOO_LONG")

    def test_set_default_enforces_single_default_invariant(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            first = self._create_preset(client, headers, preset_name="First", make_default=True)
            second = self._create_preset(client, headers, preset_name="Second", make_default=False)

            switched = client.post(f"/v1/metadata/video-tag-presets/{second['id']}/set-default", headers=headers)
            self.assertEqual(switched.status_code, 200)
            self.assertTrue(switched.json()["is_default"])

            conn = dbm.connect(env)
            try:
                count_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM video_tag_presets WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1",
                    ("darkwood-reverie",),
                ).fetchone()
            finally:
                conn.close()
            assert count_row is not None
            self.assertEqual(int(count_row["c"]), 1)
            first_after = client.get(f"/v1/metadata/video-tag-presets/{first['id']}", headers=headers).json()
            self.assertFalse(first_after["is_default"])

    def test_archive_removes_default_flag(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = self._create_preset(client, headers, make_default=True)

            archived = client.post(f"/v1/metadata/video-tag-presets/{created['id']}/archive", headers=headers)
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(archived.json()["status"], "ARCHIVED")
            self.assertFalse(archived.json()["is_default"])

    def test_activate_restores_active_but_not_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = self._create_preset(client, headers, make_default=True)
            client.post(f"/v1/metadata/video-tag-presets/{created['id']}/archive", headers=headers)

            activated = client.post(f"/v1/metadata/video-tag-presets/{created['id']}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)
            self.assertEqual(activated.json()["status"], "ACTIVE")
            self.assertFalse(activated.json()["is_default"])

    def test_already_default_set_default_is_safe_no_op(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = self._create_preset(client, headers, make_default=True)

            again = client.post(f"/v1/metadata/video-tag-presets/{created['id']}/set-default", headers=headers)
            self.assertEqual(again.status_code, 200)
            self.assertTrue(again.json()["is_default"])

    def test_archive_already_archived_is_safe_consistent(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = self._create_preset(client, headers)

            first = client.post(f"/v1/metadata/video-tag-presets/{created['id']}/archive", headers=headers)
            second = client.post(f"/v1/metadata/video-tag-presets/{created['id']}/archive", headers=headers)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(second.json()["status"], "ARCHIVED")
            self.assertFalse(second.json()["is_default"])

    def test_lifecycle_endpoints_do_not_mutate_release_tags_json(self) -> None:
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
            created = self._create_preset(client, headers, make_default=False)

            conn = dbm.connect(env)
            try:
                before = conn.execute("SELECT tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            assert before is not None

            client.patch(
                f"/v1/metadata/video-tag-presets/{created['id']}",
                headers=headers,
                json={"preset_name": "Edited"},
            )
            client.post(f"/v1/metadata/video-tag-presets/{created['id']}/set-default", headers=headers)
            client.post(f"/v1/metadata/video-tag-presets/{created['id']}/archive", headers=headers)
            client.post(f"/v1/metadata/video-tag-presets/{created['id']}/activate", headers=headers)

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            assert after is not None
            self.assertEqual(json.loads(str(before["tags_json"])), json.loads(str(after["tags_json"])))


if __name__ == "__main__":
    unittest.main()
