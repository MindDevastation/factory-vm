from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataVideoTagsGenApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_release(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        planned_at: str | None = "2026-04-09T12:00:00Z",
        title: str = "Night Ritual",
        tags_json: str = '["ambient", "night"]',
    ):
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, "desc", tags_json, planned_at, "f", f"meta-vtags-{channel_slug}-{title}", dbm.now_ts()),
        )
        return int(cur.lastrowid)

    def _seed_preset(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        is_default: bool = True,
        status: str = "ACTIVE",
        validation_status: str = "VALID",
        body: list[str] | None = None,
        name: str = "preset",
    ) -> int:
        return dbm.create_video_tag_preset(
            conn,
            channel_slug=channel_slug,
            preset_name=name,
            preset_body_json=dbm.json_dumps(body or ["{{channel_display_name}}", "{{release_title}}", "ambient", "ambient"]),
            status=status,
            is_default=is_default,
            validation_status=validation_status,
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            archived_at=None,
        )

    def test_context_endpoint_with_default_present(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                default_id = self._seed_preset(conn, is_default=True, name="default")
                self._seed_preset(conn, is_default=False, name="alt")
                self._seed_preset(conn, status="ARCHIVED", is_default=False, name="old")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/metadata/releases/{release_id}/video-tags/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["release_id"], release_id)
            self.assertEqual(body["default_preset"]["id"], default_id)
            self.assertEqual(sorted(body["default_preset"].keys()), ["id", "is_default", "preset_name", "status"])
            self.assertEqual(len(body["active_presets"]), 2)
            self.assertTrue(body["can_generate_with_default"])

    def test_context_endpoint_without_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/metadata/releases/{release_id}/video-tags/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertIsNone(resp.json()["default_preset"])
            self.assertFalse(resp.json()["can_generate_with_default"])

    def test_context_endpoint_invalid_default_not_advertised_and_unusable_excluded(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                valid_id = self._seed_preset(conn, is_default=False, validation_status="VALID", name="valid")
                self._seed_preset(conn, is_default=True, validation_status="INVALID", name="invalid-default")
                self._seed_preset(conn, is_default=False, status="ARCHIVED", validation_status="VALID", name="archived")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/metadata/releases/{release_id}/video-tags/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIsNone(body["default_preset"])
            self.assertFalse(body["can_generate_with_default"])
            self.assertEqual([item["id"] for item in body["active_presets"]], [valid_id])

    def test_generate_with_default_preset(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                preset_id = self._seed_preset(conn, is_default=True)
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["used_preset"]["id"], preset_id)
            self.assertEqual(body["proposed_tags_json"], ["Darkwood Reverie", "Night Ritual", "ambient"])
            self.assertEqual(body["removed_duplicates"], ["ambient"])

    def test_generate_with_explicit_override_preset(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                explicit = self._seed_preset(conn, is_default=False, body=["{{channel_slug}}"])
                mismatch = self._seed_preset(conn, channel_slug="channel-b", is_default=False)
                archived = self._seed_preset(conn, is_default=False, status="ARCHIVED")
                invalid = self._seed_preset(conn, is_default=False, validation_status="INVALID")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            ok = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={"preset_id": explicit})
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.json()["used_preset"]["id"], explicit)

            bad_mismatch = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={"preset_id": mismatch})
            self.assertEqual(bad_mismatch.status_code, 422)
            self.assertEqual(bad_mismatch.json()["error"]["code"], "MTV_PRESET_CHANNEL_MISMATCH")

            bad_archived = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={"preset_id": archived})
            self.assertEqual(bad_archived.status_code, 422)
            self.assertEqual(bad_archived.json()["error"]["code"], "MTV_PRESET_NOT_ACTIVE")

            bad_invalid = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={"preset_id": invalid})
            self.assertEqual(bad_invalid.status_code, 422)
            self.assertEqual(bad_invalid.json()["error"]["code"], "MTV_PRESET_INVALID")

    def test_generate_no_default_requires_explicit_preset(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, channel_slug="channel-b")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTV_DEFAULT_PRESET_NOT_CONFIGURED")

    def test_generate_missing_title_and_date_context_errors(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                missing_title = self._seed_release(conn, title="   ")
                self._seed_preset(conn, is_default=True, body=["{{release_title}}"], name="title")
                missing_schedule = self._seed_release(conn, title="ok", planned_at=None)
                date_preset = self._seed_preset(conn, is_default=False, body=["{{release_year}}"], name="date")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            title_resp = client.post(f"/v1/metadata/releases/{missing_title}/video-tags/generate", headers=headers, json={})
            self.assertEqual(title_resp.status_code, 422)
            self.assertEqual(title_resp.json()["error"]["code"], "MTV_RELEASE_TITLE_NOT_USABLE")

            date_resp = client.post(
                f"/v1/metadata/releases/{missing_schedule}/video-tags/generate",
                headers=headers,
                json={"preset_id": date_preset},
            )
            self.assertEqual(date_resp.status_code, 422)
            self.assertEqual(date_resp.json()["error"]["code"], "MTV_RELEASE_DATE_CONTEXT_MISSING")

    def test_generate_maps_render_time_validation_failure_to_preset_invalid(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="ok")
                self._seed_preset(conn, is_default=True, body=["x" * 501], validation_status="VALID")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTV_PRESET_INVALID")

    def test_generate_does_not_mutate_release_tags_json(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, tags_json='["ambient", "night"]')
                self._seed_preset(conn, is_default=True)
                before = conn.execute("SELECT tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()["tags_json"]
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=headers, json={})
            self.assertEqual(generated.status_code, 200)
            self.assertTrue(generated.json()["overwrite_required"])
            self.assertTrue(generated.json()["warnings"])

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()["tags_json"]
            finally:
                conn.close()
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
