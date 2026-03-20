from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataPreviewApplyApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_release(self, conn, *, title: str = "t", description: str = "d", tags_json: str = '["a"]', planned_at: str | None = "2026-04-09T12:00:00Z") -> int:
        ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, description, tags_json, planned_at, "f", f"meta-{dbm.now_ts()}", dbm.now_ts()),
        )
        return int(cur.lastrowid)

    def _seed_defaults(self, conn) -> tuple[int, int, int]:
        t = dbm.create_title_template(
            conn,
            channel_slug="darkwood-reverie",
            template_name="t",
            template_body="{{channel_display_name}}",
            status="ACTIVE",
            is_default=True,
            validation_status="VALID",
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            archived_at=None,
        )
        d = dbm.create_description_template(
            conn,
            channel_slug="darkwood-reverie",
            template_name="d",
            template_body="{{channel_display_name}}",
            status="ACTIVE",
            is_default=True,
            validation_status="VALID",
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            archived_at=None,
        )
        p = dbm.create_video_tag_preset(
            conn,
            channel_slug="darkwood-reverie",
            preset_name="p",
            preset_body_json='["{{channel_slug}}"]',
            status="ACTIVE",
            is_default=True,
            validation_status="VALID",
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            archived_at=None,
        )
        return t, d, p

    def test_context_endpoint_returns_current_defaults_and_active(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="arch",
                    template_body="x",
                    status="ARCHIVED",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at="2026-01-01T00:00:00+00:00",
                )
                dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="invalid",
                    template_body="x",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="INVALID",
                    validation_errors_json='[{"code":"bad"}]',
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/metadata/releases/{release_id}/preview-apply/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(set(body.keys()), {"release_id", "channel_slug", "current", "defaults", "active_sources"})
            self.assertEqual(body["current"]["title"], "t")
            self.assertTrue(all(item["status"] == "ACTIVE" for item in body["active_sources"]["title_templates"]))
            self.assertTrue(all(item["template_name"] != "invalid" for item in body["active_sources"]["description_templates"]))

    def test_preview_prepare_one_two_and_all_fields(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            one = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title"], "sources": {}},
            )
            self.assertEqual(one.status_code, 200)
            self.assertEqual(one.json()["summary"]["requested_fields"], ["title"])

            two = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title", "description"], "sources": {}},
            )
            self.assertEqual(two.status_code, 200)
            self.assertEqual(two.json()["summary"]["requested_fields"], ["title", "description"])

            all_three = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"sources": {}},
            )
            self.assertEqual(all_three.status_code, 200)
            self.assertEqual(all_three.json()["summary"]["requested_fields"], ["title", "description", "tags"])

    def test_partial_failure_and_omitted_not_requested_and_no_mutation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="keep", description="keep desc", planned_at=None)
                self._seed_defaults(conn)
                bad_title = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="bad",
                    template_body="{{release_year}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                before = dict(conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone())
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={
                    "fields": ["title", "description"],
                    "sources": {"title_template_id": bad_title},
                },
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["fields"]["title"]["status"], "GENERATION_FAILED")
            self.assertEqual(body["fields"]["tags"]["status"], "NOT_REQUESTED")
            self.assertNotEqual(body["fields"]["description"]["status"], "GENERATION_FAILED")
            session_id = body["session_id"]

            conn = dbm.connect(env)
            try:
                after = dict(conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone())
                stored = conn.execute("SELECT fields_snapshot_json FROM metadata_preview_sessions WHERE id = ?", (session_id,)).fetchone()
            finally:
                conn.close()
            self.assertEqual(before, after)
            self.assertEqual(stored["fields_snapshot_json"], dbm.json_dumps(body["fields"]))


if __name__ == "__main__":
    unittest.main()
