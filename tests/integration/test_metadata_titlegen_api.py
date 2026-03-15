from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataTitleGenApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_release(self, conn, *, channel_slug: str = "darkwood-reverie", planned_at: str | None = "2026-04-09T12:00:00Z", title: str = "Current"):
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, "d", "[]", planned_at, "f", f"meta-{channel_slug}-{planned_at}-{title}", dbm.now_ts()),
        )
        return int(cur.lastrowid)

    def _seed_template(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        is_default: bool = True,
        status: str = "ACTIVE",
        validation_status: str = "VALID",
        body: str = "{{channel_display_name}} {{release_year}}",
    ) -> int:
        return dbm.create_title_template(
            conn,
            channel_slug=channel_slug,
            template_name="tmpl",
            template_body=body,
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
                self._seed_template(conn, is_default=True)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIsNotNone(body["default_template"])
            self.assertTrue(body["overwrite_required"])

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
            resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertIsNone(resp.json()["default_template"])

    def test_generate_with_default_template_and_no_release_mutation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="  Keep Me  ")
                self._seed_template(conn, is_default=True)
                before = conn.execute("SELECT title FROM releases WHERE id = ?", (release_id,)).fetchone()["title"]
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["proposed_title"], "Darkwood Reverie 2026")
            self.assertEqual(resp.json()["warnings"][0]["code"], "MTG_OVERWRITE_REQUIRED")

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT title FROM releases WHERE id = ?", (release_id,)).fetchone()["title"]
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_generate_with_explicit_template_override_and_rejections(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                good = self._seed_template(conn, is_default=False, body="{{channel_slug}}")
                mismatch = self._seed_template(conn, channel_slug="channel-b", is_default=False)
                archived = self._seed_template(conn, is_default=False, status="ARCHIVED")
                invalid = self._seed_template(conn, is_default=False, validation_status="INVALID")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            ok = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": good})
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.json()["template"]["source"], "explicit")

            bad_mismatch = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": mismatch})
            self.assertEqual(bad_mismatch.status_code, 422)
            self.assertEqual(bad_mismatch.json()["error"]["code"], "MTG_TEMPLATE_CHANNEL_MISMATCH")

            bad_archived = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": archived})
            self.assertEqual(bad_archived.status_code, 422)
            self.assertEqual(bad_archived.json()["error"]["code"], "MTG_TEMPLATE_NOT_ACTIVE")

            bad_invalid = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=headers, json={"template_id": invalid})
            self.assertEqual(bad_invalid.status_code, 422)
            self.assertEqual(bad_invalid.json()["error"]["code"], "MTG_TEMPLATE_INVALID")

    def test_generate_without_default_or_missing_schedule(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_no_default = self._seed_release(conn, title="", channel_slug="channel-b")
                release_missing_planned = self._seed_release(conn, title="", planned_at=None)
                self._seed_template(conn, is_default=True, body="{{channel_display_name}} {{release_year}}")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            no_default = client.post(f"/v1/metadata/releases/{release_no_default}/titlegen/generate", headers=headers, json={})
            self.assertEqual(no_default.status_code, 422)
            self.assertEqual(no_default.json()["error"]["code"], "MTG_DEFAULT_TEMPLATE_NOT_CONFIGURED")

            missing_planned = client.post(f"/v1/metadata/releases/{release_missing_planned}/titlegen/generate", headers=headers, json={})
            self.assertEqual(missing_planned.status_code, 422)
            self.assertEqual(missing_planned.json()["error"]["code"], "MTG_REQUIRED_CONTEXT_MISSING")


if __name__ == "__main__":
    unittest.main()
