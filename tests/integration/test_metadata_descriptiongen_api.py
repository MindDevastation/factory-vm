from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataDescriptionGenApi(unittest.TestCase):
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
        description: str = "Existing description text",
    ):
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, description, "[]", planned_at, "f", f"meta-{channel_slug}-{planned_at}-{title}", dbm.now_ts()),
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
        body: str = "{{channel_display_name}}\n\n{{release_title}}",
        name: str = "tmpl",
        ) -> int:
        return dbm.create_description_template(
            conn,
            channel_slug=channel_slug,
            template_name=name,
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

    def _release_row(self, env, release_id: int):
        conn = dbm.connect(env)
        try:
            row = conn.execute("SELECT * FROM releases WHERE id = ?", (release_id,)).fetchone()
            assert row is not None
            return dict(row)
        finally:
            conn.close()

    def test_context_endpoint_with_default_present(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                default_id = self._seed_template(conn, is_default=True, name="default")
                self._seed_template(conn, is_default=False, name="alt")
                self._seed_template(conn, status="ARCHIVED", is_default=False, name="arch")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get(f"/v1/metadata/releases/{release_id}/descriptiongen/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["release_id"], release_id)
            self.assertEqual(body["default_template"]["id"], default_id)
            self.assertEqual(len(body["active_templates"]), 2)
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
            resp = client.get(f"/v1/metadata/releases/{release_id}/descriptiongen/context", headers=headers)
            self.assertEqual(resp.status_code, 200)
            self.assertIsNone(resp.json()["default_template"])
            self.assertFalse(resp.json()["can_generate_with_default"])

    def test_generate_with_default_template(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                template_id = self._seed_template(conn, is_default=True)
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["used_template"]["id"], template_id)
            self.assertEqual(body["proposed_description"], "Darkwood Reverie\n\nNight Ritual")
            self.assertEqual(body["line_count"], 3)

    def test_generate_with_explicit_template_override(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                explicit = self._seed_template(conn, is_default=False, body="{{channel_slug}}")
                mismatch = self._seed_template(conn, channel_slug="channel-b", is_default=False)
                archived = self._seed_template(conn, is_default=False, status="ARCHIVED")
                invalid = self._seed_template(conn, is_default=False, validation_status="INVALID")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            ok = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={"template_id": explicit})
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.json()["used_template"]["id"], explicit)

            bad_mismatch = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={"template_id": mismatch})
            self.assertEqual(bad_mismatch.status_code, 422)
            self.assertEqual(bad_mismatch.json()["error"]["code"], "MTD_TEMPLATE_CHANNEL_MISMATCH")

            bad_archived = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={"template_id": archived})
            self.assertEqual(bad_archived.status_code, 422)
            self.assertEqual(bad_archived.json()["error"]["code"], "MTD_TEMPLATE_NOT_ACTIVE")

            bad_invalid = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={"template_id": invalid})
            self.assertEqual(bad_invalid.status_code, 422)
            self.assertEqual(bad_invalid.json()["error"]["code"], "MTD_TEMPLATE_INVALID")

    def test_no_default_configured_requires_explicit_template(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, channel_slug="channel-b")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={})
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTD_DEFAULT_TEMPLATE_NOT_CONFIGURED")

    def test_missing_release_title_and_date_context_errors(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                missing_title = self._seed_release(conn, title="   ")
                self._seed_template(conn, is_default=True, body="{{release_title}}", name="title")
                missing_schedule = self._seed_release(conn, title="ok", planned_at=None, description="")
                date_template = self._seed_template(conn, is_default=False, body="{{release_year}}", name="date")
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            title_resp = client.post(f"/v1/metadata/releases/{missing_title}/descriptiongen/generate", headers=headers, json={})
            self.assertEqual(title_resp.status_code, 422)
            self.assertEqual(title_resp.json()["error"]["code"], "MTD_RELEASE_TITLE_NOT_USABLE")

            date_resp = client.post(
                f"/v1/metadata/releases/{missing_schedule}/descriptiongen/generate",
                headers=headers,
                json={"template_id": date_template},
            )
            self.assertEqual(date_resp.status_code, 422)
            self.assertEqual(date_resp.json()["error"]["code"], "MTD_RELEASE_DATE_CONTEXT_MISSING")

    def test_generate_does_not_mutate_release_description(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, description=" Existing description text ")
                self._seed_template(conn, is_default=True)
                before = conn.execute("SELECT description FROM releases WHERE id = ?", (release_id,)).fetchone()["description"]
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={})
            self.assertEqual(generated.status_code, 200)
            self.assertTrue(generated.json()["overwrite_required"])
            self.assertTrue(generated.json()["warnings"])

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT description FROM releases WHERE id = ?", (release_id,)).fetchone()["description"]
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_apply_after_generate_updates_only_release_description(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, description="Existing description text")
                self._seed_template(conn, is_default=True)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={}).json()
            before = self._release_row(env, release_id)

            applied = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=headers,
                json={"generation_fingerprint": generated["generation_fingerprint"], "overwrite_confirmed": True},
            )
            self.assertEqual(applied.status_code, 200)
            self.assertTrue(applied.json()["description_updated"])

            after = self._release_row(env, release_id)
            changed_release_cols = {k for k in before if before[k] != after[k]}
            self.assertEqual(changed_release_cols, {"description"})

    def test_apply_overwrite_requires_confirmation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, description="Manual description")
                self._seed_template(conn, is_default=True)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={}).json()
            resp = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=headers,
                json={"generation_fingerprint": generated["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTD_OVERWRITE_CONFIRMATION_REQUIRED")

    def test_apply_stale_fingerprint_blocks_template_and_release_context_changes(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Night Ritual", description="")
                template_id = self._seed_template(conn, is_default=True)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={}).json()

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE description_templates SET updated_at = ? WHERE id = ?", ("2026-03-03T00:00:00+00:00", template_id))
                conn.commit()
            finally:
                conn.close()
            stale_template = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=headers,
                json={"generation_fingerprint": generated["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(stale_template.status_code, 422)
            self.assertEqual(stale_template.json()["error"]["code"], "MTD_PREVIEW_STALE")

            regenerated = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={}).json()
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("Night Ritual v2", release_id))
                conn.commit()
            finally:
                conn.close()
            stale_context = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=headers,
                json={"generation_fingerprint": regenerated["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(stale_context.status_code, 422)
            self.assertEqual(stale_context.json()["error"]["code"], "MTD_PREVIEW_STALE")

    def test_apply_same_description_is_safe_noop(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, description="Darkwood Reverie\n\nNight Ritual")
                template_id = self._seed_template(conn, is_default=True)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            generated = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=headers, json={})
            self.assertEqual(generated.status_code, 200)
            applied = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=headers,
                json={"generation_fingerprint": generated.json()["generation_fingerprint"], "overwrite_confirmed": False},
            )
            self.assertEqual(applied.status_code, 200)
            body = applied.json()
            self.assertFalse(body["description_updated"])
            self.assertEqual(body["used_template_id"], template_id)
            self.assertIn("already matches", body["message"])


if __name__ == "__main__":
    unittest.main()
