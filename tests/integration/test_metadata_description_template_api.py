from __future__ import annotations

import importlib
import threading
import time
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.metadata import description_template_service as dts
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

    def _create_template(self, client: TestClient, headers: dict[str, str], **overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "channel_slug": "darkwood-reverie",
            "template_name": "Main description template",
            "template_body": "{{channel_display_name}}\n\n{{release_title}}",
            "make_default": False,
        }
        payload.update(overrides)
        resp = client.post("/v1/metadata/description-templates", headers=headers, json=payload)
        self.assertEqual(resp.status_code, 200)
        return resp.json()

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

    def test_create_list_detail_patch_lifecycle(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            release_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                title="Seed Title",
                planned_at="2026-03-01T00:00:00+00:00",
            )
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            created = self._create_template(client, headers)
            template_id = int(created["id"])
            self.assertEqual(created["status"], "ACTIVE")
            self.assertEqual(created["validation_status"], "VALID")
            self.assertEqual(created["validation_errors"], [])
            self.assertIsNotNone(created["last_validated_at"])

            list_resp = client.get(
                "/v1/metadata/description-templates?channel_slug=darkwood-reverie&status=active&q=Main",
                headers=headers,
            )
            self.assertEqual(list_resp.status_code, 200)
            self.assertEqual(len(list_resp.json()["items"]), 1)

            detail_resp = client.get(f"/v1/metadata/description-templates/{template_id}", headers=headers)
            self.assertEqual(detail_resp.status_code, 200)
            self.assertIn("{{release_title}}", detail_resp.json()["template_body"])

            patched = client.patch(
                f"/v1/metadata/description-templates/{template_id}",
                headers=headers,
                json={"template_name": "Renamed", "template_body": "{{channel_slug}}\n\n{{release_title}}"},
            )
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(int(patched.json()["id"]), template_id)
            self.assertEqual(patched.json()["template_name"], "Renamed")

            set_default = client.post(f"/v1/metadata/description-templates/{template_id}/set-default", headers=headers)
            self.assertEqual(set_default.status_code, 200)
            self.assertTrue(set_default.json()["is_default"])

            set_default_again = client.post(
                f"/v1/metadata/description-templates/{template_id}/set-default",
                headers=headers,
            )
            self.assertEqual(set_default_again.status_code, 200)
            self.assertTrue(set_default_again.json()["is_default"])

            archived = client.post(f"/v1/metadata/description-templates/{template_id}/archive", headers=headers)
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(archived.json()["status"], "ARCHIVED")
            self.assertFalse(archived.json()["is_default"])

            archived_again = client.post(f"/v1/metadata/description-templates/{template_id}/archive", headers=headers)
            self.assertEqual(archived_again.status_code, 200)
            self.assertEqual(archived_again.json()["status"], "ARCHIVED")

            denied = client.post(f"/v1/metadata/description-templates/{template_id}/set-default", headers=headers)
            self.assertEqual(denied.status_code, 422)
            self.assertEqual(denied.json()["error"]["code"], "MTD_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT")

            activated = client.post(f"/v1/metadata/description-templates/{template_id}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)
            self.assertEqual(activated.json()["status"], "ACTIVE")
            self.assertFalse(activated.json()["is_default"])

            release_conn = dbm.connect(env)
            try:
                rel = release_conn.execute("SELECT description FROM releases WHERE id = ?", (release_id,)).fetchone()
                self.assertIsNotNone(rel)
                self.assertEqual(str(rel["description"]), "desc")
            finally:
                release_conn.close()

    def test_invalid_template_rejected_and_tab_validation_preserved(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/description-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "  ",
                    "template_body": "{{channel_display_name}}",
                },
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTD_TEMPLATE_NAME_REQUIRED")

            resp_tab = client.post(
                "/v1/metadata/description-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Tab test",
                    "template_body": "{{channel_display_name}}\n\t",
                },
            )
            self.assertEqual(resp_tab.status_code, 422)
            self.assertIn(
                resp_tab.json()["error"]["code"],
                {"MTD_TEMPLATE_TAB_NOT_ALLOWED", "MTD_RENDER_TAB_NOT_ALLOWED"},
            )

    def test_set_default_single_default_invariant_with_light_concurrency(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            first = self._create_template(client, headers, template_name="First")
            second = self._create_template(client, headers, template_name="Second")
            first_id = int(first["id"])
            second_id = int(second["id"])

            errors: list[Exception] = []

            def _set_default(template_id: int) -> None:
                conn = dbm.connect(env)
                try:
                    dts.set_default_description_template(conn, template_id=template_id)
                    conn.commit()
                except Exception as exc:  # pragma: no cover - explicit capture for assertion
                    errors.append(exc)
                finally:
                    conn.close()

            t1 = threading.Thread(target=_set_default, args=(first_id,))
            t2 = threading.Thread(target=_set_default, args=(second_id,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            self.assertEqual(errors, [])

            conn = dbm.connect(env)
            try:
                rows = conn.execute(
                    """
                    SELECT id, is_default
                    FROM description_templates
                    WHERE channel_slug = ? AND status = 'ACTIVE'
                    ORDER BY id ASC
                    """,
                    ("darkwood-reverie",),
                ).fetchall()
            finally:
                conn.close()
            default_count = sum(1 for row in rows if bool(int(row["is_default"])))
            self.assertEqual(default_count, 1)


if __name__ == "__main__":
    unittest.main()
