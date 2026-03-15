from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
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

    def test_create_and_details_and_patch(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            create_resp = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main title template",
                    "template_body": "{{channel_display_name}} — {{release_year}}-{{release_month_number}}",
                    "make_default": False,
                },
            )
            self.assertEqual(create_resp.status_code, 200)
            created = create_resp.json()
            self.assertEqual(created["status"], "ACTIVE")
            self.assertEqual(created["validation_status"], "VALID")
            self.assertEqual(created["validation_errors"], [])

            detail_resp = client.get(f"/v1/metadata/title-templates/{created['id']}", headers=headers)
            self.assertEqual(detail_resp.status_code, 200)
            detail = detail_resp.json()
            self.assertEqual(detail["template_body"], "{{channel_display_name}} — {{release_year}}-{{release_month_number}}")
            self.assertIn("last_validated_at", detail)

            patch_resp = client.patch(
                f"/v1/metadata/title-templates/{created['id']}",
                headers=headers,
                json={"template_name": "Edited", "template_body": "{{channel_slug}} - {{release_year}}"},
            )
            self.assertEqual(patch_resp.status_code, 200)
            patched = patch_resp.json()
            self.assertEqual(patched["id"], created["id"])
            self.assertEqual(patched["template_name"], "Edited")
            self.assertEqual(patched["template_body"], "{{channel_slug}} - {{release_year}}")

            persisted_resp = client.get(f"/v1/metadata/title-templates/{created['id']}", headers=headers)
            self.assertEqual(persisted_resp.status_code, 200)
            persisted = persisted_resp.json()
            self.assertEqual(persisted["template_name"], "Edited")
            self.assertEqual(persisted["template_body"], "{{channel_slug}} - {{release_year}}")

    def test_list_filters_by_channel_status_and_query(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main title template",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            create_second = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "channel-b",
                    "template_name": "B template",
                    "template_body": "{{channel_slug}}",
                    "make_default": False,
                },
            )
            second_id = create_second.json()["id"]

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE title_templates SET status = 'ARCHIVED' WHERE id = ?", (second_id,))
            finally:
                conn.close()

            filtered = client.get(
                "/v1/metadata/title-templates",
                headers=headers,
                params={"channel_slug": "darkwood-reverie", "status": "active", "q": "Main"},
            )
            self.assertEqual(filtered.status_code, 200)
            self.assertEqual(len(filtered.json()["items"]), 1)

            archived = client.get(
                "/v1/metadata/title-templates",
                headers=headers,
                params={"status": "archived"},
            )
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(len(archived.json()["items"]), 1)
            self.assertEqual(archived.json()["items"][0]["channel_slug"], "channel-b")

    def test_invalid_template_save_rejected(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": " ",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTB_TEMPLATE_NAME_REQUIRED")

    def test_create_make_default_enforces_single_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            first = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": True,
                },
            )
            second = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default B",
                    "template_body": "{{channel_slug}}",
                    "make_default": True,
                },
            )
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)

            conn = dbm.connect(env)
            try:
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM title_templates WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1",
                    ("darkwood-reverie",),
                ).fetchone()
            finally:
                conn.close()
            self.assertEqual(int(row["c"]), 1)

    def test_create_and_patch_do_not_mutate_releases(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                ts = dbm.now_ts()
                conn.execute(
                    "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (int(ch["id"]), "Original", "d", "[]", None, None, "meta-fixed", ts),
                )
                before = conn.execute("SELECT title FROM releases WHERE origin_meta_file_id = 'meta-fixed'").fetchone()
                assert before is not None
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(created.status_code, 200)
            client.patch(
                f"/v1/metadata/title-templates/{created.json()['id']}",
                headers=headers,
                json={"template_name": "Main2"},
            )

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT title FROM releases WHERE origin_meta_file_id = 'meta-fixed'").fetchone()
            finally:
                conn.close()

            assert before is not None and after is not None
            self.assertEqual(before["title"], after["title"])


if __name__ == "__main__":
    unittest.main()
