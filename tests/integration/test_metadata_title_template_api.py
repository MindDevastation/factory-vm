from __future__ import annotations

import importlib
import threading
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.metadata import title_template_service
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

    def test_preview_invalid_release_date_returns_deterministic_error_shape(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates/preview",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name}} {{release_year}}",
                    "release_date": "2026-13-40",
                },
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json(), {
                "error": {
                    "code": "MTB_INVALID_RELEASE_DATE",
                    "message": "release_date must use YYYY-MM-DD format",
                }
            })

    def test_list_invalid_status_returns_deterministic_error_shape(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/metadata/title-templates?status=unknown", headers=headers)
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json(), {
                "error": {
                    "code": "MTB_INVALID_STATUS_FILTER",
                    "message": "status must be active|archived|all",
                }
            })

    def test_create_rejects_non_whitelisted_variable(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main",
                    "template_body": "{{channel_display_name}} {{artist_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "MTB_TEMPLATE_VARIABLE_NOT_ALLOWED")

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

    def test_set_default_on_active_valid_template(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Active",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(created.status_code, 200)

            set_default = client.post(
                f"/v1/metadata/title-templates/{created.json()['id']}/set-default",
                headers=headers,
            )
            self.assertEqual(set_default.status_code, 200)
            self.assertTrue(set_default.json()["is_default"])

    def test_set_default_switching_unsets_previous_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            first = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": True,
                },
            )
            second = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "B",
                    "template_body": "{{channel_slug}}",
                    "make_default": False,
                },
            )
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)

            switched = client.post(f"/v1/metadata/title-templates/{second.json()['id']}/set-default", headers=headers)
            self.assertEqual(switched.status_code, 200)
            self.assertTrue(switched.json()["is_default"])

            first_detail = client.get(f"/v1/metadata/title-templates/{first.json()['id']}", headers=headers)
            self.assertEqual(first_detail.status_code, 200)
            self.assertFalse(first_detail.json()["is_default"])

    def test_archived_template_cannot_be_set_as_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(created.status_code, 200)
            archived = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/archive", headers=headers)
            self.assertEqual(archived.status_code, 200)

            denied = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/set-default", headers=headers)
            self.assertEqual(denied.status_code, 422)
            self.assertEqual(denied.json()["error"]["code"], "MTB_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT")

    def test_invalid_template_cannot_be_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(created.status_code, 200)

            conn = dbm.connect(env)
            try:
                conn.execute(
                    "UPDATE title_templates SET validation_status = 'INVALID', validation_errors_json = '[\"x\"]' WHERE id = ?",
                    (created.json()["id"],),
                )
                conn.commit()
            finally:
                conn.close()

            denied = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/set-default", headers=headers)
            self.assertEqual(denied.status_code, 422)
            self.assertEqual(denied.json()["error"]["code"], "MTB_INVALID_TEMPLATE_CANNOT_BE_DEFAULT")

    def test_archive_clears_default_flag(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": True,
                },
            )
            self.assertEqual(created.status_code, 200)

            archived = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/archive", headers=headers)
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(archived.json()["status"], "ARCHIVED")
            self.assertFalse(archived.json()["is_default"])
            self.assertIsNotNone(archived.json()["archived_at"])

    def test_activate_restores_active_but_not_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": True,
                },
            )
            self.assertEqual(created.status_code, 200)
            client.post(f"/v1/metadata/title-templates/{created.json()['id']}/archive", headers=headers)

            activated = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)
            self.assertEqual(activated.json()["status"], "ACTIVE")
            self.assertFalse(activated.json()["is_default"])
            self.assertIsNone(activated.json()["archived_at"])

    def test_archive_already_archived_is_safe_and_consistent(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(created.status_code, 200)

            first = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/archive", headers=headers)
            second = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/archive", headers=headers)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(first.json()["status"], "ARCHIVED")
            self.assertEqual(second.json()["status"], "ARCHIVED")
            self.assertEqual(first.json()["archived_at"], second.json()["archived_at"])

    def test_set_default_already_default_is_safe_and_consistent(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "A",
                    "template_body": "{{channel_display_name}}",
                    "make_default": True,
                },
            )
            self.assertEqual(created.status_code, 200)

            first = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/set-default", headers=headers)
            second = client.post(f"/v1/metadata/title-templates/{created.json()['id']}/set-default", headers=headers)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertTrue(first.json()["is_default"])
            self.assertTrue(second.json()["is_default"])

    def test_concurrent_default_switching_preserves_single_default_invariant(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                first_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="T1",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at=dbm.now_ts(),
                    created_at=dbm.now_ts(),
                    updated_at=dbm.now_ts(),
                    archived_at=None,
                )
                second_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="T2",
                    template_body="{{channel_slug}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at=dbm.now_ts(),
                    created_at=dbm.now_ts(),
                    updated_at=dbm.now_ts(),
                    archived_at=None,
                )
                conn.commit()
            finally:
                conn.close()

            barrier = threading.Barrier(2)
            errors: list[str] = []

            def _worker(template_id: int) -> None:
                worker_conn = dbm.connect(env)
                try:
                    barrier.wait(timeout=5)
                    title_template_service.set_default_title_template(worker_conn, template_id=template_id)
                    worker_conn.commit()
                except Exception as exc:
                    errors.append(str(exc))
                finally:
                    worker_conn.close()

            t1 = threading.Thread(target=_worker, args=(first_id,))
            t2 = threading.Thread(target=_worker, args=(second_id,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            self.assertEqual(errors, [])
            conn = dbm.connect(env)
            try:
                count_row = conn.execute(
                    "SELECT COUNT(*) AS c FROM title_templates WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1",
                    ("darkwood-reverie",),
                ).fetchone()
                ids = {
                    int(r["id"])
                    for r in conn.execute(
                        "SELECT id FROM title_templates WHERE channel_slug = ? AND status = 'ACTIVE' AND is_default = 1",
                        ("darkwood-reverie",),
                    ).fetchall()
                }
            finally:
                conn.close()
            self.assertEqual(int(count_row["c"]), 1)
            self.assertTrue(ids.issubset({first_id, second_id}))

    def test_lifecycle_operations_do_not_mutate_releases(self) -> None:
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
            client.post(f"/v1/metadata/title-templates/{created.json()['id']}/set-default", headers=headers)
            client.post(f"/v1/metadata/title-templates/{created.json()['id']}/archive", headers=headers)
            client.post(f"/v1/metadata/title-templates/{created.json()['id']}/activate", headers=headers)

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT title FROM releases WHERE origin_meta_file_id = 'meta-fixed'").fetchone()
            finally:
                conn.close()

            assert before is not None and after is not None
            self.assertEqual(before["title"], after["title"])

    def test_create_remains_channel_only_when_extra_content_type_is_sent(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            create_resp = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                    "content_type": "video",
                },
            )
            self.assertEqual(create_resp.status_code, 200)
            created = create_resp.json()
            self.assertNotIn("content_type", created)
            self.assertEqual(created["channel_slug"], "darkwood-reverie")

            detail_resp = client.get(f"/v1/metadata/title-templates/{created['id']}", headers=headers)
            self.assertEqual(detail_resp.status_code, 200)
            detail = detail_resp.json()
            self.assertNotIn("content_type", detail)
            self.assertEqual(detail["channel_slug"], "darkwood-reverie")

    def test_delete_endpoint_not_exposed(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            create_resp = client.post(
                "/v1/metadata/title-templates",
                headers=headers,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Main",
                    "template_body": "{{channel_display_name}}",
                    "make_default": False,
                },
            )
            self.assertEqual(create_resp.status_code, 200)

            resp = client.delete(
                f"/v1/metadata/title-templates/{create_resp.json()['id']}",
                headers=headers,
            )
            self.assertEqual(resp.status_code, 405)


if __name__ == "__main__":
    unittest.main()
