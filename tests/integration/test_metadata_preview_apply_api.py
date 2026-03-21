from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

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
        dbm.upsert_channel_metadata_defaults(
            conn,
            channel_slug="darkwood-reverie",
            default_title_template_id=t,
            default_description_template_id=d,
            default_video_tag_preset_id=p,
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
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
            self.assertTrue(all(item["name"] != "invalid" for item in body["active_sources"]["description_templates"]))
            self.assertIn("name", body["defaults"]["title_template"])
            self.assertNotIn("template_name", body["defaults"]["title_template"])
            self.assertIn("name", body["defaults"]["description_template"])
            self.assertNotIn("template_name", body["defaults"]["description_template"])
            self.assertIn("name", body["defaults"]["video_tag_preset"])
            self.assertNotIn("preset_name", body["defaults"]["video_tag_preset"])
            self.assertTrue(all("name" in item for item in body["active_sources"]["title_templates"]))
            self.assertTrue(all("template_name" not in item for item in body["active_sources"]["title_templates"]))
            self.assertTrue(all("name" in item for item in body["active_sources"]["description_templates"]))
            self.assertTrue(all("template_name" not in item for item in body["active_sources"]["description_templates"]))
            self.assertTrue(all("name" in item for item in body["active_sources"]["video_tag_presets"]))
            self.assertTrue(all("preset_name" not in item for item in body["active_sources"]["video_tag_presets"]))

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
            title_source = one.json()["fields"]["title"]["source"]
            self.assertEqual(set(["source_type", "source_id", "source_name", "selection_mode"]).issubset(title_source.keys()), True)
            self.assertTrue(title_source["source_name"])

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

    def test_preview_with_override_returns_provenance_and_source_name(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                _, _, _ = self._seed_defaults(conn)
                alt_title = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="alt-title",
                    template_body="{{channel_slug}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-02T00:00:00+00:00",
                    created_at="2026-01-02T00:00:00+00:00",
                    updated_at="2026-01-02T00:00:00+00:00",
                    archived_at=None,
                )
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title", "description"], "sources": {"title_template_id": alt_title}},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            title_source = body["fields"]["title"]["source"]
            desc_source = body["fields"]["description"]["source"]
            self.assertEqual(title_source["selection_mode"], "temporary_override")
            self.assertEqual(title_source["source_type"], "title_template")
            self.assertEqual(title_source["source_id"], alt_title)
            self.assertEqual(title_source["source_name"], "alt-title")
            self.assertEqual(desc_source["selection_mode"], "channel_default")
            self.assertTrue(desc_source["source_name"])

    def test_preview_error_messages_are_operator_readable(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                invalid_title = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="bad-title",
                    template_body="{{channel_slug}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="INVALID",
                    validation_errors_json='[{"code":"bad"}]',
                    last_validated_at="2026-01-02T00:00:00+00:00",
                    created_at="2026-01-02T00:00:00+00:00",
                    updated_at="2026-01-02T00:00:00+00:00",
                    archived_at=None,
                )
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            invalid_override = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title"], "sources": {"title_template_id": invalid_title}},
            )
            self.assertEqual(invalid_override.status_code, 200)
            err_msg = invalid_override.json()["fields"]["title"]["errors"][0]["message"]
            self.assertIn("title", err_msg)
            self.assertIn("temporary override", err_msg)
            self.assertIn("title_template", err_msg)
            self.assertIn(f"#{invalid_title}", err_msg)
            self.assertIn("source must be VALID", err_msg)

            missing = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["description"], "sources": {}},
            )
            self.assertEqual(missing.status_code, 200)
            missing_msg = missing.json()["fields"]["description"]["errors"][0]["message"]
            self.assertIn("description", missing_msg)
            self.assertIn("no temporary override", missing_msg)
            self.assertIn("no configured channel default", missing_msg)

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
            self.assertTrue(body["fields"]["title"]["errors"])
            first_error = body["fields"]["title"]["errors"][0]
            self.assertEqual(set(first_error.keys()), {"code", "message"})
            self.assertEqual(body["fields"]["tags"]["status"], "NOT_REQUESTED")
            self.assertNotEqual(body["fields"]["description"]["status"], "GENERATION_FAILED")
            self.assertIn("name", body["fields"]["description"]["source"])
            self.assertNotIn("template_name", body["fields"]["description"]["source"])
            session_id = body["session_id"]

            conn = dbm.connect(env)
            try:
                after = dict(conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone())
                stored = conn.execute("SELECT fields_snapshot_json FROM metadata_preview_sessions WHERE id = ?", (session_id,)).fetchone()
            finally:
                conn.close()
            self.assertEqual(before, after)
            self.assertEqual(stored["fields_snapshot_json"], dbm.json_dumps(body["fields"]))

    def test_session_retrieval_recalculates_stale(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="before")
                self._seed_defaults(conn)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["description"], "sources": {}},
            )
            self.assertEqual(preview.status_code, 200)
            session_id = preview.json()["session_id"]

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("after", release_id))
                conn.commit()
            finally:
                conn.close()

            session = client.get(f"/v1/metadata/preview-apply/sessions/{session_id}", headers=headers)
            self.assertEqual(session.status_code, 200)
            self.assertEqual(session.json()["fields"]["description"]["status"], "STALE")

    def test_default_source_change_does_not_stale_prepared_field(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="before")
                first_title_id, _, _ = self._seed_defaults(conn)
                second_title_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="t2",
                    template_body="{{channel_slug}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-02T00:00:00+00:00",
                    created_at="2026-01-02T00:00:00+00:00",
                    updated_at="2026-01-02T00:00:00+00:00",
                    archived_at=None,
                )
                conn.execute("UPDATE title_templates SET is_default = 0 WHERE id = ?", (first_title_id,))
                conn.execute("UPDATE title_templates SET is_default = 1 WHERE id = ?", (second_title_id,))
                conn.commit()
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title"], "sources": {}},
            )
            self.assertEqual(preview.status_code, 200)
            session_id = preview.json()["session_id"]
            preview_status = preview.json()["fields"]["title"]["status"]
            conn = dbm.connect(env)
            try:
                before_snapshot = dict(
                    conn.execute(
                        "SELECT effective_source_selection_json, effective_source_provenance_json FROM metadata_preview_sessions WHERE id = ?",
                        (session_id,),
                    ).fetchone()
                )
            finally:
                conn.close()

            conn = dbm.connect(env)
            try:
                third_title_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="t3",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-03T00:00:00+00:00",
                    created_at="2026-01-03T00:00:00+00:00",
                    updated_at="2026-01-03T00:00:00+00:00",
                    archived_at=None,
                )
                conn.execute("UPDATE title_templates SET is_default = 0 WHERE id = ?", (second_title_id,))
                conn.execute("UPDATE title_templates SET is_default = 1 WHERE id = ?", (third_title_id,))
                conn.commit()
            finally:
                conn.close()

            session = client.get(f"/v1/metadata/preview-apply/sessions/{session_id}", headers=headers)
            self.assertEqual(session.status_code, 200)
            self.assertEqual(session.json()["fields"]["title"]["status"], preview_status)
            conn = dbm.connect(env)
            try:
                after_snapshot = dict(
                    conn.execute(
                        "SELECT effective_source_selection_json, effective_source_provenance_json FROM metadata_preview_sessions WHERE id = ?",
                        (session_id,),
                    ).fetchone()
                )
            finally:
                conn.close()
            self.assertEqual(before_snapshot, after_snapshot)

    def test_apply_subset_atomic_and_single_use(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="old", description="old-desc", tags_json='["old"]')
                self._seed_defaults(conn)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title", "description", "tags"], "sources": {}},
            )
            self.assertEqual(preview.status_code, 200)
            session_id = preview.json()["session_id"]

            apply_resp = client.post(
                f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                headers=headers,
                json={"selected_fields": ["title"], "overwrite_confirmed_fields": ["title"]},
            )
            self.assertEqual(apply_resp.status_code, 200)
            body = apply_resp.json()
            self.assertEqual(body["applied_fields"], ["title"])
            self.assertEqual(body["result"], "success")

            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            self.assertNotEqual(row["title"], "old")
            self.assertEqual(row["description"], "old-desc")
            self.assertEqual(row["tags_json"], '["old"]')

            second_apply = client.post(
                f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                headers=headers,
                json={"selected_fields": ["description"], "overwrite_confirmed_fields": ["description"]},
            )
            self.assertEqual(second_apply.status_code, 422)
            self.assertEqual(second_apply.json()["error"]["code"], "MPA_APPLY_CONFLICT")

    def test_apply_fails_atomically_when_selected_field_is_stale(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="old", description="old-desc")
                self._seed_defaults(conn)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["description", "tags"], "sources": {}},
            )
            self.assertEqual(preview.status_code, 200)
            session_id = preview.json()["session_id"]

            conn = dbm.connect(env)
            try:
                before = dict(conn.execute("SELECT description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone())
                conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("changed-title", release_id))
                conn.commit()
            finally:
                conn.close()

            apply_resp = client.post(
                f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                headers=headers,
                json={"selected_fields": ["description", "tags"], "overwrite_confirmed_fields": ["description", "tags"]},
            )
            self.assertEqual(apply_resp.status_code, 422)
            self.assertEqual(apply_resp.json()["error"]["code"], "MPA_PREVIEW_STALE")

            conn = dbm.connect(env)
            try:
                after = dict(conn.execute("SELECT description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone())
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_stale_apply_logging_includes_channel_and_stale_fields(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="old", description="old-desc")
                self._seed_defaults(conn)
            finally:
                conn.close()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["description"], "sources": {}},
            )
            session_id = preview.json()["session_id"]

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("changed-title", release_id))
                conn.commit()
            finally:
                conn.close()

            with patch("services.factory_api.app.logger.info") as info_mock:
                apply_resp = client.post(
                    f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                    headers=headers,
                    json={"selected_fields": ["description"], "overwrite_confirmed_fields": ["description"]},
                )
            self.assertEqual(apply_resp.status_code, 422)
            stale_calls = [call for call in info_mock.call_args_list if call.args and call.args[0] == "metadata.preview_apply.stale_detected"]
            self.assertTrue(stale_calls)
            stale_extra = stale_calls[-1].kwargs.get("extra", {})
            self.assertEqual(stale_extra.get("channel_slug"), "darkwood-reverie")
            self.assertEqual(stale_extra.get("stale_fields"), ["description"])

    def test_no_change_only_apply_conflicts_when_dependency_changes_mid_apply(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="same-title", tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
            finally:
                conn.close()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["tags"], "sources": {}},
            )
            session_id = preview.json()["session_id"]

            original = mod.preview_apply_service._apply_selected_fields_atomic

            def _mutating_guard(*args, **kwargs):
                db = dbm.connect(env)
                try:
                    db.execute("UPDATE releases SET title = ? WHERE id = ?", ("changed-mid-apply", release_id))
                    db.commit()
                finally:
                    db.close()
                return original(*args, **kwargs)

            with patch("services.metadata.preview_apply_service._apply_selected_fields_atomic", side_effect=_mutating_guard):
                apply_resp = client.post(
                    f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                    headers=headers,
                    json={"selected_fields": ["tags"], "overwrite_confirmed_fields": []},
                )
            self.assertEqual(apply_resp.status_code, 422)
            self.assertEqual(apply_resp.json()["error"]["code"], "MPA_APPLY_CONFLICT")

    def test_mixed_selected_no_change_and_update_keeps_unselected_untouched(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="old-title", description="keep-desc", tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
            finally:
                conn.close()
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["title", "tags"], "sources": {}},
            )
            self.assertEqual(preview.status_code, 200)
            self.assertEqual(preview.json()["fields"]["tags"]["status"], "NO_CHANGE")
            session_id = preview.json()["session_id"]

            apply_resp = client.post(
                f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                headers=headers,
                json={"selected_fields": ["title", "tags"], "overwrite_confirmed_fields": ["title"]},
            )
            self.assertEqual(apply_resp.status_code, 200)
            body = apply_resp.json()
            self.assertEqual(body["applied_fields"], ["title"])
            self.assertEqual(body["unchanged_fields"], ["tags"])

            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            self.assertNotEqual(row["title"], "old-title")
            self.assertEqual(row["description"], "keep-desc")
            self.assertEqual(row["tags_json"], '["darkwood-reverie"]')

    def test_no_change_only_apply_conflicts_if_session_finalized_concurrently(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="same-title", tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
            finally:
                conn.close()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=headers,
                json={"fields": ["tags"], "sources": {}},
            )
            session_id = preview.json()["session_id"]
            original = mod.preview_apply_service._mark_session_applied_open_only

            def _mark_after_external_apply(*args, **kwargs):
                db = dbm.connect(env)
                try:
                    db.execute(
                        "UPDATE metadata_preview_sessions SET session_status = 'APPLIED', applied_at = ? WHERE id = ?",
                        ("2026-01-01T00:00:00+00:00", session_id),
                    )
                    db.commit()
                finally:
                    db.close()
                return original(*args, **kwargs)

            with patch("services.metadata.preview_apply_service._mark_session_applied_open_only", side_effect=_mark_after_external_apply):
                apply_resp = client.post(
                    f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                    headers=headers,
                    json={"selected_fields": ["tags"], "overwrite_confirmed_fields": []},
                )
            self.assertEqual(apply_resp.status_code, 422)
            self.assertEqual(apply_resp.json()["error"]["code"], "MPA_APPLY_CONFLICT")


if __name__ == "__main__":
    unittest.main()
