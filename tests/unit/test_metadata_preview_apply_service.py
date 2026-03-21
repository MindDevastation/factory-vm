from __future__ import annotations

import unittest
from datetime import datetime
from unittest import mock

from services.common import db as dbm
from services.metadata import preview_apply_service as svc
from tests._helpers import seed_minimal_db, temp_env


class TestMetadataPreviewApplyService(unittest.TestCase):
    def _seed_release(self, conn, *, title: str = "Current", description: str = "Current desc", tags_json: str = '["one"]', planned_at: str | None = "2026-04-09T12:00:00Z") -> int:
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
            template_name="t-default",
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
            template_name="d-default",
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
            preset_name="p-default",
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

    def test_requested_subset_and_not_requested_status(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(out["summary"]["requested_fields"], ["title"])
            self.assertEqual(out["fields"]["description"]["status"], "NOT_REQUESTED")
            self.assertEqual(out["fields"]["tags"]["status"], "NOT_REQUESTED")

    def test_source_resolution_without_default_is_configuration_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(out["fields"]["title"]["status"], "CONFIGURATION_MISSING")
            self.assertEqual(out["fields"]["title"]["errors"][0]["code"], "MDO_CONFIGURATION_MISSING")
            self.assertIn("title", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("no temporary override", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("no configured channel default", out["fields"]["title"]["errors"][0]["message"])

    def test_invalid_stored_default_returns_invalid_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                invalid_title = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="t-invalid",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="INVALID",
                    validation_errors_json='[{"code":"bad"}]',
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                dbm.upsert_channel_metadata_defaults(
                    conn,
                    channel_slug="darkwood-reverie",
                    default_title_template_id=invalid_title,
                    default_description_template_id=None,
                    default_video_tag_preset_id=None,
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                )
                out = svc.create_preview_session(conn, release_id=release_id, requested_fields=["title"], sources={}, created_by="u", ttl_seconds=1800)
            finally:
                conn.close()
            self.assertEqual(out["fields"]["title"]["status"], "INVALID_DEFAULT")
            self.assertEqual(out["fields"]["title"]["errors"][0]["code"], "MDO_DEFAULT_SOURCE_INVALID")
            self.assertIn("title", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("channel default", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("title_template", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn(f"#{invalid_title}", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("source must be VALID", out["fields"]["title"]["errors"][0]["message"])

    def test_invalid_override_does_not_fallback_to_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                invalid_title = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="t-invalid",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="INVALID",
                    validation_errors_json='[{"code":"bad"}]',
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title"],
                    sources={"title_template_id": invalid_title},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()
            self.assertEqual(out["fields"]["title"]["status"], "INVALID_OVERRIDE")
            self.assertEqual(out["fields"]["title"]["errors"][0]["code"], "MDO_OVERRIDE_SOURCE_INVALID")
            self.assertEqual(out["fields"]["title"]["source"]["selection_mode"], "temporary_override")
            self.assertIn("title", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("temporary override", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("title_template", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn(f"#{invalid_title}", out["fields"]["title"]["errors"][0]["message"])
            self.assertIn("source must be VALID", out["fields"]["title"]["errors"][0]["message"])

    def test_preview_persists_effective_source_selection_and_provenance(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title", "description", "tags"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
                row = conn.execute(
                    "SELECT effective_source_selection_json, effective_source_provenance_json FROM metadata_preview_sessions WHERE id = ?",
                    (out["session_id"],),
                ).fetchone()
            finally:
                conn.close()
            selection = dbm.json_loads(row["effective_source_selection_json"])
            provenance = dbm.json_loads(row["effective_source_provenance_json"])
            self.assertEqual(selection["title"]["selection_mode"], "channel_default")
            self.assertTrue(selection["title"]["source_name"])
            self.assertEqual(provenance["title"]["source_id"], out["fields"]["title"]["source"]["source_id"])
            self.assertEqual(provenance["title"]["source_type"], "title_template")
            self.assertEqual(provenance["title"]["selection_mode"], "channel_default")

    def test_override_observability_events_emitted(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                _, _, _ = self._seed_defaults(conn)
                valid_override = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="t-override",
                    template_body="{{channel_slug}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                with mock.patch("services.metadata.preview_apply_service.logger.info") as info_mock:
                    svc.create_preview_session(
                        conn,
                        release_id=release_id,
                        requested_fields=["title"],
                        sources={"title_template_id": 999999},
                        created_by="u",
                        ttl_seconds=1800,
                    )
                    svc.create_preview_session(
                        conn,
                        release_id=release_id,
                        requested_fields=["title"],
                        sources={"title_template_id": valid_override},
                        created_by="u",
                        ttl_seconds=1800,
                    )
            finally:
                conn.close()
            event_names = [call.args[0] for call in info_mock.call_args_list if call.args]
            self.assertIn("metadata.override.invalid", event_names)
            self.assertIn("metadata.preview.source_resolution_failed", event_names)
            self.assertIn("metadata.override.used_in_preview", event_names)
            invalid_call = next(call for call in info_mock.call_args_list if call.args and call.args[0] == "metadata.override.invalid")
            for key in ["channel_slug", "release_id", "field_name", "selection_mode", "source_type", "source_id", "result_status", "error_codes"]:
                self.assertIn(key, invalid_call.kwargs.get("extra", {}))

    def test_invalid_default_title_template_is_configuration_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="bad-default-title",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=True,
                    validation_status="INVALID",
                    validation_errors_json='[{"code":"bad"}]',
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()
            self.assertEqual(out["fields"]["title"]["status"], "CONFIGURATION_MISSING")

    def test_invalid_default_description_template_is_configuration_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="bad-default-description",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=True,
                    validation_status="INVALID",
                    validation_errors_json='[{"code":"bad"}]',
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["description"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()
            self.assertEqual(out["fields"]["description"]["status"], "CONFIGURATION_MISSING")

    def test_overwrite_and_diff_status(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Current")
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(out["fields"]["title"]["status"], "OVERWRITE_READY")
            self.assertTrue(out["fields"]["title"]["overwrite_required"])

    def test_no_change_status_for_tags(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["tags"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(out["fields"]["tags"]["status"], "NO_CHANGE")
            self.assertFalse(out["fields"]["tags"]["changed"])

    def test_field_failure_is_independent(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, planned_at=None)
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
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title", "description"],
                    sources={"title_template_id": bad_title},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(out["fields"]["title"]["status"], "GENERATION_FAILED")
            self.assertTrue(out["fields"]["title"]["errors"])
            first_error = out["fields"]["title"]["errors"][0]
            self.assertIsInstance(first_error, dict)
            self.assertIn("code", first_error)
            self.assertIn("message", first_error)
            self.assertIn(out["fields"]["description"]["status"], {"OVERWRITE_READY", "PROPOSED_READY", "NO_CHANGE"})

    def test_session_ttl_default_and_persistence(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=None,
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
                row = conn.execute("SELECT * FROM metadata_preview_sessions WHERE id = ?", (out["session_id"],)).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(row)
            expires_at = datetime.fromisoformat(str(row["expires_at"]))
            created_at = datetime.fromisoformat(str(row["created_at"]))
            self.assertGreaterEqual((expires_at - created_at).total_seconds(), 1799)
            self.assertEqual(row["fields_snapshot_json"], dbm.json_dumps(out["fields"]))

    def test_persisted_proposed_bundle_json_uses_canonical_shape(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title", "description", "tags"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
                row = conn.execute("SELECT proposed_bundle_json FROM metadata_preview_sessions WHERE id = ?", (out["session_id"],)).fetchone()
            finally:
                conn.close()
            proposed = dbm.json_loads(row["proposed_bundle_json"])
            self.assertEqual(set(proposed.keys()), {"title", "description", "tags"})
            for field in ["title", "description", "tags"]:
                self.assertIsInstance(proposed[field], dict)
                self.assertEqual(set(proposed[field].keys()), {"prepared", "value"})

    def test_summary_fields_derivation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["title", "description"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(set(out["summary"].keys()), {"requested_fields", "prepared_fields", "applyable_fields", "failed_fields"})
            self.assertEqual(out["summary"]["requested_fields"], ["title", "description"])
            self.assertTrue(set(out["summary"]["applyable_fields"]).issubset({"title", "description"}))

    def test_no_change_field_is_applyable_in_create_summary(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
                out = svc.create_preview_session(
                    conn,
                    release_id=release_id,
                    requested_fields=["tags"],
                    sources={},
                    created_by="u",
                    ttl_seconds=1800,
                )
            finally:
                conn.close()

            self.assertEqual(out["fields"]["tags"]["status"], "NO_CHANGE")
            self.assertEqual(out["summary"]["applyable_fields"], ["tags"])

    def test_build_payload_excludes_current_only_from_prepared_fields(self) -> None:
        session = {
            "id": "s1",
            "release_id": 1,
            "channel_slug": "darkwood-reverie",
            "expires_at": "2026-01-01T00:00:00+00:00",
            "requested_fields_json": dbm.json_dumps(["title"]),
            "current_bundle_json": dbm.json_dumps({"title": "x", "description": "", "tags_json": []}),
        }
        fields = {
            "title": {
                "status": "CURRENT_ONLY",
                "current_value": "x",
                "proposed_value": None,
                "changed": False,
                "overwrite_required": False,
                "source": None,
                "warnings": [],
                "errors": [],
            },
            "description": svc._build_not_requested_record(""),
            "tags": svc._build_not_requested_record([]),
        }
        payload = svc._build_session_payload(session=session, fields=fields, session_status="OPEN")
        self.assertEqual(payload["summary"]["prepared_fields"], [])

    def test_apply_requires_non_empty_selected_fields(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn)
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["title"], sources={}, created_by="u", ttl_seconds=1800)
                with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                    svc.apply_preview_session(conn, session_id=preview["session_id"], selected_fields=[], overwrite_confirmed_fields=[])
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_SELECTED_FIELDS_EMPTY")

    def test_apply_requires_overwrite_confirmation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Current")
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["title"], sources={}, created_by="u", ttl_seconds=1800)
                with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                    svc.apply_preview_session(conn, session_id=preview["session_id"], selected_fields=["title"], overwrite_confirmed_fields=[])
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_OVERWRITE_CONFIRMATION_REQUIRED")

    def test_get_session_marks_field_stale_when_dependency_changes(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="Before")
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["description"], sources={}, created_by="u", ttl_seconds=1800)
                conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("After", release_id))
                conn.commit()
                session = svc.get_preview_session(conn, session_id=preview["session_id"])
            finally:
                conn.close()
            self.assertEqual(session["fields"]["description"]["status"], "STALE")

    def test_apply_no_change_field_returns_unchanged(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["tags"], sources={}, created_by="u", ttl_seconds=1800)
                result = svc.apply_preview_session(conn, session_id=preview["session_id"], selected_fields=["tags"], overwrite_confirmed_fields=[])
            finally:
                conn.close()
            self.assertEqual(result["applied_fields"], [])
            self.assertEqual(result["unchanged_fields"], ["tags"])

    def test_apply_uses_stored_snapshot_without_regeneration(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["title"], sources={}, created_by="u", ttl_seconds=1800)
                with mock.patch("services.metadata.titlegen_service.generate_title_preview", side_effect=AssertionError("should not regenerate")):
                    result = svc.apply_preview_session(
                        conn,
                        session_id=preview["session_id"],
                        selected_fields=["title"],
                        overwrite_confirmed_fields=[],
                    )
            finally:
                conn.close()
            self.assertEqual(result["result"], "success")

    def test_apply_same_session_twice_fails(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["title"], sources={}, created_by="u", ttl_seconds=1800)
                svc.apply_preview_session(conn, session_id=preview["session_id"], selected_fields=["title"], overwrite_confirmed_fields=[])
                with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                    svc.apply_preview_session(conn, session_id=preview["session_id"], selected_fields=["title"], overwrite_confirmed_fields=[])
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_APPLY_CONFLICT")

    def test_expired_session_blocks_apply_and_updates_status(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="")
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["title"], sources={}, created_by="u", ttl_seconds=1800)
                conn.execute("UPDATE metadata_preview_sessions SET expires_at = ? WHERE id = ?", ("2000-01-01T00:00:00+00:00", preview["session_id"]))
                conn.commit()
                with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                    svc.apply_preview_session(conn, session_id=preview["session_id"], selected_fields=["title"], overwrite_confirmed_fields=[])
                row = conn.execute("SELECT session_status FROM metadata_preview_sessions WHERE id = ?", (preview["session_id"],)).fetchone()
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_SESSION_EXPIRED")
            self.assertEqual(row["session_status"], "EXPIRED")

    def test_apply_conflicts_if_release_changes_during_critical_window(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="", description="stable-desc")
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["description"], sources={}, created_by="u", ttl_seconds=1800)
                original = svc._apply_selected_fields_atomic

                def _mutating_guard(*args, **kwargs):
                    conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("changed-after-validate", release_id))
                    return original(*args, **kwargs)

                with mock.patch("services.metadata.preview_apply_service._apply_selected_fields_atomic", side_effect=_mutating_guard):
                    with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                        svc.apply_preview_session(
                            conn,
                            session_id=preview["session_id"],
                            selected_fields=["description"],
                            overwrite_confirmed_fields=["description"],
                        )
                row = conn.execute("SELECT description FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_APPLY_CONFLICT")
            self.assertEqual(row["description"], "stable-desc")

    def test_no_change_only_apply_conflicts_if_dependency_changes_in_critical_window(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="same-title", tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["tags"], sources={}, created_by="u", ttl_seconds=1800)
                self.assertEqual(preview["fields"]["tags"]["status"], "NO_CHANGE")
                original = svc._apply_selected_fields_atomic

                def _mutating_guard(*args, **kwargs):
                    conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("changed-mid-apply", release_id))
                    return original(*args, **kwargs)

                with mock.patch("services.metadata.preview_apply_service._apply_selected_fields_atomic", side_effect=_mutating_guard):
                    with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                        svc.apply_preview_session(
                            conn,
                            session_id=preview["session_id"],
                            selected_fields=["tags"],
                            overwrite_confirmed_fields=[],
                        )
                row = conn.execute("SELECT tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_APPLY_CONFLICT")
            self.assertEqual(row["tags_json"], '["darkwood-reverie"]')

    def test_no_change_only_apply_conflicts_if_session_finalized_concurrently(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release(conn, title="same-title", tags_json='["darkwood-reverie"]')
                self._seed_defaults(conn)
                preview = svc.create_preview_session(conn, release_id=release_id, requested_fields=["tags"], sources={}, created_by="u", ttl_seconds=1800)
                original = svc._mark_session_applied_open_only

                def _mark_after_external_apply(*args, **kwargs):
                    conn.execute(
                        "UPDATE metadata_preview_sessions SET session_status = 'APPLIED', applied_at = ? WHERE id = ?",
                        ("2026-01-01T00:00:00+00:00", preview["session_id"]),
                    )
                    return original(*args, **kwargs)

                with mock.patch("services.metadata.preview_apply_service._mark_session_applied_open_only", side_effect=_mark_after_external_apply):
                    with self.assertRaises(svc.MetadataPreviewApplyError) as ctx:
                        svc.apply_preview_session(
                            conn,
                            session_id=preview["session_id"],
                            selected_fields=["tags"],
                            overwrite_confirmed_fields=[],
                        )
            finally:
                conn.close()
            self.assertEqual(ctx.exception.code, "MPA_APPLY_CONFLICT")


if __name__ == "__main__":
    unittest.main()
