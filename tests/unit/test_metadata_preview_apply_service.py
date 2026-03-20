from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

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


if __name__ == "__main__":
    unittest.main()
