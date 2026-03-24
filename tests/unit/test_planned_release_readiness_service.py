from __future__ import annotations

import unittest

from services.common import db as dbm
from services.common.env import Env
from services.planner.planned_release_readiness_service import PlannedReleaseReadinessService
from tests._helpers import seed_minimal_db, temp_env


class TestPlannedReleaseReadinessService(unittest.TestCase):
    def _insert_planned_release(self, *, env, channel_slug: str = "darkwood-reverie", publish_at: str | None = "2025-05-01T10:00:00+02:00") -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES (?, 'LONG', 'Title', ?, 'notes', 'PLANNED', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """,
                (channel_slug, publish_at),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def _insert_linked_release(self, *, env, planned_release_id: int, title: str = "T", description: str = "D", tags_json: str = '["a"]') -> int:
        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert channel
            cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES (?, ?, ?, ?, '2025-05-01T10:00:00+02:00', NULL, ?, 1.0)
                """,
                (int(channel["id"]), title, description, tags_json, f"meta-{planned_release_id}"),
            )
            release_id = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES (?, ?, '2025-01-01T00:00:00Z', 'u')",
                (planned_release_id, release_id),
            )
            return release_id
        finally:
            conn.close()

    def _insert_defaults(self, *, env, invalid: bool = False) -> None:
        conn = dbm.connect(env)
        try:
            title_status = "ARCHIVED" if invalid else "ACTIVE"
            title_validation = "INVALID" if invalid else "VALID"
            conn.execute(
                """
                INSERT INTO title_templates(id, channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (101, 'darkwood-reverie', 't', 'x', ?, 1, ?, '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """,
                (title_status, title_validation),
            )
            conn.execute(
                """
                INSERT INTO description_templates(id, channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (102, 'darkwood-reverie', 'd', 'x', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO video_tag_presets(id, channel_slug, preset_name, preset_body_json, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (103, 'darkwood-reverie', 'v', '["a"]', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO channel_metadata_defaults(channel_slug, default_title_template_id, default_description_template_id, default_video_tag_preset_id, created_at, updated_at)
                VALUES ('darkwood-reverie', 101, 102, 103, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
        finally:
            conn.close()

    def _insert_playlist_settings_and_tracks(self, *, env, invalid_settings: bool = False) -> None:
        conn = dbm.connect(env)
        try:
            conn.execute(
                """
                INSERT INTO playlist_builder_channel_settings(
                    channel_slug, default_generation_mode, min_duration_min, max_duration_min,
                    tolerance_min, preferred_month_batch, preferred_batch_ratio, allow_cross_channel,
                    novelty_target_min, novelty_target_max, position_memory_window,
                    strictness_mode, vocal_policy, reuse_policy, created_at, updated_at
                ) VALUES ('darkwood-reverie', 'AUTO', ?, ?, 1, NULL, 70, 0, 0.5, 0.8, 10, 'balanced', 'allow', 'avoid_recent', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """,
                (40 if invalid_settings else 10, 20 if invalid_settings else 40),
            )
            if not invalid_settings:
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, filename, discovered_at)
                    VALUES ('darkwood-reverie', 't-1', 'g-1', 'a.wav', 1.0)
                    """
                )
        finally:
            conn.close()

    def _insert_playlist_and_visual(self, *, env, release_id: int, empty_playlist: bool = False, partial_background: bool = False) -> None:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, created_at, updated_at) VALUES (?, 'RENDER', 'DRAFT', 'FETCH', 1, 1.0, 1.0)",
                (release_id,),
            )
            job_id = int(cur.lastrowid)
            if job_id != 1:
                conn.execute("UPDATE jobs SET root_job_id = ? WHERE id = ?", (job_id, job_id))
            ph = conn.execute(
                "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, is_active, created_at) VALUES (1, 'darkwood-reverie', ?, 'DRAFT', 'AUTO', 'balanced', 100.0, 1, 's', 'o', 'n3', 'n5', 1, '2025-01-01T00:00:00Z')",
                (job_id,),
            )
            assert ph is not None
            if not empty_playlist:
                conn.execute(
                    "INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES (1, 1, 0, 1, '2025-05', 100.0, 'darkwood-reverie')"
                )
            conn.execute(
                "INSERT INTO ui_job_drafts(job_id, channel_id, title, description, tags_csv, cover_name, cover_ext, background_name, background_ext, audio_ids_text, created_at, updated_at) VALUES (?, ?, 't', 'd', 'a', NULL, NULL, ?, ?, '1', 1.0, 1.0)",
                (job_id, 1, "bg" if not partial_background else "bg", "png" if not partial_background else ""),
            )
        finally:
            conn.close()

    def test_scheduling_missing_is_not_ready(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env, publish_at=None)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["scheduling"]["status"], "NOT_READY")
            self.assertIn("PRR_SCHEDULING_MISSING", [c["code"] for c in out["domains"]["scheduling"]["checks"]])

    def test_scheduling_invalid_is_blocked(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env, publish_at="bad")
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["scheduling"]["status"], "BLOCKED")

    def test_all_domains_ready_when_linked_assets_and_sources_exist(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            release_id = self._insert_linked_release(env=env, planned_release_id=pr_id)
            self._insert_defaults(env=env)
            self._insert_playlist_settings_and_tracks(env=env)
            self._insert_playlist_and_visual(env=env, release_id=release_id)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["aggregate_status"], "READY_FOR_MATERIALIZATION")

    def test_link_absent_uses_fallbacks(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            self._insert_defaults(env=env)
            self._insert_playlist_settings_and_tracks(env=env)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["visual_assets"]["status"], "NOT_READY")
            self.assertEqual(out["domains"]["metadata"]["status"], "READY")
            self.assertEqual(out["domains"]["playlist"]["status"], "READY")

    def test_metadata_source_missing_is_not_ready(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["metadata"]["status"], "NOT_READY")

    def test_metadata_invalid_default_is_blocked(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            self._insert_defaults(env=env, invalid=True)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["metadata"]["status"], "BLOCKED")

    def test_playlist_invalid_settings_blocks(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            self._insert_defaults(env=env)
            self._insert_playlist_settings_and_tracks(env=env, invalid_settings=True)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["playlist"]["status"], "BLOCKED")

    def test_visual_partial_background_blocks(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            release_id = self._insert_linked_release(env=env, planned_release_id=pr_id)
            self._insert_defaults(env=env)
            self._insert_playlist_settings_and_tracks(env=env)
            self._insert_playlist_and_visual(env=env, release_id=release_id, partial_background=True)
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["domains"]["visual_assets"]["status"], "BLOCKED")

    def test_blocked_precedence_and_primary_reason_ordering(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env, publish_at="bad")
            conn = dbm.connect(env)
            try:
                out = PlannedReleaseReadinessService(conn).evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(out["aggregate_status"], "BLOCKED")
            self.assertIsNotNone(out["primary_reason"])
            self.assertEqual(out["primary_reason"]["domain"], "scheduling")
            self.assertTrue(out["primary_remediation_hint"])

    def test_determinism_for_same_state(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            pr_id = self._insert_planned_release(env=env)
            conn = dbm.connect(env)
            try:
                svc = PlannedReleaseReadinessService(conn)
                a = svc.evaluate(planned_release_id=pr_id)
                b = svc.evaluate(planned_release_id=pr_id)
            finally:
                conn.close()
            self.assertEqual(a["aggregate_status"], b["aggregate_status"])
            self.assertEqual(a["reasons"], b["reasons"])
            self.assertEqual(a["domains"], b["domains"])


if __name__ == "__main__":
    unittest.main()
