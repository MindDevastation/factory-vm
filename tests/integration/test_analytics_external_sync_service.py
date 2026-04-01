from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.external_sync import (
    create_or_update_youtube_video_link,
    create_sync_run,
    link_channel_identity,
    link_release_video_context,
    transition_sync_run,
)
from services.analytics_center.write_service import write_external_identity
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.analytics_fixtures import make_sync_run_payload


class TestAnalyticsExternalSyncService(unittest.TestCase):
    def _seed_release_job(self, conn) -> tuple[int, int, int]:
        channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
        assert channel is not None
        channel_id = int(channel["id"])
        release_id = int(
            conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'ext-sync', 'd', '[]', 'meta-ext-sync', ?)",
                (channel_id, dbm.now_ts()),
            ).lastrowid
        )
        job_id = int(
            dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=release_id,
                job_type="UI",
                state="DRAFT",
                stage="DRAFT",
                priority=0,
                attempt=0,
                created_at=dbm.now_ts(),
                updated_at=dbm.now_ts(),
            )
        )
        return channel_id, release_id, job_id

    def test_sync_run_creation_lifecycle_and_scope_status(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                run_id = create_sync_run(conn, **make_sync_run_payload())
                row = conn.execute("SELECT sync_state FROM analytics_external_sync_runs WHERE id = ?", (run_id,)).fetchone()
                self.assertEqual(row["sync_state"], "RUNNING")

                transition_sync_run(
                    conn,
                    run_id=run_id,
                    to_sync_state="PARTIAL",
                    metric_families_returned=["views"],
                    metric_families_unavailable=["ctr"],
                    incomplete_backfill=True,
                    freshness_status="PARTIAL",
                    freshness_basis="window_end",
                )
                after = conn.execute("SELECT sync_state, error_code FROM analytics_external_sync_runs WHERE id = ?", (run_id,)).fetchone()
                self.assertEqual(after["sync_state"], "PARTIAL")
                self.assertEqual(after["error_code"], "E5A_INCOMPLETE_BACKFILL")

                status = conn.execute(
                    "SELECT sync_state, freshness_status, availability_status FROM analytics_external_scope_status WHERE provider_name='YOUTUBE' AND target_scope_type='CHANNEL' AND target_scope_ref='darkwood-reverie'"
                ).fetchone()
                self.assertEqual(status["sync_state"], "PARTIAL")
                self.assertEqual(status["freshness_status"], "PARTIAL")
            finally:
                conn.close()

    def test_scope_concurrency_guard(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                create_sync_run(conn, **make_sync_run_payload())
                with self.assertRaises(AnalyticsDomainError):
                    create_sync_run(conn, **make_sync_run_payload())
            finally:
                conn.close()

    def test_identity_linkage_release_job_upload_context(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id, release_id, job_id = self._seed_release_job(conn)
                write_external_identity(
                    conn,
                    entity_type="CHANNEL",
                    entity_ref=str(channel_id),
                    source_family="EXTERNAL_YOUTUBE",
                    external_namespace="youtube",
                    external_id="yt-channel-xyz",
                    payload_json={"slug": "darkwood-reverie"},
                )
                self.assertEqual(link_channel_identity(conn, channel_slug="darkwood-reverie"), "yt-channel-xyz")

                conn.execute(
                    "INSERT INTO youtube_uploads(job_id, video_id, url, studio_url, privacy, uploaded_at, error) VALUES(?, 'yt-video-123', 'https://yt.example/v/1', 'https://studio.example/v/1', 'public', ?, NULL)",
                    (job_id, dbm.now_ts()),
                )
                create_or_update_youtube_video_link(
                    conn,
                    channel_slug="darkwood-reverie",
                    youtube_video_id="yt-video-123",
                    release_id=release_id,
                    job_id=job_id,
                    youtube_channel_id="yt-channel-xyz",
                    linkage_confidence="EXACT",
                    linkage_source="UPLOAD_ARTIFACT",
                    payload_json={"context": "upload"},
                )
                linkage = link_release_video_context(conn, release_id=release_id)
                assert linkage is not None
                self.assertEqual(linkage["youtube_video_id"], "yt-video-123")
                self.assertEqual(linkage["uploaded_video_id"], "yt-video-123")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
