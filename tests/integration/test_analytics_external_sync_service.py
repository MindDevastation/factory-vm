from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.external_sync import (
    create_or_update_youtube_video_link,
    create_sync_run,
    link_channel_identity,
    link_release_video_context,
    transition_sync_run,
    run_external_youtube_ingestion,
)
from services.analytics_center.mf4_runtime import read_mf4_baselines, recompute_mf4
from services.analytics_center.write_service import write_external_identity
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.analytics_fixtures import make_sync_run_payload


class TestAnalyticsExternalSyncService(unittest.TestCase):
    class _FakeProvider:
        def __init__(self, payload: dict):
            self.payload = payload

        def fetch_channel_metrics(self, **_: object) -> dict:
            return dict(self.payload)

        def fetch_video_metrics(self, **_: object) -> dict:
            return dict(self.payload)

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

    def test_initial_backfill_writes_external_snapshot(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                run_id = create_sync_run(conn, **make_sync_run_payload(run_mode="INITIAL_BACKFILL"))
                provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 10, "impressions": 1000},
                        "metric_families_returned": ["views", "impressions"],
                        "metric_families_unavailable": [],
                        "freshness_status": "FRESH",
                        "freshness_basis": "window_end",
                        "incomplete_backfill": False,
                    }
                )
                snapshot_id = run_external_youtube_ingestion(
                    conn,
                    run_id=run_id,
                    provider=provider,
                    channel_slug="darkwood-reverie",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                )
                self.assertIsNotNone(snapshot_id)
                snap = conn.execute("SELECT source_family, snapshot_status, entity_ref FROM analytics_snapshots WHERE id = ?", (snapshot_id,)).fetchone()
                self.assertEqual(snap["source_family"], "EXTERNAL_YOUTUBE")
                self.assertEqual(snap["snapshot_status"], "CURRENT")
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                self.assertEqual(str(snap["entity_ref"]), str(channel_id))
            finally:
                conn.close()

    def test_mf2_written_channel_snapshot_feeds_mf4_channel_recompute(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                run_id = create_sync_run(conn, **make_sync_run_payload(run_mode="INITIAL_BACKFILL"))
                provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 42, "impressions": 4200},
                        "metric_families_returned": ["views", "impressions"],
                        "metric_families_unavailable": [],
                        "freshness_status": "FRESH",
                        "freshness_basis": "window_end",
                        "incomplete_backfill": False,
                    }
                )
                run_external_youtube_ingestion(
                    conn,
                    run_id=run_id,
                    provider=provider,
                    channel_slug="darkwood-reverie",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                )
                recompute_mf4(
                    conn,
                    run_kind="FULL_STACK_RECOMPUTE",
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                baselines = read_mf4_baselines(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=True)
                self.assertGreaterEqual(len(baselines), 1)
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                self.assertTrue(all(str(row["scope_ref"]) == str(channel_id) for row in baselines))
            finally:
                conn.close()

    def test_scheduled_sync_supersedes_current_snapshot(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 11},
                        "metric_families_returned": ["views"],
                        "metric_families_unavailable": [],
                        "freshness_status": "FRESH",
                        "freshness_basis": "window_end",
                        "incomplete_backfill": False,
                    }
                )
                r1 = create_sync_run(conn, **make_sync_run_payload(run_mode="SCHEDULED_SYNC"))
                s1 = run_external_youtube_ingestion(conn, run_id=r1, provider=provider, channel_slug="darkwood-reverie", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                r2 = create_sync_run(conn, **make_sync_run_payload(run_mode="SCHEDULED_SYNC"))
                s2 = run_external_youtube_ingestion(conn, run_id=r2, provider=provider, channel_slug="darkwood-reverie", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                row1 = conn.execute("SELECT snapshot_status, is_current FROM analytics_snapshots WHERE id = ?", (s1,)).fetchone()
                row2 = conn.execute("SELECT snapshot_status, is_current FROM analytics_snapshots WHERE id = ?", (s2,)).fetchone()
                self.assertEqual(row1["snapshot_status"], "SUPERSEDED")
                self.assertEqual(int(row2["is_current"]), 1)
            finally:
                conn.close()

    def test_manual_refresh_release_video_scope(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                _channel_id, release_id, job_id = self._seed_release_job(conn)
                create_or_update_youtube_video_link(
                    conn,
                    channel_slug="darkwood-reverie",
                    youtube_video_id="yt-video-900",
                    release_id=release_id,
                    job_id=job_id,
                    youtube_channel_id="yt-channel-xyz",
                    linkage_confidence="EXACT",
                    linkage_source="UPLOAD_ARTIFACT",
                    payload_json={"context": "upload"},
                )
                run_id = create_sync_run(conn, **make_sync_run_payload(target_scope_type="RELEASE_VIDEO", target_scope_ref="yt-video-900", run_mode="MANUAL_REFRESH"))
                provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 120},
                        "metric_families_returned": ["views"],
                        "metric_families_unavailable": [],
                        "freshness_status": "FRESH",
                        "freshness_basis": "window_end",
                        "incomplete_backfill": False,
                    }
                )
                sid = run_external_youtube_ingestion(conn, run_id=run_id, provider=provider, channel_slug="darkwood-reverie", target_scope_type="RELEASE_VIDEO", target_scope_ref="yt-video-900")
                snap = conn.execute("SELECT entity_type, entity_ref FROM analytics_snapshots WHERE id = ?", (sid,)).fetchone()
                self.assertEqual(snap["entity_type"], "RELEASE")
                self.assertEqual(int(snap["entity_ref"]), release_id)
            finally:
                conn.close()

    def test_partial_refresh_and_stale_resync(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                partial_provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 5},
                        "metric_families_returned": ["views"],
                        "metric_families_unavailable": ["monetization"],
                        "freshness_status": "PARTIAL",
                        "freshness_basis": "window_end",
                        "incomplete_backfill": True,
                    }
                )
                run_partial = create_sync_run(
                    conn,
                    **make_sync_run_payload(
                        run_mode="PARTIAL_REFRESH",
                        metric_families_requested=["views", "impressions", "ctr", "monetization"],
                    ),
                )
                sid = run_external_youtube_ingestion(conn, run_id=run_partial, provider=partial_provider, channel_slug="darkwood-reverie", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                snap = conn.execute("SELECT snapshot_status FROM analytics_snapshots WHERE id = ?", (sid,)).fetchone()
                self.assertEqual(snap["snapshot_status"], "PARTIAL")

                stale_provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {"views": 6},
                        "metric_families_returned": ["views"],
                        "metric_families_unavailable": [],
                        "freshness_status": "STALE",
                        "freshness_basis": "stale_threshold",
                        "incomplete_backfill": False,
                    }
                )
                run_stale = create_sync_run(conn, **make_sync_run_payload(run_mode="STALE_RESYNC"))
                run_external_youtube_ingestion(conn, run_id=run_stale, provider=stale_provider, channel_slug="darkwood-reverie", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                status = conn.execute("SELECT freshness_status FROM analytics_external_scope_status WHERE provider_name='YOUTUBE' AND target_scope_type='CHANNEL' AND target_scope_ref='darkwood-reverie'").fetchone()
                self.assertEqual(status["freshness_status"], "STALE")
            finally:
                conn.close()

    def test_source_unavailable_fails_run_without_blocking_internal_reads(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                run_id = create_sync_run(conn, **make_sync_run_payload(run_mode="MANUAL_REFRESH"))
                failed_provider = self._FakeProvider(
                    {
                        "channel_slug": "darkwood-reverie",
                        "metrics": {},
                        "metric_families_returned": [],
                        "metric_families_unavailable": ["views", "impressions", "ctr"],
                        "freshness_status": "UNKNOWN",
                        "freshness_basis": "source_unavailable",
                        "incomplete_backfill": False,
                        "source_unavailable": True,
                    }
                )
                run_external_youtube_ingestion(conn, run_id=run_id, provider=failed_provider, channel_slug="darkwood-reverie", target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie")
                run = conn.execute("SELECT sync_state FROM analytics_external_sync_runs WHERE id = ?", (run_id,)).fetchone()
                self.assertEqual(run["sync_state"], "FAILED")
                rows = conn.execute("SELECT COUNT(*) AS c FROM analytics_snapshots WHERE source_family='INTERNAL_OPERATIONAL'").fetchone()
                self.assertEqual(int(rows["c"]), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
