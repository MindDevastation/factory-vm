from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.write_service import (
    write_external_identity,
    write_rollup_link,
    write_scope_link,
    write_snapshot,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.analytics_fixtures import make_snapshot_input


class TestAnalyticsWriteService(unittest.TestCase):
    def _seed_release_job(self, conn) -> tuple[int, int, int]:
        channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
        assert channel is not None
        channel_id = int(channel["id"])
        release_id = int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, 'analytics-release', 'd', '[]', NULL, NULL, 'meta-e5a-mf1', ?)
                """,
                (channel_id, dbm.now_ts()),
            ).lastrowid
        )
        job_id = dbm.insert_job_with_lineage_defaults(
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
        return channel_id, release_id, int(job_id)

    def test_multi_entity_snapshot_writes_and_linkage(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id, release_id, job_id = self._seed_release_job(conn)
                channel_snapshot = write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id)))
                release_snapshot = write_snapshot(conn, make_snapshot_input(entity_type="RELEASE", entity_ref=str(release_id)))
                batch_snapshot = write_snapshot(
                    conn,
                    make_snapshot_input(
                        entity_type="BATCH",
                        entity_ref="2026-03",
                        window_type="MONTHLY_BATCH",
                        snapshot_status="PARTIAL",
                        freshness_status="PARTIAL",
                    ),
                )
                job_snapshot = write_snapshot(conn, make_snapshot_input(entity_type="JOB_RUNTIME", entity_ref=str(job_id)))
                portfolio_snapshot = write_snapshot(
                    conn,
                    make_snapshot_input(entity_type="PORTFOLIO", entity_ref="portfolio-core", snapshot_status="FAILED", freshness_status="UNKNOWN", is_current=False),
                )

                baseline_snapshot = write_snapshot(conn, make_snapshot_input(entity_type="RELEASE", entity_ref=str(release_id), is_current=False, snapshot_status="HISTORICAL"))
                with_baseline = make_snapshot_input(entity_type="RELEASE", entity_ref=str(release_id), comparison_baseline_snapshot_id=baseline_snapshot)
                with_baseline_id = write_snapshot(conn, with_baseline)

                write_scope_link(
                    conn,
                    entity_type="RELEASE",
                    entity_ref=str(release_id),
                    channel_id=channel_id,
                    release_id=release_id,
                    job_id=job_id,
                    batch_ref="2026-03",
                    portfolio_ref="portfolio-core",
                    payload_json={"scope": "release_job_batch"},
                )
                write_external_identity(
                    conn,
                    entity_type="CHANNEL",
                    entity_ref=str(channel_id),
                    source_family="EXTERNAL_YOUTUBE",
                    external_namespace="youtube",
                    external_id="yt-channel-1",
                    payload_json={"handle": "darkwood"},
                )
                write_rollup_link(
                    conn,
                    parent_snapshot_id=release_snapshot,
                    child_snapshot_id=job_snapshot,
                    relation_type="RELEASE_TO_JOB_RUNTIME",
                    payload_json={"edge": "runtime"},
                )
                write_rollup_link(
                    conn,
                    parent_snapshot_id=release_snapshot,
                    child_snapshot_id=batch_snapshot,
                    relation_type="RELEASE_TO_BATCH",
                    payload_json={"edge": "batch"},
                )

                current = conn.execute(
                    "SELECT id, snapshot_status, is_current, comparison_baseline_snapshot_id FROM analytics_snapshots WHERE id = ?",
                    (with_baseline_id,),
                ).fetchone()
                self.assertEqual(int(current["comparison_baseline_snapshot_id"]), baseline_snapshot)
                self.assertEqual(int(current["is_current"]), 1)

                counts = conn.execute("SELECT COUNT(*) AS c FROM analytics_snapshots").fetchone()
                self.assertGreaterEqual(int(counts["c"]), 7)
                self.assertGreater(channel_snapshot, 0)
                self.assertGreater(portfolio_snapshot, 0)
            finally:
                conn.close()

    def test_current_snapshot_supersedes_transactionally(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                first = write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id)))
                second = write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id)))

                first_row = conn.execute("SELECT snapshot_status, is_current FROM analytics_snapshots WHERE id = ?", (first,)).fetchone()
                second_row = conn.execute("SELECT snapshot_status, is_current FROM analytics_snapshots WHERE id = ?", (second,)).fetchone()
                self.assertEqual(first_row["snapshot_status"], "SUPERSEDED")
                self.assertEqual(int(first_row["is_current"]), 0)
                self.assertEqual(second_row["snapshot_status"], "CURRENT")
                self.assertEqual(int(second_row["is_current"]), 1)
            finally:
                conn.close()

    def test_external_identity_uniqueness_conflict(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                write_external_identity(
                    conn,
                    entity_type="CHANNEL",
                    entity_ref=str(channel_id),
                    source_family="EXTERNAL_YOUTUBE",
                    external_namespace="youtube",
                    external_id="duplicate-id",
                    payload_json={"x": 1},
                )
                with self.assertRaises(AnalyticsDomainError) as ctx:
                    write_external_identity(
                        conn,
                        entity_type="CHANNEL",
                        entity_ref=str(channel_id),
                        source_family="EXTERNAL_YOUTUBE",
                        external_namespace="youtube",
                        external_id="duplicate-id",
                        payload_json={"x": 2},
                    )
                self.assertEqual(ctx.exception.code, "E5A_EXTERNAL_IDENTITY_CONFLICT")
            finally:
                conn.close()



if __name__ == "__main__":
    unittest.main()
