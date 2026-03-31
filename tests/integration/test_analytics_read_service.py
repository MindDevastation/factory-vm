from __future__ import annotations

import unittest

from services.analytics_center.read_service import (
    SnapshotReadFilters,
    read_linkage_for_scope,
    read_snapshots,
    resolve_current_snapshot,
)
from services.analytics_center.write_service import write_rollup_link, write_scope_link, write_snapshot
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.analytics_fixtures import make_snapshot_input


class TestAnalyticsReadService(unittest.TestCase):
    def test_snapshot_read_filtering(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id), source_family="INTERNAL_OPERATIONAL"))
                write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id), source_family="DERIVED_ROLLUP", is_current=False))
                rows = read_snapshots(
                    conn,
                    SnapshotReadFilters(
                        entity_type="CHANNEL",
                        entity_ref=str(channel_id),
                        source_family="INTERNAL_OPERATIONAL",
                        window_type="LAST_KNOWN_CURRENT",
                        current_only=True,
                    ),
                )
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["source_family"], "INTERNAL_OPERATIONAL")
            finally:
                conn.close()

    def test_linkage_read_inspection(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'r', 'd', '[]', 'meta-read', ?)",
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
                write_scope_link(
                    conn,
                    entity_type="RELEASE",
                    entity_ref=str(release_id),
                    channel_id=channel_id,
                    release_id=release_id,
                    job_id=job_id,
                    batch_ref="2026-03",
                    portfolio_ref="portfolio-a",
                    payload_json={"scope": "ok"},
                )
                parent = write_snapshot(conn, make_snapshot_input(entity_type="RELEASE", entity_ref=str(release_id)))
                child = write_snapshot(conn, make_snapshot_input(entity_type="JOB_RUNTIME", entity_ref=str(job_id)))
                write_rollup_link(conn, parent_snapshot_id=parent, child_snapshot_id=child, relation_type="RELEASE_TO_JOB_RUNTIME", payload_json={"k": "v"})

                linkage = read_linkage_for_scope(conn, entity_type="RELEASE", entity_ref=str(release_id))
                assert linkage is not None
                self.assertEqual(int(linkage["release_id"]), release_id)
                self.assertGreaterEqual(len(linkage["rollup_links"]), 1)
            finally:
                conn.close()

    def test_current_snapshot_resolution(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id), snapshot_status="CURRENT"))
                resolved = resolve_current_snapshot(
                    conn,
                    entity_type="CHANNEL",
                    entity_ref=str(channel_id),
                    source_family="INTERNAL_OPERATIONAL",
                    window_type="LAST_KNOWN_CURRENT",
                )
                assert resolved is not None
                self.assertEqual(int(resolved["is_current"]), 1)
                self.assertEqual(resolved["snapshot_status"], "CURRENT")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
