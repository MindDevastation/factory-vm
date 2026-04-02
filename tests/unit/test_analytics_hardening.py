from __future__ import annotations

import logging
import unittest

from services.analytics_center.write_service import write_scope_link, write_snapshot
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.analytics_fixtures import make_snapshot_input


class TestAnalyticsHardening(unittest.TestCase):
    def test_events_emitted_for_snapshot_lifecycle_and_lineage(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                baseline = write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id), is_current=False, snapshot_status="HISTORICAL"))
                write_snapshot(
                    conn,
                    make_snapshot_input(
                        entity_type="CHANNEL",
                        entity_ref=str(channel_id),
                        comparison_baseline_snapshot_id=baseline,
                        snapshot_status="PARTIAL",
                        freshness_status="PARTIAL",
                    ),
                )
                write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id)))
                write_snapshot(
                    conn,
                    make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id), snapshot_status="FAILED", freshness_status="UNKNOWN", is_current=False),
                )
                events = {
                    str(r["event_type"]) for r in conn.execute("SELECT event_type FROM analytics_events").fetchall()
                }
                required = {
                    "SNAPSHOT_CREATED",
                    "SNAPSHOT_MARKED_CURRENT",
                    "SNAPSHOT_SUPERSEDED",
                    "BASELINE_REFERENCE_ATTACHED",
                    "LINEAGE_PAYLOAD_PERSISTED",
                    "PARTIAL_SNAPSHOT_STORED",
                    "FAILED_SNAPSHOT_STORED",
                }
                self.assertTrue(required.issubset(events))
            finally:
                conn.close()

    def test_structured_logging_fields_present(self) -> None:
        with temp_env() as (_td, env), self.assertLogs("services.analytics_center.write_service", level=logging.INFO) as logs:
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                write_snapshot(conn, make_snapshot_input(entity_type="CHANNEL", entity_ref=str(channel_id)))
            finally:
                conn.close()

            merged = "\n".join(logs.output)
            self.assertIn("entity_type=CHANNEL", merged)
            self.assertIn("entity_ref=", merged)
            self.assertIn("source_family=INTERNAL_OPERATIONAL", merged)
            self.assertIn("window_type=LAST_KNOWN_CURRENT", merged)
            self.assertIn("snapshot_status=CURRENT", merged)
            self.assertIn("freshness_status=FRESH", merged)
            self.assertIn("snapshot_id=", merged)

    def test_scope_linkage_events(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'r', 'd', '[]', 'meta-hardening', ?)",
                        (channel_id, dbm.now_ts()),
                    ).lastrowid
                )
                job_id = int(dbm.insert_job_with_lineage_defaults(conn, release_id=release_id, job_type="UI", state="DRAFT", stage="DRAFT", priority=0, attempt=0, created_at=dbm.now_ts(), updated_at=dbm.now_ts()))
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
                evt = conn.execute("SELECT event_type FROM analytics_events WHERE event_type IN ('LINKAGE_CREATED','LINKAGE_UPDATED')").fetchall()
                self.assertGreaterEqual(len(evt), 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
