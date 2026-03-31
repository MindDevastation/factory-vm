from __future__ import annotations

import unittest

from services.analytics_center.literals import (
    ANALYTICS_ENTITY_TYPES,
    ANALYTICS_FRESHNESS_STATUSES,
    ANALYTICS_ROLLUP_RELATION_TYPES,
    ANALYTICS_SNAPSHOT_STATUSES,
    ANALYTICS_SOURCE_FAMILIES,
    ANALYTICS_WINDOW_TYPES,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestAnalyticsSchemaFoundation(unittest.TestCase):
    def test_literals_match_frozen_contract(self) -> None:
        self.assertEqual(
            ANALYTICS_ENTITY_TYPES,
            ("CHANNEL", "RELEASE", "BATCH", "JOB_RUNTIME", "PORTFOLIO"),
        )
        self.assertEqual(
            ANALYTICS_SOURCE_FAMILIES,
            (
                "EXTERNAL_YOUTUBE",
                "INTERNAL_OPERATIONAL",
                "DERIVED_ROLLUP",
                "COMPARISON_BASELINE",
                "EXPLAINABILITY_OUTPUT",
            ),
        )
        self.assertEqual(
            ANALYTICS_WINDOW_TYPES,
            (
                "POINT_IN_TIME",
                "BOUNDED_WINDOW",
                "ROLLING_BASELINE",
                "LAST_KNOWN_CURRENT",
                "MONTHLY_BATCH",
            ),
        )
        self.assertEqual(
            ANALYTICS_SNAPSHOT_STATUSES,
            ("CURRENT", "HISTORICAL", "SUPERSEDED", "PARTIAL", "FAILED"),
        )
        self.assertEqual(
            ANALYTICS_FRESHNESS_STATUSES,
            ("FRESH", "STALE", "PARTIAL", "UNKNOWN"),
        )
        self.assertEqual(
            ANALYTICS_ROLLUP_RELATION_TYPES,
            ("CHANNEL_TO_RELEASE", "RELEASE_TO_JOB_RUNTIME", "RELEASE_TO_BATCH", "PORTFOLIO_TO_ENTITY"),
        )

    def test_migrate_creates_required_analytics_tables_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                tables = {
                    str(row["name"])
                    for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                for table in (
                    "analytics_external_identities",
                    "analytics_scope_links",
                    "analytics_snapshots",
                    "analytics_rollup_links",
                ):
                    self.assertIn(table, tables)

                snapshots_cols = {
                    str(row["name"]) for row in conn.execute("PRAGMA table_info(analytics_snapshots)").fetchall()
                }
                self.assertIn("comparison_baseline_snapshot_id", snapshots_cols)
                self.assertIn("payload_json", snapshots_cols)
                self.assertIn("explainability_json", snapshots_cols)
                self.assertIn("lineage_json", snapshots_cols)
                self.assertIn("anomaly_markers_json", snapshots_cols)

                indexes = {
                    str(row["name"]) for row in conn.execute("PRAGMA index_list(analytics_snapshots)").fetchall()
                }
                self.assertIn("idx_as_current_scope_unique", indexes)
                self.assertIn("idx_as_read_filters", indexes)
                self.assertIn("idx_as_scope_current", indexes)

                aei_indexes = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA index_list(analytics_external_identities)").fetchall()
                }
                self.assertIn("idx_aei_entity", aei_indexes)
                self.assertIn("idx_aei_source_external", aei_indexes)
            finally:
                conn.close()

    def test_migration_regression_does_not_break_release_job_planner_foundation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
                assert channel is not None
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'analytics-smoke', 'd', '[]', NULL, NULL, 'meta-analytics-smoke', ?)
                        """,
                        (int(channel["id"]), dbm.now_ts()),
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
                self.assertGreater(job_id, 0)
                planned_id = conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                    VALUES('darkwood-reverie', 'LONG', 'p', '2026-03-01T00:00:00Z', NULL, 'PLANNED', '2026-03-01T00:00:00Z', '2026-03-01T00:00:00Z')
                    """
                ).lastrowid
                self.assertGreater(int(planned_id), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
