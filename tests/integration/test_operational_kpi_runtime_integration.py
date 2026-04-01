from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.operational_kpi_runtime import (
    create_recompute_run,
    list_operational_problems,
    read_operational_kpis,
    recompute_operational_kpis,
)
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestOperationalKpiRuntimeIntegration(unittest.TestCase):
    def _seed_release_job(self, conn) -> int:
        channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
        assert channel is not None
        release_id = int(
            conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'runtime-kpi', 'd', '[]', 'meta-runtime-kpi', ?)",
                (int(channel["id"]), dbm.now_ts()),
            ).lastrowid
        )
        job_id = int(
            dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=release_id,
                job_type="UI",
                state="FAILED",
                stage="QA",
                priority=0,
                attempt=0,
                created_at=dbm.now_ts() - 90000,
                updated_at=dbm.now_ts(),
            )
        )
        conn.execute(
            "INSERT INTO qa_reports(job_id, hard_ok, warnings_json, info_json, duration_expected, duration_actual, vcodec, acodec, fps, width, height, sr, ch, mean_volume_db, max_volume_db, created_at) VALUES(?, 0, '[]', '{}', 60.0, 58.0, 'h264', 'aac', 24.0, 1920, 1080, 44100, 2, -14.0, -1.0, ?)",
            (job_id, dbm.now_ts()),
        )
        conn.execute(
            "INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at) VALUES('darkwood-reverie', 'LONG', 'p', '2026-04-01T00:00:00Z', NULL, 'FAILED', '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z')"
        )
        return release_id

    def test_recompute_scopes_and_snapshot_supersession(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._seed_release_job(conn)
                recompute_operational_kpis(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie", recompute_mode="FULL_RECOMPUTE")
                recompute_operational_kpis(conn, target_scope_type="RELEASE", target_scope_ref=str(release_id), recompute_mode="FULL_RECOMPUTE")
                recompute_operational_kpis(conn, target_scope_type="BATCH_MONTH", target_scope_ref="2026-04", recompute_mode="INCREMENTAL_RECOMPUTE")
                recompute_operational_kpis(conn, target_scope_type="PORTFOLIO", target_scope_ref="core", recompute_mode="TARGETED_RECOMPUTE")
                recompute_operational_kpis(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie", recompute_mode="FULL_RECOMPUTE")

                rows = read_operational_kpis(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie", current_only=True)
                self.assertEqual(len(rows), 9)
                superseded = conn.execute(
                    "SELECT COUNT(*) AS c FROM analytics_operational_kpi_snapshots WHERE scope_type='CHANNEL' AND scope_ref='darkwood-reverie' AND is_current = 0"
                ).fetchone()
                self.assertGreater(int(superseded["c"]), 0)
            finally:
                conn.close()

    def test_partial_run_keeps_successful_snapshots_and_problem_listing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_release_job(conn)
                create_recompute_run(
                    conn,
                    target_scope_type="RELEASE",
                    target_scope_ref="99999",
                    recompute_mode="FULL_RECOMPUTE",
                    observed_from=None,
                    observed_to=None,
                )
                with self.assertRaises(AnalyticsDomainError):
                    create_recompute_run(
                        conn,
                        target_scope_type="RELEASE",
                        target_scope_ref="99999",
                        recompute_mode="FULL_RECOMPUTE",
                        observed_from=None,
                        observed_to=None,
                    )

                recompute_operational_kpis(conn, target_scope_type="CHANNEL", target_scope_ref="darkwood-reverie", recompute_mode="FULL_RECOMPUTE")
                problems = list_operational_problems(conn, scope_type="CHANNEL")
                self.assertGreaterEqual(len(problems), 1)
                self.assertTrue(all(p["status_class"] in {"ANOMALY", "RISK"} for p in problems))
            finally:
                conn.close()

    def test_missing_source_snapshots_explicit_failure(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(AnalyticsDomainError):
                    recompute_operational_kpis(conn, target_scope_type="RELEASE", target_scope_ref="999999", recompute_mode="FULL_RECOMPUTE")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
