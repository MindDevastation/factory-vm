from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.operational_kpi import derive_operational_kpis
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env
from tests.operational_kpi_fixtures import seed_scope_isolation_jobs


class TestOperationalKpiDerivationService(unittest.TestCase):
    def _seed_release_job(self, conn) -> tuple[int, int]:
        channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
        assert channel is not None
        release_id = int(
            conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'kpi-release', 'd', '[]', 'meta-kpi', ?)",
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
            "INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at) VALUES('darkwood-reverie', 'LONG', 'p', '2026-04-01T00:00:00Z', NULL, 'FAILED', '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO qa_reports(job_id, hard_ok, warnings_json, info_json, duration_expected, duration_actual, vcodec, acodec, fps, width, height, sr, ch, mean_volume_db, max_volume_db, created_at) VALUES(?, 0, '[]', '{}', 60.0, 58.0, 'h264', 'aac', 24.0, 1920, 1080, 44100, 2, -14.0, -1.0, ?)",
            (job_id, dbm.now_ts()),
        )
        return release_id, job_id

    def test_derive_operational_kpis_for_all_scopes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, _job_id = self._seed_release_job(conn)
                outputs_channel = derive_operational_kpis(conn, scope_type="CHANNEL", scope_ref="darkwood-reverie")
                outputs_release = derive_operational_kpis(conn, scope_type="RELEASE", scope_ref=str(release_id))
                outputs_batch = derive_operational_kpis(conn, scope_type="BATCH_MONTH", scope_ref="2026-04")
                outputs_portfolio = derive_operational_kpis(conn, scope_type="PORTFOLIO", scope_ref="core")
                self.assertEqual(len(outputs_channel), 9)
                self.assertEqual(len(outputs_release), 9)
                self.assertEqual(len(outputs_batch), 9)
                self.assertEqual(len(outputs_portfolio), 9)

                risk_or_anomaly = [o for o in outputs_channel if o.status_class in {"ANOMALY", "RISK"}]
                self.assertTrue(all(o.explainability_payload is not None for o in risk_or_anomaly))
            finally:
                conn.close()

    def test_missing_source_snapshots_failure(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(AnalyticsDomainError):
                    derive_operational_kpis(conn, scope_type="RELEASE", scope_ref="999999")
            finally:
                conn.close()

    def test_channel_and_batch_scope_isolation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                seeded = seed_scope_isolation_jobs(conn)
                out_a = derive_operational_kpis(conn, scope_type="CHANNEL", scope_ref=seeded["channel_a_slug"])
                out_b = derive_operational_kpis(conn, scope_type="CHANNEL", scope_ref=seeded["channel_b_slug"])
                batch_a = derive_operational_kpis(conn, scope_type="BATCH_MONTH", scope_ref="2026-04")
                batch_b = derive_operational_kpis(conn, scope_type="BATCH_MONTH", scope_ref="2026-05")

                readiness_a = next(k for k in out_a if k.kpi_family == "READINESS")
                readiness_b = next(k for k in out_b if k.kpi_family == "READINESS")
                qa_a = next(k for k in out_a if k.kpi_family == "QA_STATUS")
                qa_b = next(k for k in out_b if k.kpi_family == "QA_STATUS")
                batch_comp_a = next(k for k in batch_a if k.kpi_family == "BATCH_COMPLETENESS")
                batch_comp_b = next(k for k in batch_b if k.kpi_family == "BATCH_COMPLETENESS")

                self.assertNotEqual(readiness_a.value_payload["readiness_health_ratio"], readiness_b.value_payload["readiness_health_ratio"])
                self.assertNotEqual(qa_a.value_payload["qa_failure_ratio"], qa_b.value_payload["qa_failure_ratio"])
                self.assertNotEqual(batch_comp_a.value_payload["batch_completeness_ratio"], batch_comp_b.value_payload["batch_completeness_ratio"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
