from __future__ import annotations

import logging
import unittest
from unittest import mock

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.operational_kpi import KpiOutput
from services.analytics_center.operational_kpi_runtime import list_operational_problems, recompute_operational_kpis
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestOperationalKpiHardening(unittest.TestCase):
    def _seed_release_job(self, conn) -> int:
        channel = conn.execute("SELECT id FROM channels WHERE slug='darkwood-reverie'").fetchone()
        assert channel is not None
        release_id = int(
            conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'runtime-kpi', 'd', '[]', 'meta-runtime-kpi-hardening', ?)",
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

    def test_runtime_writes_required_lifecycle_audit_events(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_release_job(conn)
                recompute_operational_kpis(
                    conn,
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                # second run forces supersession event path
                recompute_operational_kpis(
                    conn,
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                with mock.patch(
                    "services.analytics_center.operational_kpi_runtime.derive_operational_kpis",
                    return_value=[
                        KpiOutput(
                            scope_type="CHANNEL",
                            scope_ref="darkwood-reverie",
                            kpi_family="PIPELINE_TIMING",
                            kpi_code="pipeline_latency",
                            status_class="ANOMALY",
                            value_payload={"v": 10},
                            explainability_payload={"primary_reason_code": "PIPELINE_TIMING_ANOMALY"},
                            source_snapshot_refs=["CHANNEL:darkwood-reverie"],
                        ),
                        KpiOutput(
                            scope_type="CHANNEL",
                            scope_ref="darkwood-reverie",
                            kpi_family="QA_STATUS",
                            kpi_code="qa_quality",
                            status_class="RISK",
                            value_payload={"v": 20},
                            explainability_payload={"primary_reason_code": "QA_STATUS_RISK"},
                            source_snapshot_refs=["CHANNEL:darkwood-reverie"],
                        ),
                    ],
                ):
                    recompute_operational_kpis(
                        conn,
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="TARGETED_RECOMPUTE",
                    )
                with self.assertRaises(AnalyticsDomainError):
                    recompute_operational_kpis(
                        conn,
                        target_scope_type="RELEASE",
                        target_scope_ref="999999",
                        recompute_mode="FULL_RECOMPUTE",
                    )

                event_types = {str(row["event_type"]) for row in conn.execute("SELECT event_type FROM analytics_operational_kpi_events")}
                self.assertIn("OPERATIONAL_KPI_RECOMPUTE_STARTED", event_types)
                self.assertIn("OPERATIONAL_KPI_RECOMPUTE_COMPLETED", event_types)
                self.assertIn("OPERATIONAL_KPI_SNAPSHOT_CREATED", event_types)
                self.assertIn("OPERATIONAL_KPI_SNAPSHOT_SUPERSEDED", event_types)
                self.assertIn("OPERATIONAL_KPI_ANOMALY_DETECTED", event_types)
                self.assertIn("OPERATIONAL_KPI_RISK_DETECTED", event_types)
                self.assertIn("OPERATIONAL_KPI_EXPLAINABILITY_PAYLOAD_ATTACHED", event_types)
                self.assertIn("OPERATIONAL_KPI_RECOMPUTE_FAILURE_RECORDED", event_types)
            finally:
                conn.close()

    def test_runtime_logs_emit_required_minimum_fields(self) -> None:
        with temp_env() as (_td, env), self.assertLogs("services.analytics_center.operational_kpi_runtime", level=logging.INFO) as logs:
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_release_job(conn)
                recompute_operational_kpis(
                    conn,
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
            finally:
                conn.close()

        merged = "\n".join(logs.output)
        for field in (
            "target_scope_type=",
            "target_scope_ref=",
            "kpi_family=",
            "kpi_code=",
            "status_class=",
            "snapshot_id=",
            "recompute_mode=",
            "run_state=",
            "anomaly_count=",
            "risk_count=",
        ):
            self.assertIn(field, merged)

    def test_operator_observability_stays_grounded_in_explainability_and_problem_listing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                self._seed_release_job(conn)
                recompute_operational_kpis(
                    conn,
                    target_scope_type="CHANNEL",
                    target_scope_ref="darkwood-reverie",
                    recompute_mode="FULL_RECOMPUTE",
                )
                problems = list_operational_problems(conn, scope_type="CHANNEL")
                self.assertGreaterEqual(len(problems), 1)
                self.assertTrue(all(str(p["status_class"]) in {"ANOMALY", "RISK"} for p in problems))
                self.assertTrue(all(bool(p["explainability_payload_json"]) for p in problems))
            finally:
                conn.close()

    def test_partial_recompute_event_is_recorded(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with mock.patch(
                    "services.analytics_center.operational_kpi_runtime.derive_operational_kpis",
                    return_value=[
                        KpiOutput(
                            scope_type="CHANNEL",
                            scope_ref="darkwood-reverie",
                            kpi_family="PIPELINE_TIMING",
                            kpi_code="pipeline_latency",
                            status_class="NORMAL",
                            value_payload={"v": 1},
                            explainability_payload=None,
                            source_snapshot_refs=["CHANNEL:darkwood-reverie"],
                        ),
                        KpiOutput(
                            scope_type="CHANNEL",
                            scope_ref="darkwood-reverie",
                            kpi_family="QA_STATUS",
                            kpi_code="qa_quality",
                            status_class="ANOMALY",
                            value_payload={"v": 2},
                            explainability_payload=None,
                            source_snapshot_refs=["CHANNEL:darkwood-reverie"],
                        ),
                    ],
                ):
                    recompute_operational_kpis(
                        conn,
                        target_scope_type="CHANNEL",
                        target_scope_ref="darkwood-reverie",
                        recompute_mode="FULL_RECOMPUTE",
                    )

                event_types = {str(row["event_type"]) for row in conn.execute("SELECT event_type FROM analytics_operational_kpi_events")}
                self.assertIn("OPERATIONAL_KPI_RECOMPUTE_PARTIAL_RECORDED", event_types)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
