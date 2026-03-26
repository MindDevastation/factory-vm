from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from services.common import db as dbm
from services.planner.materialization_service import PlannerMaterializationError, PlannerMaterializationService
from tests._helpers import seed_minimal_db, temp_env


class TestPlannerMaterializationService(unittest.TestCase):
    def _insert_planner_item(self, conn: sqlite3.Connection, *, channel_slug: str = "darkwood-reverie") -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES(?, 'LONG', 'Planned title', '2026-01-01T00:00:00Z', 'seed notes', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (channel_slug,),
        )
        return int(cur.lastrowid)

    def test_create_then_return_existing_and_summary_diagnostics(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_release_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)
                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
                ):
                    first = svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")
                    second = svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")

                self.assertEqual(first.result, "CREATED_NEW")
                self.assertEqual(second.result, "RETURNED_EXISTING")
                self.assertEqual(second.release_id, first.release_id)

                self.assertEqual(first.materialization_state_summary["materialization_state"], "ALREADY_MATERIALIZED")
                self.assertEqual(first.materialization_state_summary["invariant_status"], "OK")
                self.assertEqual(first.binding_diagnostics["invariant_status"], "OK")
                self.assertTrue(first.binding_diagnostics["linked_release_exists"])

                release_count = conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"]
                self.assertEqual(int(release_count), 1)
            finally:
                conn.close()

    def test_readiness_recheck_not_ready_and_blocked(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_release_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)

                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "NOT_READY"},
                ):
                    with self.assertRaises(PlannerMaterializationError) as not_ready_ctx:
                        svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")
                self.assertEqual(not_ready_ctx.exception.code, "PRM_NOT_READY")

                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "BLOCKED"},
                ):
                    with self.assertRaises(PlannerMaterializationError) as blocked_ctx:
                        svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")
                self.assertEqual(blocked_ctx.exception.code, "PRM_BLOCKED")
            finally:
                conn.close()

    def test_inconsistent_binding_path(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_release_id = self._insert_planner_item(conn)
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'tmp', '', '[]', NULL, NULL, 'tmp-inconsistent-unit', 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, planned_release_id))
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("DELETE FROM releases WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")
                svc = PlannerMaterializationService(conn)
                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
                ):
                    with self.assertRaises(PlannerMaterializationError) as ctx:
                        svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")

                self.assertEqual(ctx.exception.code, "PRM_BINDING_INCONSISTENT")
                self.assertIsNotNone(ctx.exception.materialization_state_summary)
                self.assertEqual(ctx.exception.binding_diagnostics["invariant_status"], "INCONSISTENT")
            finally:
                conn.close()

    def test_concurrency_recovery_returns_existing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_release_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)
                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
                ):
                    created = svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")
                out = svc._recover_after_concurrency_conflict(planned_release_id=planned_release_id)
                self.assertEqual(out.release_id, created.release_id)
                self.assertEqual(out.result, "RETURNED_EXISTING")
            finally:
                conn.close()

    def test_concurrency_recovery_unresolved_returns_conflict(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_release_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)

                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
                ), patch(
                    "services.planner.materialization_service.set_materialized_release_id",
                    side_effect=sqlite3.IntegrityError("simulated race"),
                ):
                    with self.assertRaises(PlannerMaterializationError) as ctx:
                        svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")

                self.assertEqual(ctx.exception.code, "PRM_CONCURRENCY_CONFLICT")
            finally:
                conn.close()

    def test_no_heuristic_release_lookup_queries_are_used(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            traced_sql: list[str] = []
            conn.set_trace_callback(lambda sql: traced_sql.append(str(sql)))
            try:
                planned_release_id = self._insert_planner_item(conn)
                svc = PlannerMaterializationService(conn)
                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
                ):
                    svc.materialize_planned_release(planned_release_id=planned_release_id, created_by="tester")

                normalized = " ".join(" ".join(traced_sql).split()).upper()
                self.assertNotIn("SELECT ID FROM RELEASES WHERE CHANNEL_ID =", normalized)
                self.assertNotIn("SELECT ID FROM RELEASES WHERE TITLE =", normalized)
                self.assertNotIn("SELECT ID FROM RELEASES WHERE PLANNED_AT =", normalized)
            finally:
                conn.set_trace_callback(None)
                conn.close()
