from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from unittest import mock

from services.common import db as dbm
from services.planner import mass_actions_execute_service as svc
from services.planner import mass_actions_preview_service as preview_svc
from services.planner.materialization_service import MaterializationResult
from services.planner.release_job_creation_service import ReleaseJobCreateOrSelectResult
from tests._helpers import seed_minimal_db, temp_env


class TestPlannerMassActionsExecuteService(unittest.TestCase):
    def _insert_planned_release(self, conn, *, publish_at: str, title: str = "P", status: str = "PLANNED") -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES('darkwood-reverie', 'LONG', ?, ?, 'n', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (title, publish_at, status),
        )
        return int(cur.lastrowid)

    def _insert_release(self, conn, *, title: str, meta_id: str) -> int:
        channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
        cur = conn.execute(
            """
            INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
            VALUES(?, ?, 'd', '[]', '2026-01-01T00:00:00Z', NULL, ?, 0)
            """,
            (channel_id, title, meta_id),
        )
        return int(cur.lastrowid)

    def _create_preview_session(self, conn, *, action_type: str, selected_ids: list[int], ttl_seconds: int = 1800) -> str:
        out = preview_svc.create_mass_action_preview_session(
            conn,
            action_type=action_type,
            selected_item_ids=selected_ids,
            created_by="tester",
            ttl_seconds=ttl_seconds,
        )
        return str(out["session_id"])

    def test_execute_rejects_superset_subset_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z")
                session_id = self._create_preview_session(conn, action_type=preview_svc.ACTION_MATERIALIZE, selected_ids=[p1])

                with self.assertRaises(svc.PlannerMassActionExecuteError) as ctx:
                    svc.execute_mass_action_session(
                        conn,
                        session_id=session_id,
                        selected_item_ids=[p1, p1 + 100],
                        executed_by="u",
                    )
                self.assertEqual(ctx.exception.code, "PMA_EXECUTE_SUBSET_INVALID")
            finally:
                conn.close()

    def test_execute_marks_expired_session_and_returns_context(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z")
                session_id = self._create_preview_session(conn, action_type=preview_svc.ACTION_MATERIALIZE, selected_ids=[p1])
                expired_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
                conn.execute("UPDATE planner_mass_action_sessions SET expires_at = ? WHERE id = ?", (expired_at, session_id))
                conn.commit()

                with self.assertRaises(svc.PlannerMassActionExecuteError) as ctx:
                    svc.execute_mass_action_session(conn, session_id=session_id, selected_item_ids=None, executed_by="u")
                self.assertEqual(ctx.exception.code, "PMA_SESSION_EXPIRED")
                self.assertEqual(ctx.exception.details["session_id"], session_id)
                row = conn.execute("SELECT preview_status FROM planner_mass_action_sessions WHERE id = ?", (session_id,)).fetchone()
                self.assertEqual(str(row["preview_status"]), "EXPIRED")
            finally:
                conn.close()

    def test_execute_invalidated_session_error(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z")
                session_id = self._create_preview_session(conn, action_type=preview_svc.ACTION_MATERIALIZE, selected_ids=[p1])
                conn.execute("UPDATE planner_mass_action_sessions SET preview_status = 'INVALIDATED' WHERE id = ?", (session_id,))
                conn.commit()

                with self.assertRaises(svc.PlannerMassActionExecuteError) as ctx:
                    svc.execute_mass_action_session(conn, session_id=session_id, selected_item_ids=None, executed_by="u")
                self.assertEqual(ctx.exception.code, "PMA_SESSION_INVALIDATED")
            finally:
                conn.close()

    def test_execute_materialize_summary_and_executed_transition(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z", title="create")
                p2 = self._insert_planned_release(conn, publish_at="2026-01-02T00:00:00Z", title="existing")
                p3 = self._insert_planned_release(conn, publish_at="2026-01-03T00:00:00Z", title="skipped")
                p4 = self._insert_planned_release(conn, publish_at="2026-01-04T00:00:00Z", title="failed")
                existing_release_id = self._insert_release(conn, title="existing", meta_id="meta-existing")

                session_id = self._create_preview_session(
                    conn,
                    action_type=preview_svc.ACTION_MATERIALIZE,
                    selected_ids=[p1, p2, p3, p4],
                )

                created_result = MaterializationResult(
                    planned_release_id=p1,
                    result="CREATED_NEW",
                    release_id=501,
                    release_channel_slug="darkwood-reverie",
                    materialized_binding={"planned_release_id": p1, "release_id": 501},
                    materialization_state_summary={},
                    binding_diagnostics={},
                )
                existing_result = MaterializationResult(
                    planned_release_id=p2,
                    result="RETURNED_EXISTING",
                    release_id=existing_release_id,
                    release_channel_slug="darkwood-reverie",
                    materialized_binding={"planned_release_id": p2, "release_id": existing_release_id},
                    materialization_state_summary={},
                    binding_diagnostics={},
                )

                class _FakeErr(Exception):
                    def __init__(self, code: str, message: str):
                        self.code = code
                        self.message = message

                def _fake_materialize(*, planned_release_id: int, created_by: str):
                    del created_by
                    if planned_release_id == p1:
                        return created_result
                    if planned_release_id == p2:
                        return existing_result
                    if planned_release_id == p3:
                        raise _FakeErr("PRM_NOT_READY", "not ready")
                    raise _FakeErr("PRM_BINDING_INCONSISTENT", "inconsistent")

                with mock.patch(
                    "services.planner.mass_actions_execute_service.PlannerMaterializationService.materialize_planned_release",
                    side_effect=_fake_materialize,
                ), mock.patch(
                    "services.planner.mass_actions_execute_service.PlannerMaterializationError",
                    _FakeErr,
                ):
                    out = svc.execute_mass_action_session(conn, session_id=session_id, selected_item_ids=None, executed_by="u")

                self.assertEqual(out["summary"]["total_selected"], 4)
                self.assertEqual(out["summary"]["succeeded"], 2)
                self.assertEqual(out["summary"]["skipped"], 1)
                self.assertEqual(out["summary"]["failed"], 1)
                self.assertEqual(out["summary"]["created_new_entities"], 1)
                self.assertEqual(out["summary"]["returned_existing_entities"], 1)
                self.assertEqual(out["preview_status"], "EXECUTED")

                row = conn.execute(
                    "SELECT preview_status, executed_at FROM planner_mass_action_sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
                self.assertEqual(str(row["preview_status"]), "EXECUTED")
                self.assertTrue(str(row["executed_at"]))

                with self.assertRaises(svc.PlannerMassActionExecuteError) as ctx2:
                    svc.execute_mass_action_session(conn, session_id=session_id, selected_item_ids=None, executed_by="u")
                self.assertEqual(ctx2.exception.code, "PMA_SELECTION_SCOPE_MISMATCH")
            finally:
                conn.close()

    def test_execute_job_creation_payload_shape_with_subset(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-05T00:00:00Z", title="job-create")
                p2 = self._insert_planned_release(conn, publish_at="2026-01-06T00:00:00Z", title="job-existing")
                p3 = self._insert_planned_release(conn, publish_at="2026-01-07T00:00:00Z", title="job-skip")

                r1 = self._insert_release(conn, title="r1", meta_id="m1")
                r2 = self._insert_release(conn, title="r2", meta_id="m2")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (r1, p1))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (r2, p2))
                conn.commit()

                session_id = self._create_preview_session(
                    conn,
                    action_type=preview_svc.ACTION_CREATE_JOBS,
                    selected_ids=[p1, p2, p3],
                )

                out_create = ReleaseJobCreateOrSelectResult(
                    release_id=r1,
                    result="CREATED_NEW_JOB",
                    job={"id": 901, "release_id": r1, "channel_slug": "darkwood-reverie", "status": "DRAFT"},
                    current_open_relation={"release_id": r1, "job_id": 901},
                    job_creation_state_summary={},
                    open_job_diagnostics={},
                )
                out_existing = ReleaseJobCreateOrSelectResult(
                    release_id=r2,
                    result="RETURNED_EXISTING_OPEN_JOB",
                    job={"id": 902, "release_id": r2, "channel_slug": "darkwood-reverie", "status": "DRAFT"},
                    current_open_relation={"release_id": r2, "job_id": 902},
                    job_creation_state_summary={},
                    open_job_diagnostics={},
                )

                def _fake_create_or_select(*, release_id: int):
                    if release_id == r1:
                        return out_create
                    return out_existing

                with mock.patch(
                    "services.planner.mass_actions_execute_service.ReleaseJobCreationService.create_or_select",
                    side_effect=_fake_create_or_select,
                ):
                    out = svc.execute_mass_action_session(
                        conn,
                        session_id=session_id,
                        selected_item_ids=[p1, p2],
                        executed_by="u",
                    )

                self.assertEqual(out["summary"]["total_selected"], 2)
                self.assertEqual(out["summary"]["succeeded"], 2)
                self.assertEqual(out["summary"]["failed"], 0)
                self.assertEqual(out["summary"]["skipped"], 0)
                self.assertEqual(out["summary"]["created_new_entities"], 1)
                self.assertEqual(out["summary"]["returned_existing_entities"], 1)
                self.assertEqual(len(out["items"]), 2)
                self.assertIn("executed_at", out)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
