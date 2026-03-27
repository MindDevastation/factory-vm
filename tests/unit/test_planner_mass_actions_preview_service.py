from __future__ import annotations

import unittest
from unittest import mock

from services.common import db as dbm
from services.planner import mass_actions_preview_service as svc
from tests._helpers import seed_minimal_db, temp_env


class TestPlannerMassActionsPreviewService(unittest.TestCase):
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

    def test_validation_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(svc.PlannerMassActionPreviewError) as ctx1:
                    svc.create_mass_action_preview_session(
                        conn,
                        action_type="NOPE",
                        selected_item_ids=[1],
                        created_by="u",
                        ttl_seconds=1800,
                    )
                self.assertEqual(ctx1.exception.code, "PMA_INVALID_ACTION_TYPE")

                with self.assertRaises(svc.PlannerMassActionPreviewError) as ctx2:
                    svc.create_mass_action_preview_session(
                        conn,
                        action_type=svc.ACTION_MATERIALIZE,
                        selected_item_ids=[],
                        created_by="u",
                        ttl_seconds=1800,
                    )
                self.assertEqual(ctx2.exception.code, "PMA_SELECTION_EMPTY")

                with self.assertRaises(svc.PlannerMassActionPreviewError) as ctx_bool:
                    svc.create_mass_action_preview_session(
                        conn,
                        action_type=svc.ACTION_MATERIALIZE,
                        selected_item_ids=[True],
                        created_by="u",
                        ttl_seconds=1800,
                    )
                self.assertEqual(ctx_bool.exception.code, "PMA_SELECTION_EMPTY")

                with self.assertRaises(svc.PlannerMassActionPreviewError) as ctx3:
                    svc.create_mass_action_preview_session(
                        conn,
                        action_type=svc.ACTION_MATERIALIZE,
                        selected_item_ids=list(range(1, 202)),
                        created_by="u",
                        ttl_seconds=1800,
                    )
                self.assertEqual(ctx3.exception.code, "PMA_SELECTION_TOO_LARGE")
            finally:
                conn.close()

    def test_validation_rejects_duplicate_selected_item_ids(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_id = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z")
                with self.assertRaises(svc.PlannerMassActionPreviewError) as ctx:
                    svc.create_mass_action_preview_session(
                        conn,
                        action_type=svc.ACTION_MATERIALIZE,
                        selected_item_ids=[planned_id, planned_id],
                        created_by="u",
                        ttl_seconds=1800,
                    )
                self.assertEqual(ctx.exception.code, "PMA_SELECTION_SCOPE_MISMATCH")
            finally:
                conn.close()

    def test_materialization_preview_mixed_and_persistence(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                create_new_id = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z", title="new")
                existing_id = self._insert_planned_release(conn, publish_at="2026-01-01T01:00:00Z", title="existing")
                skipped_id = self._insert_planned_release(conn, publish_at="", title="skipped")
                failed_id = self._insert_planned_release(conn, publish_at="2026-01-01T03:00:00Z", title="failed")

                rel_existing = self._insert_release(conn, title="existing release", meta_id="meta-existing")
                rel_failed = self._insert_release(conn, title="failed release", meta_id="meta-failed")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_existing, existing_id))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_failed, failed_id))
                conn.execute(
                    "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')",
                    (failed_id, rel_existing),
                )
                conn.commit()

                def _fake_evaluate(*, planned_release_id: int):
                    status_by_id = {
                        create_new_id: "READY_FOR_MATERIALIZATION",
                        existing_id: "READY_FOR_MATERIALIZATION",
                        skipped_id: "NOT_READY",
                        failed_id: "READY_FOR_MATERIALIZATION",
                    }
                    return {"aggregate_status": status_by_id[planned_release_id]}

                with mock.patch(
                    "services.planner.mass_actions_preview_service.PlannedReleaseReadinessService.evaluate",
                    side_effect=_fake_evaluate,
                ):
                    out = svc.create_mass_action_preview_session(
                        conn,
                        action_type=svc.ACTION_MATERIALIZE,
                        selected_item_ids=[create_new_id, existing_id, skipped_id, failed_id],
                        created_by="tester",
                        ttl_seconds=120,
                    )
                self.assertEqual(out["aggregate"]["total_selected"], 4)
                self.assertEqual(out["aggregate"]["would_create_new"], 1)
                self.assertEqual(out["aggregate"]["would_return_existing"], 1)
                self.assertEqual(out["aggregate"]["would_skip"], 1)
                self.assertEqual(out["aggregate"]["would_fail"], 1)

                by_id = {int(item["planned_release_id"]): item for item in out["items"]}
                self.assertEqual(by_id[create_new_id]["result_kind"], "SUCCESS_CREATED_NEW")
                self.assertEqual(by_id[existing_id]["result_kind"], "SUCCESS_RETURNED_EXISTING")
                self.assertEqual(by_id[skipped_id]["result_kind"], "SKIPPED_NON_EXECUTABLE")
                self.assertEqual(by_id[failed_id]["result_kind"], "FAILED_INVALID_OR_INCONSISTENT")

                row = conn.execute(
                    "SELECT action_type, preview_status, selected_item_ids_json, expires_at FROM planner_mass_action_sessions WHERE id = ?",
                    (out["session_id"],),
                ).fetchone()
                self.assertEqual(str(row["action_type"]), svc.ACTION_MATERIALIZE)
                self.assertEqual(str(row["preview_status"]), "OPEN")
                self.assertIn(str(create_new_id), str(row["selected_item_ids_json"]))
                self.assertTrue(str(row["expires_at"]))
            finally:
                conn.close()

    def test_job_creation_preview_mixed_outcomes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                create_new_id = self._insert_planned_release(conn, publish_at="2026-01-02T00:00:00Z", title="job-new")
                existing_id = self._insert_planned_release(conn, publish_at="2026-01-02T01:00:00Z", title="job-existing")
                skipped_id = self._insert_planned_release(conn, publish_at="2026-01-02T02:00:00Z", title="job-skipped")
                failed_id = self._insert_planned_release(conn, publish_at="2026-01-02T03:00:00Z", title="job-failed")

                rel_create = self._insert_release(conn, title="rel-create", meta_id="meta-job-create")
                rel_existing = self._insert_release(conn, title="rel-existing", meta_id="meta-job-existing")
                rel_failed = self._insert_release(conn, title="rel-failed", meta_id="meta-job-failed")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_create, create_new_id))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_existing, existing_id))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_failed, failed_id))
                conn.execute("UPDATE releases SET current_open_job_id = NULL WHERE id = ?", (rel_create,))
                job_existing = dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=rel_existing,
                    job_type="RELEASE",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=0,
                    updated_at=0,
                )
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_existing, rel_existing))
                dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=rel_failed,
                    job_type="RELEASE",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=0,
                    updated_at=0,
                )
                dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=rel_failed,
                    job_type="RELEASE",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=0,
                    updated_at=0,
                )
                conn.commit()

                out = svc.create_mass_action_preview_session(
                    conn,
                    action_type=svc.ACTION_CREATE_JOBS,
                    selected_item_ids=[create_new_id, existing_id, skipped_id, failed_id],
                    created_by="tester",
                    ttl_seconds=1800,
                )
                by_id = {int(item["planned_release_id"]): item for item in out["items"]}
                self.assertEqual(by_id[create_new_id]["result_kind"], "SUCCESS_CREATED_NEW")
                self.assertEqual(by_id[existing_id]["result_kind"], "SUCCESS_RETURNED_EXISTING")
                self.assertEqual(by_id[skipped_id]["result_kind"], "SKIPPED_NON_EXECUTABLE")
                self.assertEqual(by_id[failed_id]["result_kind"], "FAILED_INVALID_OR_INCONSISTENT")
                self.assertEqual(by_id[skipped_id]["reason"]["code"], "PMA_RELEASE_NOT_MATERIALIZED")
            finally:
                conn.close()

    def test_get_session_not_found(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                with self.assertRaises(svc.PlannerMassActionPreviewError) as ctx:
                    svc.get_mass_action_preview_session(conn, session_id="missing")
                self.assertEqual(ctx.exception.code, "PMA_SESSION_NOT_FOUND")
            finally:
                conn.close()
