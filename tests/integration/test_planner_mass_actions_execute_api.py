from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.planner import mass_actions_preview_service as preview_svc
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerMassActionsExecuteApi(unittest.TestCase):
    def _insert_planned_release(self, conn, *, publish_at: str, title: str) -> int:
        return int(
            conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES('darkwood-reverie', 'LONG', ?, ?, 'n', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (title, publish_at),
            ).lastrowid
        )

    def _insert_release(self, conn, *, title: str, meta_id: str) -> int:
        channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
        return int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, ?, 'd', '[]', '2026-01-01T00:00:00Z', NULL, ?, 0)
                """,
                (channel_id, title, meta_id),
            ).lastrowid
        )

    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_execute_materialize_mixed_and_repeated_execute_blocked(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p_create = self._insert_planned_release(conn, publish_at="2026-01-01T00:00:00Z", title="create")
                p_existing = self._insert_planned_release(conn, publish_at="2026-01-02T00:00:00Z", title="existing")
                p_skip = self._insert_planned_release(conn, publish_at="2026-01-03T00:00:00Z", title="skip")
                p_fail = self._insert_planned_release(conn, publish_at="2026-01-04T00:00:00Z", title="fail")

                rel_existing = self._insert_release(conn, title="rel-existing", meta_id="meta-existing")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_existing, p_existing))
                conn.commit()

                session = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_MATERIALIZE_SELECTED",
                    selected_item_ids=[p_create, p_existing, p_skip, p_fail],
                    created_by="u",
                    ttl_seconds=1800,
                )
                session_id = str(session["session_id"])
            finally:
                conn.close()

            client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            def _fake_materialize(*, planned_release_id: int, created_by: str):
                del created_by
                if planned_release_id == p_create:
                    return type("R", (), {"result": "CREATED_NEW", "release_id": 501})()
                if planned_release_id == p_existing:
                    return type("R", (), {"result": "RETURNED_EXISTING", "release_id": rel_existing})()
                if planned_release_id == p_skip:
                    raise type("E", (Exception,), {"code": "PRM_NOT_READY", "message": "not ready"})()
                raise type("E", (Exception,), {"code": "PRM_BINDING_INCONSISTENT", "message": "bad"})()

            with mock.patch(
                "services.planner.mass_actions_execute_service.PlannerMaterializationService.materialize_planned_release",
                side_effect=_fake_materialize,
            ), mock.patch(
                "services.planner.mass_actions_execute_service.PlannerMaterializationError",
                Exception,
            ):
                resp = client.post(f"/v1/planner/mass-actions/{session_id}/execute", headers=auth, json={})

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["summary"]["total_selected"], 4)
            self.assertEqual(body["summary"]["succeeded"], 2)
            self.assertEqual(body["summary"]["skipped"], 1)
            self.assertEqual(body["summary"]["failed"], 1)

            second = client.post(f"/v1/planner/mass-actions/{session_id}/execute", headers=auth, json={})
            self.assertEqual(second.status_code, 409)
            self.assertEqual(second.json()["error"]["code"], "PMA_SELECTION_SCOPE_MISMATCH")

    def test_execute_job_creation_subset_and_no_hidden_materialize(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-05T00:00:00Z", title="job-create")
                p2 = self._insert_planned_release(conn, publish_at="2026-01-06T00:00:00Z", title="job-existing")
                p3 = self._insert_planned_release(conn, publish_at="2026-01-07T00:00:00Z", title="job-skip")

                r1 = self._insert_release(conn, title="rel-1", meta_id="meta-1")
                r2 = self._insert_release(conn, title="rel-2", meta_id="meta-2")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (r1, p1))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (r2, p2))
                conn.commit()

                before_release_count = int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"])
                session = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_CREATE_JOBS_FOR_SELECTED",
                    selected_item_ids=[p1, p2, p3],
                    created_by="u",
                    ttl_seconds=1800,
                )
                session_id = str(session["session_id"])
            finally:
                conn.close()

            client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            def _fake_create_or_select(*, release_id: int):
                if release_id == r1:
                    return type("J", (), {"result": "CREATED_NEW_JOB", "job": {"id": 9001}})()
                return type("J", (), {"result": "RETURNED_EXISTING_OPEN_JOB", "job": {"id": 9002}})()

            with mock.patch(
                "services.planner.mass_actions_execute_service.ReleaseJobCreationService.create_or_select",
                side_effect=_fake_create_or_select,
            ):
                resp = client.post(
                    f"/v1/planner/mass-actions/{session_id}/execute",
                    headers=auth,
                    json={"selected_item_ids": [p1, p2]},
                )

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["summary"]["total_selected"], 2)
            self.assertEqual(body["summary"]["created_new_entities"], 1)
            self.assertEqual(body["summary"]["returned_existing_entities"], 1)

            conn = dbm.connect(env)
            try:
                after_release_count = int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"])
                self.assertEqual(before_release_count, after_release_count)
            finally:
                conn.close()

    def test_non_materialized_is_skipped_for_job_creation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p_skip = self._insert_planned_release(conn, publish_at="2026-01-08T00:00:00Z", title="job-skip")
                session = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_CREATE_JOBS_FOR_SELECTED",
                    selected_item_ids=[p_skip],
                    created_by="u",
                    ttl_seconds=1800,
                )
                session_id = str(session["session_id"])
            finally:
                conn.close()

            client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/planner/mass-actions/{session_id}/execute", headers=auth, json={})
            self.assertEqual(resp.status_code, 200)
            item = resp.json()["items"][0]
            self.assertEqual(item["result_kind"], "SKIPPED_NON_EXECUTABLE")
            self.assertEqual(item["reason"]["code"], "PMA_RELEASE_NOT_MATERIALIZED")

    def test_expired_session_and_subset_invalid_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planned_release(conn, publish_at="2026-01-09T00:00:00Z", title="materialize")
                session = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_MATERIALIZE_SELECTED",
                    selected_item_ids=[p1],
                    created_by="u",
                    ttl_seconds=1800,
                )
                session_id = str(session["session_id"])
                conn.execute(
                    "UPDATE planner_mass_action_sessions SET expires_at = ? WHERE id = ?",
                    ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), session_id),
                )
                conn.commit()
            finally:
                conn.close()

            client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            expired = client.post(f"/v1/planner/mass-actions/{session_id}/execute", headers=auth, json={})
            self.assertEqual(expired.status_code, 409)
            expired_body = expired.json()
            self.assertEqual(expired_body["error"]["code"], "PMA_SESSION_EXPIRED")
            self.assertEqual(expired_body["error"]["details"]["session_id"], session_id)
            self.assertIn("expires_at", expired_body["error"]["details"])

            conn = dbm.connect(env)
            try:
                p2 = self._insert_planned_release(conn, publish_at="2026-01-10T00:00:00Z", title="subset")
                session2 = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_MATERIALIZE_SELECTED",
                    selected_item_ids=[p2],
                    created_by="u",
                    ttl_seconds=1800,
                )
                session2_id = str(session2["session_id"])
            finally:
                conn.close()

            subset_invalid = client.post(
                f"/v1/planner/mass-actions/{session2_id}/execute",
                headers=auth,
                json={"selected_item_ids": [p2, 999999]},
            )
            self.assertEqual(subset_invalid.status_code, 422)
            self.assertEqual(subset_invalid.json()["error"]["code"], "PMA_EXECUTE_SUBSET_INVALID")

            subset_duplicate = client.post(
                f"/v1/planner/mass-actions/{session2_id}/execute",
                headers=auth,
                json={"selected_item_ids": [p2, p2]},
            )
            self.assertEqual(subset_duplicate.status_code, 400)
            self.assertEqual(subset_duplicate.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_expired_invalidated_session_returns_error_before_item_processing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_id = self._insert_planned_release(conn, publish_at="2026-01-11T00:00:00Z", title="stale")
                expired = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_MATERIALIZE_SELECTED",
                    selected_item_ids=[planned_id],
                    created_by="u",
                    ttl_seconds=1800,
                )
                expired_id = str(expired["session_id"])
                conn.execute(
                    "UPDATE planner_mass_action_sessions SET expires_at = ? WHERE id = ?",
                    ((datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(), expired_id),
                )

                invalidated = preview_svc.create_mass_action_preview_session(
                    conn,
                    action_type="BATCH_MATERIALIZE_SELECTED",
                    selected_item_ids=[planned_id],
                    created_by="u",
                    ttl_seconds=1800,
                )
                invalidated_id = str(invalidated["session_id"])
                conn.execute(
                    "UPDATE planner_mass_action_sessions SET preview_status = 'INVALIDATED' WHERE id = ?",
                    (invalidated_id,),
                )
                conn.commit()
            finally:
                conn.close()

            client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with mock.patch(
                "services.planner.mass_actions_execute_service._execute_materialize_item",
                side_effect=AssertionError("must not execute items for stale sessions"),
            ):
                expired_resp = client.post(f"/v1/planner/mass-actions/{expired_id}/execute", headers=auth, json={})
                self.assertEqual(expired_resp.status_code, 409)
                self.assertEqual(expired_resp.json()["error"]["code"], "PMA_SESSION_EXPIRED")

                invalid_resp = client.post(f"/v1/planner/mass-actions/{invalidated_id}/execute", headers=auth, json={})
                self.assertEqual(invalid_resp.status_code, 409)
                self.assertEqual(invalid_resp.json()["error"]["code"], "PMA_SESSION_INVALIDATED")


if __name__ == "__main__":
    unittest.main()
