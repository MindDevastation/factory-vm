from __future__ import annotations

import importlib
import sqlite3
import threading
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.planner.materialization_service import PlannerMaterializationError
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerMaterializeApi(unittest.TestCase):
    def _insert_planner_item(self, env: Env, *, publish_at: str = "2026-01-01T00:00:00Z") -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES('darkwood-reverie', 'LONG', 'P title', ?, 'P notes', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (publish_at,),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_materialize_create_then_idempotent_existing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planned_release_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with patch(
                "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
            ):
                first = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)
                second = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(first.json()["result"], "CREATED_NEW")
            self.assertEqual(second.json()["result"], "RETURNED_EXISTING")
            self.assertEqual(first.json()["release"]["id"], second.json()["release"]["id"])
            self.assertIn("materialization_state_summary", first.json())
            self.assertIn("binding_diagnostics", first.json())

            conn = dbm.connect(env)
            try:
                jobs_count = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(jobs_count, 0)
            finally:
                conn.close()

    def test_not_ready_and_blocked(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planned_release_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with patch(
                "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                return_value={"aggregate_status": "NOT_READY"},
            ):
                not_ready = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)
            self.assertEqual(not_ready.status_code, 409)
            self.assertEqual(not_ready.json()["error"]["code"], "PRM_NOT_READY")

            with patch(
                "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                return_value={"aggregate_status": "BLOCKED"},
            ):
                blocked = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)
            self.assertEqual(blocked.status_code, 409)
            self.assertEqual(blocked.json()["error"]["code"], "PRM_BLOCKED")

    def test_inconsistent_binding_failure_and_addons(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planned_release_id = self._insert_planner_item(env)

            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'tmp', '', '[]', NULL, NULL, 'tmp-inconsistent-integration', 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, planned_release_id))
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("DELETE FROM releases WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with patch(
                "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
            ):
                resp = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)
            self.assertEqual(resp.status_code, 409)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "PRM_BINDING_INCONSISTENT")
            self.assertIsNotNone(body["materialization_state_summary"])
            self.assertIsNotNone(body["binding_diagnostics"])

    def test_concurrent_double_call_one_create_one_existing(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planned_release_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            results: list[dict[str, int | str]] = []
            lock = threading.Lock()

            def _call_once() -> None:
                client = TestClient(mod.app)
                with patch(
                    "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                    return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
                ):
                    resp = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)
                with lock:
                    results.append({"status": resp.status_code, "result": resp.json().get("result")})

            t1 = threading.Thread(target=_call_once)
            t2 = threading.Thread(target=_call_once)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            self.assertEqual(len(results), 2)
            self.assertEqual(sorted(item["status"] for item in results), [200, 200])
            self.assertEqual(sorted(item["result"] for item in results), ["CREATED_NEW", "RETURNED_EXISTING"])

            conn = dbm.connect(env)
            try:
                release_count = int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"])
                jobs_count = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(release_count, 1)
                self.assertEqual(jobs_count, 0)
            finally:
                conn.close()

    def test_unresolved_concurrency_conflict_returns_explainable_failure(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planned_release_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with patch(
                "services.planner.materialization_service.PlannedReleaseReadinessService.evaluate",
                return_value={"aggregate_status": "READY_FOR_MATERIALIZATION"},
            ), patch(
                "services.planner.materialization_service.set_materialized_release_id",
                side_effect=sqlite3.IntegrityError("simulated race"),
            ), patch(
                "services.planner.materialization_service.PlannerMaterializationService._recover_after_concurrency_conflict",
                side_effect=PlannerMaterializationError(
                    code="PRM_CONCURRENCY_CONFLICT",
                    message="Concurrency conflict could not be resolved.",
                    planned_release_id=planned_release_id,
                    materialization_state_summary={"planned_release_id": planned_release_id},
                    binding_diagnostics={"planned_release_id": planned_release_id},
                ),
            ):
                resp = client.post(f"/v1/planner/planned-releases/{planned_release_id}/materialize", headers=auth)

            self.assertEqual(resp.status_code, 409)
            body = resp.json()
            self.assertEqual(body["result"], "FAILED")
            self.assertEqual(body["error"]["code"], "PRM_CONCURRENCY_CONFLICT")
            self.assertEqual(body["materialization_state_summary"]["planned_release_id"], planned_release_id)
            self.assertEqual(body["binding_diagnostics"]["planned_release_id"], planned_release_id)
