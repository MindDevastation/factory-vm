from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.planner.materialization_service import PlannerMaterializationError
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerMaterializeApi(unittest.TestCase):
    def _insert_planner_item(self, env: Env, *, status: str = "PLANNED", publish_at: str = "2026-01-01T00:00:00Z") -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES('darkwood-reverie', 'LONG', 'P title', ?, 'P notes', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (publish_at, status),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_materialize_success_then_idempotent_and_patch_locked(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planner_item_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            first = client.post(f"/v1/planner/items/{planner_item_id}/materialize", headers=auth)
            self.assertEqual(first.status_code, 200)
            first_body = first.json()
            self.assertEqual(first_body["materialization_status"], "CREATED")
            self.assertEqual(first_body["planner_status"], "LOCKED")

            second = client.post(f"/v1/planner/items/{planner_item_id}/materialize", headers=auth)
            self.assertEqual(second.status_code, 200)
            second_body = second.json()
            self.assertEqual(second_body["materialization_status"], "EXISTING_BINDING")
            self.assertEqual(second_body["release_id"], first_body["release_id"])

            patch = client.patch(f"/v1/planner/releases/{planner_item_id}", headers=auth, json={"title": "new title"})
            self.assertEqual(patch.status_code, 409)
            self.assertEqual(patch.json()["error"]["code"], "PLR_RELEASE_LOCKED")

    def test_error_code_status_mapping(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            failed_item = self._insert_planner_item(env, status="FAILED", publish_at="2026-01-01T00:00:00Z")
            locked_item = self._insert_planner_item(env, status="LOCKED", publish_at="2026-01-01T01:00:00Z")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            not_found = client.post("/v1/planner/items/999999/materialize", headers=auth)
            self.assertEqual(not_found.status_code, 404)
            self.assertEqual(not_found.json()["error"]["code"], "PLM_NOT_FOUND")

            invalid = client.post(f"/v1/planner/items/{failed_item}/materialize", headers=auth)
            self.assertEqual(invalid.status_code, 409)
            self.assertEqual(invalid.json()["error"]["code"], "PLM_INVALID_STATUS")

            inconsistent = client.post(f"/v1/planner/items/{locked_item}/materialize", headers=auth)
            self.assertEqual(inconsistent.status_code, 409)
            self.assertEqual(inconsistent.json()["error"]["code"], "PLM_INCONSISTENT_STATE")

    def test_binding_conflict_maps_to_409(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planner_item_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with patch(
                "services.factory_api.planner.PlannerMaterializationService.materialize_or_get",
                side_effect=PlannerMaterializationError(code="PLM_BINDING_CONFLICT", message="binding conflict"),
            ):
                resp = client.post(f"/v1/planner/items/{planner_item_id}/materialize", headers=auth)
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLM_BINDING_CONFLICT")

    def test_internal_error_maps_to_500(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            planner_item_id = self._insert_planner_item(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with patch(
                "services.factory_api.planner.PlannerMaterializationService.materialize_or_get",
                side_effect=PlannerMaterializationError(code="PLM_INTERNAL", message="materialization failed"),
            ):
                resp = client.post(f"/v1/planner/items/{planner_item_id}/materialize", headers=auth)

            self.assertEqual(resp.status_code, 500)
            self.assertEqual(resp.json()["error"]["code"], "PLM_INTERNAL")
