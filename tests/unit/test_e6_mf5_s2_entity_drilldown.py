from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.operator_workspaces import entity_drilldown_contract
from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestE6Mf5S2EntityDrilldown(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_entity_drilldown_contract_shape(self) -> None:
        payload = entity_drilldown_contract(
            entry_context="problem_page",
            scope="JOB_WORKSPACE:7",
            related_context_links=[{"kind": "release_workspace", "href": "/v1/workspaces/release/3"}],
            return_path="/ui/problems/readiness",
            open_full_context_path="/v1/workspaces/job/7",
        )
        self.assertTrue(payload["preserves_parent_identity"])

    def test_entity_drilldown_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get(f"/v1/workspaces/job/{job_id}/drilldown?entry=problem_page", headers=h).json()
            self.assertEqual(payload["entry_context"], "problem_page")
            self.assertIn("JOB_WORKSPACE", payload["current_entity_scope"])



    def test_entity_drilldown_endpoint_not_found(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/workspaces/job/999999/drilldown", headers=h)
            self.assertEqual(resp.status_code, 404)

if __name__ == "__main__":
    unittest.main()
