from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.operator_workspaces import entity_drilldown_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf5S2EntityDrilldown(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_entity_drilldown_contract_shape(self) -> None:
        payload = entity_drilldown_contract(
            entry_context="problem_page",
            scope="job:7",
            related_context_links=[{"kind": "release", "href": "/ui/workspaces/release/3"}],
            return_path="/ui/problems/readiness",
            open_full_context_path="/ui/workspaces/job/7",
        )
        self.assertTrue(payload["preserves_parent_identity"])

    def test_entity_drilldown_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/workspaces/job/7/drilldown?entry=problem_page", headers=h).json()
            self.assertEqual(payload["entry_context"], "problem_page")


if __name__ == "__main__":
    unittest.main()
