from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.operator_workspaces import workspace_family_catalog, workspace_summary_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf5S1WorkspaceModel(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_workspace_catalog_and_summary_contract(self) -> None:
        catalog = workspace_family_catalog()
        self.assertIn("CHANNEL_WORKSPACE", catalog["workspace_families"])
        summary = workspace_summary_contract(
            family="JOB_WORKSPACE",
            entity_id="job:42",
            title="Job 42",
            blockers=["missing token"],
            next_actions=["open recovery"],
            related_contexts=[{"kind": "channel", "href": "/ui/workspaces/channel/9"}],
        )
        self.assertTrue(summary["is_task_container"])

    def test_workspace_catalog_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/workspaces/catalog", headers=h).json()
            self.assertIn("workspace_families", payload)


if __name__ == "__main__":
    unittest.main()
