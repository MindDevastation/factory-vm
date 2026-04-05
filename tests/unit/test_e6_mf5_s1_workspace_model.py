from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.operator_workspaces import build_workspace_summary, workspace_family_catalog
from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestE6Mf5S1WorkspaceModel(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_workspace_catalog_and_summary_contract(self) -> None:
        catalog = workspace_family_catalog()
        self.assertIn("CHANNEL_WORKSPACE", catalog["workspace_families"])
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = importlib.import_module("services.common.db").connect(env)
            try:
                summary = build_workspace_summary(conn=conn, family="channel", entity_id="1")
            finally:
                conn.close()
        self.assertTrue(summary["is_task_container"])
        self.assertEqual(summary["workspace_family"], "CHANNEL_WORKSPACE")

    def test_workspace_catalog_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/workspaces/catalog", headers=h).json()
            self.assertIn("workspace_families", payload)
            job_summary = client.get(f"/v1/workspaces/job/{job_id}", headers=h).json()
            self.assertEqual(job_summary["workspace_family"], "JOB_WORKSPACE")


if __name__ == "__main__":
    unittest.main()
