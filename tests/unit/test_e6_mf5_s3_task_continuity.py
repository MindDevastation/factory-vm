from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.operator_workspaces import task_continuity_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf5S3TaskContinuity(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_task_continuity_contract(self) -> None:
        payload = task_continuity_contract(parent_context_ref="problem:blocked", filters={"status": "blocked"}, scope="job:42", result_return_path="/ui/problems/readiness")
        self.assertTrue(payload["restorable"])

    def test_task_continuity_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/workspaces/task-continuity?parent=problem:blocked&status=blocked&scope=job:42", headers=h).json()
            self.assertEqual(payload["current_scope"], "job:42")


if __name__ == "__main__":
    unittest.main()
