from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.operator_workspaces import result_return_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf5S4ResultReturn(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_result_return_contract(self) -> None:
        payload = result_return_contract(from_action="publish_retry", return_path="/ui/workspaces/job/7", open_full_context_path="/ui/workspaces/job/7?full=1")
        self.assertTrue(payload["continuation_supported"])

    def test_result_return_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/workspaces/result-return?action=publish_retry&return_path=/ui/workspaces/job/7", headers=h).json()
            self.assertEqual(payload["return_path"], "/ui/workspaces/job/7")


if __name__ == "__main__":
    unittest.main()
