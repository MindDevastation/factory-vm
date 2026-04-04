from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.shared_action_flows import batch_preview_execute_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf6S4BatchContracts(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_batch_preview_contract(self) -> None:
        payload = batch_preview_execute_contract(targets=["job:1", "job:2"], action="batch_execute")
        self.assertEqual(payload["action_class"], "BATCH_MUTATE")

    def test_batch_preview_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/actions/contracts/batch-preview-execute?action=batch_execute&targets=job:1,job:2", headers=h).json()
            self.assertEqual(payload["target_count"], 2)


if __name__ == "__main__":
    unittest.main()
