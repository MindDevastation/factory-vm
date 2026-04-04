from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf3S1ControlCenterContract(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_contract_skeleton_shape(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/control-center/contract-skeleton", headers=h).json()
            self.assertEqual(payload["surface"], "CONTROL_CENTER_OVERVIEW")
            self.assertEqual(payload["contract_version"], "MF3_S1_BASELINE")
            self.assertIn("factory_summary", payload)
            self.assertIn("attention_summary", payload)
            self.assertIn("channel_summary", payload)
            self.assertIn("batch_month_summary", payload)
            self.assertGreaterEqual(len(payload["task_routing"]), 1)


if __name__ == "__main__":
    unittest.main()
