from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.shared_action_flows import cross_domain_consistency_contract, result_continuation_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf6S5ResultContinuity(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_result_continuation_contracts(self) -> None:
        payload = result_continuation_contract(result_class="PARTIAL", what_changed=["job:1"], what_failed=["job:2"], unresolved=[], next_step="review failures", return_path="/ui/workspaces/job/1")
        self.assertTrue(payload["continuation_supported"])
        self.assertGreaterEqual(len(cross_domain_consistency_contract()["surfaces"]), 5)

    def test_result_continuation_endpoints(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            self.assertEqual(client.get("/v1/actions/contracts/result-continuation?result=PARTIAL&changed=job:1&failed=job:2", headers=h).json()["result_class"], "PARTIAL")
            self.assertIn("surfaces", client.get("/v1/actions/contracts/cross-domain-consistency", headers=h).json())


if __name__ == "__main__":
    unittest.main()
