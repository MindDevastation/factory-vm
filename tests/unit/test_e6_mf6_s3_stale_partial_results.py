from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.shared_action_flows import partial_result_summary_contract, stale_refusal_or_refresh_contract
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf6S3StalePartialResults(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_stale_and_partial_contracts(self) -> None:
        self.assertEqual(stale_refusal_or_refresh_contract(expected_version="v1", actual_version="v2")["status"], "STALE")
        self.assertEqual(partial_result_summary_contract(succeeded=["a"], failed=["b"], unresolved=[])["result_class"], "PARTIAL")

    def test_stale_and_partial_endpoints(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            self.assertEqual(client.get("/v1/actions/contracts/stale-refresh?expected=v1&actual=v2", headers=h).json()["next_action"], "refresh")
            self.assertEqual(client.get("/v1/actions/contracts/partial-result?succeeded=a&failed=b", headers=h).json()["result_class"], "PARTIAL")


if __name__ == "__main__":
    unittest.main()
