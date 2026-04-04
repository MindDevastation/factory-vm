from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.problem_readiness_contracts import (
    attention_class_for_severity,
    problem_family_for_state,
    problem_readiness_item_contract,
)
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf4S1ProblemReadinessContract(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_problem_family_and_attention_mapping(self) -> None:
        self.assertEqual(problem_family_for_state(state="FAILED"), "EXECUTION_FAILURE")
        self.assertEqual(attention_class_for_severity(severity="HIGH"), "PRIORITY")

    def test_problem_item_contract_shape(self) -> None:
        item = problem_readiness_item_contract(
            state="FAILED",
            severity="HIGH",
            primary_reason="x",
            supporting_signals=["a", "b"],
            next_direction="open workspace",
        )
        self.assertEqual(item["problem_family"], "EXECUTION_FAILURE")
        self.assertIn("explanation", item)

    def test_problem_readiness_contract_endpoint(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/problems/readiness/contract", headers=h).json()
            self.assertIn("catalog", payload)
            self.assertIn("sample", payload)
            self.assertIn("problem_family", payload["sample"])


if __name__ == "__main__":
    unittest.main()
