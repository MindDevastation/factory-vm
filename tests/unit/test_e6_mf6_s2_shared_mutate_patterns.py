from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.factory_api.shared_action_flows import (
    canonical_action_class_for_action,
    direct_mutate_with_confirmation_contract,
    preview_confirm_execute_contract,
    preview_to_apply_contract,
)
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf6S2SharedMutatePatterns(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_pattern_contracts(self) -> None:
        self.assertEqual(canonical_action_class_for_action(action="refresh"), "READ_ONLY")
        self.assertEqual(preview_to_apply_contract(action="retry", preview_scope="job:7")["pattern"], "PREVIEW_TO_APPLY")
        self.assertEqual(preview_confirm_execute_contract(action="approve", preview_scope="job:7")["pattern"], "PREVIEW_TO_CONFIRM_TO_EXECUTE")
        self.assertEqual(direct_mutate_with_confirmation_contract(action="cancel", target_scope="job:7")["pattern"], "DIRECT_MUTATE_WITH_CONFIRMATION")


    def test_taxonomy_consistency_between_modules(self) -> None:
        self.assertEqual(canonical_action_class_for_action(action="approve"), "GUARDED_MUTATE")
        self.assertEqual(canonical_action_class_for_action(action="batch_execute"), "BATCH_MUTATE")

    def test_pattern_endpoints(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            self.assertEqual(client.get("/v1/actions/contracts/preview-apply?action=retry&scope=job:7", headers=h).json()["action_class"], "LOW_RISK_MUTATE")
            self.assertEqual(client.get("/v1/actions/contracts/preview-confirm-execute?action=approve&scope=job:7", headers=h).json()["action_class"], "GUARDED_MUTATE")
            self.assertEqual(client.get("/v1/actions/contracts/direct-confirm?action=cancel&scope=job:7", headers=h).json()["action_class"], "HIGH_RISK_MUTATE")


if __name__ == "__main__":
    unittest.main()
