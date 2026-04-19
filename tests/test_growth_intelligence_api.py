from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestGrowthIntelligenceApi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_contracts_endpoint_shape(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            resp = client.get("/v1/growth-intelligence/contracts", headers=basic_auth_header("admin", "testpass"))
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIn("source_class", body)
            self.assertIn("feature_flags", body)
            self.assertIn("typed_linked_actions", body)

    def test_knowledge_playbook_and_flags_crud(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            create_item = client.post(
                "/v1/growth-intelligence/knowledge-items",
                headers=headers,
                json={
                    "code": "GI-API-1",
                    "title": "Item",
                    "description": "Description",
                    "source_type": "doc",
                    "source_name": "seed",
                    "source_trust": "B",
                    "impact_confidence": "Medium",
                    "source_class": "INTERNAL",
                    "action_template": "open",
                    "explanation_template": "why",
                    "status": "ACTIVE",
                },
            )
            self.assertEqual(create_item.status_code, 200)
            item_id = int(create_item.json()["id"])

            listed = client.get("/v1/growth-intelligence/knowledge-items?source_class=INTERNAL&q=item", headers=headers)
            self.assertEqual(listed.status_code, 200)
            self.assertGreaterEqual(len(listed.json()["items"]), 1)

            patched = client.patch(
                f"/v1/growth-intelligence/knowledge-items/{item_id}",
                headers=headers,
                json={"impact_confidence": "High", "description": "Updated"},
            )
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(patched.json()["impact_confidence"], "High")

            playbook = client.post(
                "/v1/growth-intelligence/playbooks",
                headers=headers,
                json={
                    "code": "PB-1",
                    "goal_type": "RETENTION",
                    "channel_types_json": ["LONG"],
                    "release_types_json": ["VIDEO"],
                    "activation_rules_json": {"min_items": 1},
                    "output_shape_json": {"kind": "plan"},
                    "trust_policy_json": {"min_trust": "B"},
                },
            )
            self.assertEqual(playbook.status_code, 200)
            playbook_id = int(playbook.json()["id"])

            listed_playbooks = client.get("/v1/growth-intelligence/playbooks", headers=headers)
            self.assertEqual(listed_playbooks.status_code, 200)
            self.assertGreaterEqual(len(listed_playbooks.json()["items"]), 1)

            patched_playbook = client.patch(
                f"/v1/growth-intelligence/playbooks/{playbook_id}", headers=headers, json={"goal_type": "AWARENESS"}
            )
            self.assertEqual(patched_playbook.status_code, 200)
            self.assertEqual(patched_playbook.json()["goal_type"], "AWARENESS")

            get_flags = client.get("/v1/growth-intelligence/channels/darkwood-reverie/feature-flags", headers=headers)
            self.assertEqual(get_flags.status_code, 200)
            self.assertFalse(bool(get_flags.json()["growth_intelligence_enabled"]))

            set_flags = client.put(
                "/v1/growth-intelligence/channels/darkwood-reverie/feature-flags",
                headers=headers,
                json={
                    "growth_intelligence_enabled": True,
                    "planning_digest_enabled": False,
                    "planner_handoff_enabled": False,
                    "export_enabled": False,
                    "assisted_planning_enabled": True,
                },
            )
            self.assertEqual(set_flags.status_code, 200)
            self.assertEqual(int(set_flags.json()["growth_intelligence_enabled"]), 1)

    def test_bootstrap_import_and_negative_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            bad_class = client.post(
                "/v1/growth-intelligence/knowledge-items",
                headers=headers,
                json={
                    "code": "GI-BAD-1",
                    "title": "Bad",
                    "description": "d",
                    "source_type": "doc",
                    "source_name": "x",
                    "source_trust": "A",
                    "impact_confidence": "Medium",
                    "source_class": "UNKNOWN",
                    "action_template": "a",
                    "explanation_template": "e",
                    "status": "ACTIVE",
                },
            )
            self.assertEqual(bad_class.status_code, 422)

            bad_trust = client.post(
                "/v1/growth-intelligence/knowledge-items",
                headers=headers,
                json={
                    "code": "GI-BAD-2",
                    "title": "Bad",
                    "description": "d",
                    "source_type": "doc",
                    "source_name": "x",
                    "source_trust": "Z",
                    "impact_confidence": "Medium",
                    "source_class": "INTERNAL",
                    "action_template": "a",
                    "explanation_template": "e",
                    "status": "ACTIVE",
                },
            )
            self.assertEqual(bad_trust.status_code, 422)

            bad_conf = client.post(
                "/v1/growth-intelligence/knowledge-items",
                headers=headers,
                json={
                    "code": "GI-BAD-3",
                    "title": "Bad",
                    "description": "d",
                    "source_type": "doc",
                    "source_name": "x",
                    "source_trust": "A",
                    "impact_confidence": "Unknown",
                    "source_class": "INTERNAL",
                    "action_template": "a",
                    "explanation_template": "e",
                    "status": "ACTIVE",
                },
            )
            self.assertEqual(bad_conf.status_code, 422)

            bad_flags = client.put(
                "/v1/growth-intelligence/channels/darkwood-reverie/feature-flags",
                headers=headers,
                json={"growth_intelligence_enabled": "yes"},
            )
            self.assertEqual(bad_flags.status_code, 422)

            malformed_bootstrap = client.post(
                "/v1/growth-intelligence/bootstrap/import",
                headers=headers,
                json={"import_source": "curated", "items": []},
            )
            self.assertEqual(malformed_bootstrap.status_code, 422)

            bootstrap = client.post(
                "/v1/growth-intelligence/bootstrap/import",
                headers=headers,
                json={
                    "import_source": "curated",
                    "import_mode": "upsert",
                    "actor": "tester",
                    "items": [
                        {
                            "code": "GI-B-1",
                            "title": "Boot",
                            "description": "d",
                            "source_type": "doc",
                            "source_name": "x",
                            "source_trust": "B",
                            "impact_confidence": "Medium",
                            "source_class": "INTERNAL",
                            "action_template": "a",
                            "explanation_template": "e",
                            "status": "ACTIVE",
                        }
                    ],
                },
            )
            self.assertEqual(bootstrap.status_code, 200)
            self.assertEqual(bootstrap.json()["created"], 1)

            bootstrap_repeat = client.post(
                "/v1/growth-intelligence/bootstrap/import",
                headers=headers,
                json={
                    "import_source": "curated",
                    "import_mode": "upsert",
                    "actor": "tester",
                    "items": [
                        {
                            "code": "GI-B-1",
                            "title": "Boot",
                            "description": "d",
                            "source_type": "doc",
                            "source_name": "x",
                            "source_trust": "B",
                            "impact_confidence": "Medium",
                            "source_class": "INTERNAL",
                            "action_template": "a",
                            "explanation_template": "e",
                            "status": "ACTIVE",
                        }
                    ],
                },
            )
            self.assertEqual(bootstrap_repeat.status_code, 200)
            self.assertEqual(bootstrap_repeat.json()["skipped"], 1)

            conn = dbm.connect(env)
            try:
                runs = conn.execute("SELECT COUNT(*) AS c FROM growth_bootstrap_import_runs").fetchone()["c"]
            finally:
                conn.close()
            self.assertGreaterEqual(int(runs), 2)


if __name__ == "__main__":
    unittest.main()
