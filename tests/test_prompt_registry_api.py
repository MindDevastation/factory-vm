from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPromptRegistryApi(unittest.TestCase):
    def _client(self, env: Env) -> TestClient:
        with patch("services.common.env.Env.load", return_value=env):
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
        return TestClient(mod.app)

    def test_contracts_records_versions_endpoints(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            contracts = client.get("/v1/prompt-registry/contracts", headers=headers)
            self.assertEqual(contracts.status_code, 200)
            self.assertIn("record_type", contracts.json())
            self.assertIn("safety_class", contracts.json())

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-template",
                    "code": "PR-API-1",
                    "title": "API Template",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])

            listed = client.get("/v1/prompt-registry/records", headers=headers)
            self.assertEqual(listed.status_code, 200)
            self.assertGreaterEqual(len(listed.json()["items"]), 1)

            fetched = client.get(f"/v1/prompt-registry/records/{prompt_id}", headers=headers)
            self.assertEqual(fetched.status_code, 200)

            patched = client.patch(f"/v1/prompt-registry/records/{prompt_id}", headers=headers, json={"status": "active"})
            self.assertEqual(patched.status_code, 422)

            version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "body {{x}}",
                    "variables": [{"name": "x", "safety_class": "standard", "required": True}],
                },
            )
            self.assertEqual(version.status_code, 200)
            version_id = int(version.json()["id"])

            versions = client.get(f"/v1/prompt-registry/records/{prompt_id}/versions", headers=headers)
            self.assertEqual(versions.status_code, 200)
            self.assertEqual(len(versions.json()["items"]), 1)

            get_version = client.get(f"/v1/prompt-registry/versions/{version_id}", headers=headers)
            self.assertEqual(get_version.status_code, 200)
            self.assertEqual(get_version.json()["variables"][0]["safety_class"], "standard")

            activated = client.post(f"/v1/prompt-registry/versions/{version_id}/activate", headers=headers)
            self.assertEqual(activated.status_code, 200)
            self.assertEqual(int(activated.json()["is_active"]), 1)

            conn = dbm.connect(env)
            try:
                audit_rows = conn.execute(
                    "SELECT event_type,actor FROM prompt_audit_events WHERE prompt_id = ? ORDER BY id ASC",
                    (prompt_id,),
                ).fetchall()
                self.assertTrue(all(row["actor"] == "admin" for row in audit_rows))
            finally:
                conn.close()

    def test_validation_duplicate_and_lifecycle_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            bad = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={"slug": "", "code": "", "title": "", "record_type": "bad", "status": "bad"},
            )
            self.assertEqual(bad.status_code, 422)

            first = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "dupe-api",
                    "code": "PR-API-DUPE",
                    "title": "Dup",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(first.status_code, 200)
            prompt_id = int(first.json()["id"])

            dupe = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "dupe-api",
                    "code": "PR-API-DUPE",
                    "title": "Dup",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(dupe.status_code, 409)

            arch = client.patch(f"/v1/prompt-registry/records/{prompt_id}", headers=headers, json={"status": "archived"})
            self.assertEqual(arch.status_code, 200)
            invalid_transition = client.patch(
                f"/v1/prompt-registry/records/{prompt_id}", headers=headers, json={"status": "active"}
            )
            self.assertEqual(invalid_transition.status_code, 422)

            invalid_version = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={"body_text": "x", "variables": [{"name": "secret", "safety_class": "not_allowed"}]},
            )
            self.assertEqual(invalid_version.status_code, 422)

    def test_version_create_atomicity_and_duplicate_names_api(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client = self._client(env)
            headers = basic_auth_header("admin", "testpass")

            created = client.post(
                "/v1/prompt-registry/records",
                headers=headers,
                json={
                    "slug": "api-atomic",
                    "code": "PR-API-ATOMIC",
                    "title": "API Atomic",
                    "record_type": "prompt_template",
                    "status": "draft",
                },
            )
            self.assertEqual(created.status_code, 200)
            prompt_id = int(created.json()["id"])

            invalid_payload = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={"body_text": "x", "variables": ["not-an-object"]},
            )
            self.assertEqual(invalid_payload.status_code, 422)

            duplicate_names = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={
                    "body_text": "x {{a}} {{a}}",
                    "variables": [
                        {"name": "a", "safety_class": "standard"},
                        {"name": "a", "safety_class": "standard"},
                    ],
                },
            )
            self.assertEqual(duplicate_names.status_code, 422)

            contradictory_active = client.post(
                f"/v1/prompt-registry/records/{prompt_id}/versions",
                headers=headers,
                json={"body_text": "x", "status": "active"},
            )
            self.assertEqual(contradictory_active.status_code, 422)

            conn = dbm.connect(env)
            try:
                version_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()["c"]
                )
                self.assertEqual(version_count, 0)
            finally:
                conn.close()
