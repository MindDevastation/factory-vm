from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsBulkJsonApi(unittest.TestCase):
    def test_preview_and_confirm_with_upsert_counts(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                catalog_service_payload = {
                    "code": "solar",
                    "label": "Solar",
                    "category": "VISUAL",
                    "description": "old",
                    "is_active": True,
                }
                from services.custom_tags import catalog_service

                catalog_service.create_tag(conn, catalog_service_payload)
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            payload = {
                "items": [
                    {"category": "VISUAL", "slug": "solar", "name": "Solar", "description": "new", "is_active": True},
                    {"category": "MOOD", "slug": "calm", "name": "Calm", "description": None, "is_active": False},
                    {"category": "MOOD", "slug": "calm", "name": "Calm", "description": None, "is_active": False},
                ]
            }
            preview = client.post("/v1/track-catalog/custom-tags/bulk/preview", headers=h, json=payload)
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertTrue(body["can_confirm"])
            self.assertEqual(body["summary"]["total"], 3)
            self.assertEqual(body["summary"]["valid"], 2)
            self.assertEqual(body["summary"]["duplicates_in_payload"], 1)
            self.assertEqual(body["summary"]["upserts_against_db"], 1)

            confirm = client.post("/v1/track-catalog/custom-tags/bulk/confirm", headers=h, json=payload)
            self.assertEqual(confirm.status_code, 200)
            confirm_body = confirm.json()
            self.assertTrue(confirm_body["can_confirm"])
            self.assertEqual(confirm_body["inserted"], 1)
            self.assertEqual(confirm_body["updated"], 1)
            self.assertEqual(confirm_body["unchanged"], 0)

    def test_confirm_is_atomic_when_conflict_present(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            payload = {
                "items": [
                    {"category": "VISUAL", "slug": "solar", "name": "Solar", "description": None, "is_active": True},
                    {"category": "VISUAL", "slug": "solar", "name": "Solar 2", "description": None, "is_active": True},
                    {"category": "MOOD", "slug": "calm", "name": "Calm", "description": None, "is_active": True},
                ]
            }
            confirm = client.post("/v1/track-catalog/custom-tags/bulk/confirm", headers=h, json=payload)
            self.assertEqual(confirm.status_code, 200)
            body = confirm.json()
            self.assertFalse(body["can_confirm"])
            self.assertEqual(body["inserted"], 0)
            self.assertEqual(body["updated"], 0)
            self.assertEqual(body["unchanged"], 0)

            listed = client.get("/v1/track-catalog/custom-tags/catalog", headers=h)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(listed.json()["tags"], [])


if __name__ == "__main__":
    unittest.main()
