from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.custom_tags import catalog_service
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsBulkRulesApi(unittest.TestCase):
    def test_preview_and_confirm_atomic_create_only(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                catalog_service.create_tag(
                    conn,
                    {"code": "cyber_arena", "label": "Cyber Arena", "category": "VISUAL", "description": None, "is_active": True},
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            payload = {
                "items": [
                    {
                        "tag_code": "cyber_arena",
                        "source_path": "track_features.payload_json.voice_flag",
                        "operator": "equals",
                        "value_json": "false",
                        "priority": 100,
                        "weight": 1.0,
                        "required": True,
                        "stop_after_match": False,
                        "is_active": True,
                        "match_mode": "ALL",
                    },
                    {
                        "tag_code": "cyber_arena",
                        "source_path": "track_features.payload_json.energy",
                        "operator": "gte",
                        "value_json": "0.6",
                        "priority": 80,
                        "weight": 0.5,
                        "required": False,
                        "stop_after_match": False,
                        "is_active": True,
                        "match_mode": "ANY",
                    },
                ]
            }

            preview = client.post("/v1/track-catalog/custom-tags/bulk-rules/preview", headers=h, json=payload)
            self.assertEqual(preview.status_code, 200)
            self.assertTrue(preview.json()["can_confirm"])
            self.assertEqual(preview.json()["summary"], {"total": 2, "create": 2, "invalid": 0})

            confirm = client.post("/v1/track-catalog/custom-tags/bulk-rules/confirm", headers=h, json=payload)
            self.assertEqual(confirm.status_code, 200)
            self.assertTrue(confirm.json()["ok"])
            self.assertEqual(confirm.json()["summary"], {"total": 2, "created": 2, "invalid": 0})

            conn = dbm.connect(env)
            try:
                rows = conn.execute("SELECT id FROM custom_tag_rules ORDER BY id ASC").fetchall()
            finally:
                conn.close()
            self.assertEqual(len(rows), 2)

    def test_confirm_fails_all_when_preview_invalid(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                catalog_service.create_tag(
                    conn,
                    {"code": "cyber_arena", "label": "Cyber Arena", "category": "VISUAL", "description": None, "is_active": True},
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            invalid_payload = {
                "items": [
                    {
                        "tag_code": "cyber_arena",
                        "source_path": "track_features.payload_json.voice_flag",
                        "operator": "equals",
                        "value_json": "false",
                        "priority": 100,
                        "weight": 1.0,
                        "required": True,
                        "stop_after_match": False,
                        "is_active": True,
                        "match_mode": "ALL",
                    },
                    {
                        "tag_code": "cyber_arena",
                        "source_path": "track_features.payload_json.voice_flag",
                        "operator": "equals",
                        "value_json": "false",
                        "priority": 101,
                        "weight": 1.0,
                        "required": True,
                        "stop_after_match": False,
                        "is_active": True,
                        "match_mode": "ALL",
                    },
                ]
            }

            confirm = client.post("/v1/track-catalog/custom-tags/bulk-rules/confirm", headers=h, json=invalid_payload)
            self.assertEqual(confirm.status_code, 200)
            body = confirm.json()
            self.assertFalse(body["ok"])
            self.assertEqual(body["summary"], {"total": 2, "create": 1, "invalid": 1})

            conn = dbm.connect(env)
            try:
                rows = conn.execute("SELECT id FROM custom_tag_rules ORDER BY id ASC").fetchall()
            finally:
                conn.close()
            self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
