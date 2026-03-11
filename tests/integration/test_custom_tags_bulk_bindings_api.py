from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.custom_tags import catalog_service
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsBulkBindingsApi(unittest.TestCase):
    def test_preview_confirm_atomic_and_idempotent(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                visual = catalog_service.create_tag(
                    conn,
                    {"code": "cyber_arena", "label": "Cyber Arena", "category": "VISUAL", "description": None, "is_active": True},
                )
                conn.execute(
                    "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                    (int(visual["id"]), "darkwood-reverie", dbm.now_ts()),
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
                    {"tag_code": "cyber_arena", "channel_slug": "titanwave-sonic", "is_active": True},
                    {"tag_code": "missing", "channel_slug": "titanwave-sonic", "is_active": True},
                ]
            }
            invalid_confirm = client.post("/v1/track-catalog/custom-tags/bulk-bindings/confirm", headers=h, json=invalid_payload)
            self.assertEqual(invalid_confirm.status_code, 200)
            self.assertFalse(invalid_confirm.json()["ok"])

            conn = dbm.connect(env)
            try:
                rows = conn.execute("SELECT id FROM custom_tag_channel_bindings").fetchall()
            finally:
                conn.close()
            self.assertEqual(len(rows), 1)

            payload = {
                "items": [
                    {"tag_code": "cyber_arena", "channel_slug": "titanwave-sonic", "is_active": True},
                    {"tag_code": "cyber_arena", "channel_slug": "darkwood-reverie", "is_active": False},
                ]
            }
            preview = client.post("/v1/track-catalog/custom-tags/bulk-bindings/preview", headers=h, json=payload)
            self.assertEqual(preview.status_code, 200)
            self.assertTrue(preview.json()["can_confirm"])

            confirm = client.post("/v1/track-catalog/custom-tags/bulk-bindings/confirm", headers=h, json=payload)
            self.assertEqual(confirm.status_code, 200)
            body = confirm.json()
            self.assertTrue(body["ok"])
            self.assertEqual(body["summary"], {"total": 2, "created": 1, "updated": 1, "noop": 0, "invalid": 0})

            repeat = client.post("/v1/track-catalog/custom-tags/bulk-bindings/confirm", headers=h, json=payload)
            self.assertEqual(repeat.status_code, 200)
            repeat_body = repeat.json()
            self.assertTrue(repeat_body["ok"])
            self.assertEqual(repeat_body["summary"], {"total": 2, "created": 0, "updated": 0, "noop": 2, "invalid": 0})


if __name__ == "__main__":
    unittest.main()
