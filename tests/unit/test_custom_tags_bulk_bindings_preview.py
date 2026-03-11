from __future__ import annotations

import unittest

from services.common import db as dbm
from services.custom_tags import bulk_bindings_service, catalog_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsBulkBindingsPreview(unittest.TestCase):
    def test_preview_classifies_create_update_noop_invalid(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                visual = catalog_service.create_tag(
                    conn,
                    {"code": "cyber_arena", "label": "Cyber Arena", "category": "VISUAL", "description": None, "is_active": True},
                )
                catalog_service.create_tag(
                    conn,
                    {"code": "calm", "label": "Calm", "category": "MOOD", "description": None, "is_active": True},
                )
                conn.execute(
                    "INSERT INTO custom_tag_channel_bindings(tag_id, channel_slug, created_at) VALUES(?,?,?)",
                    (int(visual["id"]), "darkwood-reverie", dbm.now_ts()),
                )
                conn.commit()

                out = bulk_bindings_service.preview_bulk_bindings(
                    conn,
                    [
                        {"tag_code": "cyber_arena", "channel_slug": "titanwave-sonic", "is_active": True},
                        {"tag_code": "cyber_arena", "channel_slug": "darkwood-reverie", "is_active": False},
                        {"tag_code": "cyber_arena", "channel_slug": "darkwood-reverie", "is_active": True},
                        {"tag_code": "missing", "channel_slug": "missing-channel", "is_active": True},
                        {"tag_code": "calm", "channel_slug": "darkwood-reverie", "is_active": True},
                    ],
                )
            finally:
                conn.close()

        self.assertFalse(out["can_confirm"])
        self.assertEqual(out["summary"], {"total": 5, "create": 1, "update": 1, "noop": 1, "invalid": 2})
        self.assertEqual([item["action"] for item in out["items"]], ["CREATE", "UPDATE", "NOOP", "INVALID", "INVALID"])


if __name__ == "__main__":
    unittest.main()
