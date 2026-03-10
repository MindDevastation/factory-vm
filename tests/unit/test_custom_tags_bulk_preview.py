from __future__ import annotations

import unittest

from services.common import db as dbm
from services.custom_tags import catalog_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsBulkPreview(unittest.TestCase):
    def test_preview_deduplicates_identical_payload_items(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                result = catalog_service.preview_bulk_custom_tags(
                    conn,
                    [
                        {"category": "VISUAL", "slug": "solar", "name": "Solar", "description": " Optional ", "is_active": True},
                        {"category": "visual", "slug": "solar", "name": "Solar", "description": " Optional ", "is_active": True},
                    ],
                )
            finally:
                conn.close()

        self.assertTrue(result["can_confirm"])
        self.assertEqual(result["summary"]["total"], 2)
        self.assertEqual(result["summary"]["valid"], 1)
        self.assertEqual(result["summary"]["errors"], 0)
        self.assertEqual(result["summary"]["duplicates_in_payload"], 1)
        self.assertEqual(result["summary"]["upserts_against_db"], 0)
        self.assertEqual(result["items"][0]["action"], "insert")
        self.assertEqual(result["items"][1]["action"], "deduplicated")

    def test_preview_rejects_conflicting_duplicate_key(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                result = catalog_service.preview_bulk_custom_tags(
                    conn,
                    [
                        {"category": "VISUAL", "slug": "solar", "name": "Solar", "description": None, "is_active": True},
                        {"category": "VISUAL", "slug": "solar", "name": "Solar v2", "description": None, "is_active": True},
                    ],
                )
            finally:
                conn.close()

        self.assertFalse(result["can_confirm"])
        self.assertEqual(result["summary"]["valid"], 1)
        self.assertEqual(result["summary"]["errors"], 1)
        self.assertEqual(result["items"][1]["action"], "error")
        self.assertEqual(result["items"][1]["errors"][0]["code"], "CONFLICTING_DUPLICATE_KEY")


if __name__ == "__main__":
    unittest.main()
