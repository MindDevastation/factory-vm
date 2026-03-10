from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.common import db as dbm
from services.common.env import Env
from services.custom_tags import catalog_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsSeedIo(unittest.TestCase):
    def test_import_invalid_json_raises_typed_error(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            seed_dir = Path(td)
            base = {
                "schema_version": "custom_tags_seed/1",
                "exported_at": "2026-01-01T00:00:00+00:00",
                "tags": [],
            }
            (seed_dir / "visual_tags.json").write_text(json.dumps({**base, "category": "VISUAL"}), encoding="utf-8")
            (seed_dir / "mood_tags.json").write_text(json.dumps({**base, "category": "MOOD"}), encoding="utf-8")
            (seed_dir / "theme_tags.json").write_text('{"tags": [', encoding="utf-8")
            with temp_env() as (_, _env0):
                env = Env.load()
                seed_minimal_db(env)
                conn = dbm.connect(env)
                try:
                    with self.assertRaises(catalog_service.SeedInvalidJsonError) as ctx:
                        catalog_service.import_catalog(conn, str(seed_dir))
                finally:
                    conn.close()
            self.assertEqual(ctx.exception.code, "CTA_SEED_INVALID_JSON")

    def test_export_writes_full_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            seed_dir = Path(td) / "nested" / "custom"
            with temp_env() as (_, _env0):
                env = Env.load()
                seed_minimal_db(env)
                conn = dbm.connect(env)
                try:
                    catalog_service.create_tag(
                        conn,
                        {
                            "code": "solar",
                            "label": "Solar",
                            "category": "VISUAL",
                            "description": "x",
                            "is_active": True,
                        },
                    )
                    catalog_service.export_catalog(conn, str(seed_dir))
                finally:
                    conn.close()

            visual = json.loads((seed_dir / "visual_tags.json").read_text(encoding="utf-8"))
            mood = json.loads((seed_dir / "mood_tags.json").read_text(encoding="utf-8"))
            theme = json.loads((seed_dir / "theme_tags.json").read_text(encoding="utf-8"))
            self.assertEqual(len(visual["tags"]), 1)
            self.assertEqual(visual["tags"][0]["slug"], "solar")
            self.assertEqual(visual["schema_version"], "custom_tags_seed/1")
            self.assertEqual(mood["tags"], [])
            self.assertEqual(theme["tags"], [])


if __name__ == "__main__":
    unittest.main()
