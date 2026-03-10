from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.common import db as dbm
from services.common.env import Env
from services.custom_tags import catalog_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsSeedRoundtrip(unittest.TestCase):
    def test_export_import_roundtrip_and_creates_missing_seed_dir(self) -> None:
        with temp_env() as (_td, _env0):
            env = Env.load()
            seed_minimal_db(env)
            with tempfile.TemporaryDirectory() as td:
                seed_dir = Path(td) / "data" / "seeds" / "custom_tags"
                conn = dbm.connect(env)
                try:
                    catalog_service.create_tag(
                        conn,
                        {
                            "code": "aurora",
                            "label": "Aurora",
                            "category": "VISUAL",
                            "description": "northern lights",
                            "is_active": True,
                        },
                    )
                    catalog_service.create_tag(
                        conn,
                        {
                            "code": "night_drive",
                            "label": "Night Drive",
                            "category": "MOOD",
                            "description": None,
                            "is_active": False,
                        },
                    )
                    exported = catalog_service.export_catalog(conn, str(seed_dir))
                    conn.execute("DELETE FROM custom_tags")
                    imported = catalog_service.import_catalog(conn, str(seed_dir))
                    rows = catalog_service.list_catalog(conn)
                finally:
                    conn.close()

                self.assertEqual(exported["files"], ["visual_tags.json", "mood_tags.json", "theme_tags.json"])
                self.assertTrue((seed_dir / "visual_tags.json").is_file())
                self.assertTrue((seed_dir / "mood_tags.json").is_file())
                self.assertTrue((seed_dir / "theme_tags.json").is_file())

                visual = json.loads((seed_dir / "visual_tags.json").read_text(encoding="utf-8"))
                self.assertEqual(visual["schema_version"], "custom_tags_seed/1")
                self.assertEqual(visual["category"], "VISUAL")
                self.assertEqual(visual["tags"][0]["slug"], "aurora")
                self.assertEqual(visual["tags"][0]["name"], "Aurora")

                self.assertEqual(imported["imported"], 2)
                self.assertEqual(len(rows), 2)


if __name__ == "__main__":
    unittest.main()
