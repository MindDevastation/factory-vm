from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.common import db as dbm
from services.common.env import Env
from services.custom_tags import catalog_service
from tests._helpers import seed_minimal_db, temp_env


class TestCustomTagsSeedValidation(unittest.TestCase):
    def _seed_file(self, root: Path, name: str, payload: dict) -> None:
        (root / name).write_text(json.dumps(payload), encoding="utf-8")

    def _baseline(self, category: str) -> dict:
        return {
            "schema_version": "custom_tags_seed/1",
            "category": category,
            "exported_at": "2026-01-01T00:00:00+00:00",
            "tags": [{"slug": "solar", "name": "Solar", "description": "Optional", "is_active": True}],
        }

    def test_missing_required_file_returns_cts_seed_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._seed_file(root, "visual_tags.json", self._baseline("VISUAL"))
            self._seed_file(root, "mood_tags.json", self._baseline("MOOD"))
            with temp_env() as (_td, _env0):
                env = Env.load()
                seed_minimal_db(env)
                conn = dbm.connect(env)
                try:
                    with self.assertRaises(catalog_service.SeedNotFoundError) as ctx:
                        catalog_service.import_catalog(conn, str(root))
                finally:
                    conn.close()
        self.assertEqual(ctx.exception.code, "CTS_SEED_NOT_FOUND")

    def test_schema_version_and_category_must_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bad = self._baseline("VISUAL")
            bad["schema_version"] = "wrong"
            self._seed_file(root, "visual_tags.json", bad)
            self._seed_file(root, "mood_tags.json", self._baseline("MOOD"))
            self._seed_file(root, "theme_tags.json", self._baseline("THEME"))
            with temp_env() as (_td, _env0):
                env = Env.load()
                seed_minimal_db(env)
                conn = dbm.connect(env)
                try:
                    with self.assertRaises(catalog_service.SeedValidationError):
                        catalog_service.import_catalog(conn, str(root))
                finally:
                    conn.close()

    def test_slug_validation_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            visual = self._baseline("VISUAL")
            visual["tags"][0]["slug"] = "Solar!"
            self._seed_file(root, "visual_tags.json", visual)
            self._seed_file(root, "mood_tags.json", self._baseline("MOOD"))
            self._seed_file(root, "theme_tags.json", self._baseline("THEME"))
            with temp_env() as (_td, _env0):
                env = Env.load()
                seed_minimal_db(env)
                conn = dbm.connect(env)
                try:
                    with self.assertRaises(catalog_service.SeedValidationError):
                        catalog_service.import_catalog(conn, str(root))
                finally:
                    conn.close()


if __name__ == "__main__":
    unittest.main()
