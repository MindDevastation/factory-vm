from __future__ import annotations

import importlib
import json
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCustomTagsCatalogApi(unittest.TestCase):
    def test_catalog_crud_import_export(self) -> None:
        with temp_env() as (td, _env0):
            seed_dir = Path(td.name) / "seeds" / "custom-tags"
            seed_dir.mkdir(parents=True, exist_ok=True)
            import_payloads = {
                "visual_tags.json": {"tags": [{"code": "solar", "label": "Solar", "description": "v", "is_active": True}]},
                "mood_tags.json": {"tags": [{"code": "calm", "label": "Calm", "description": None, "is_active": False}]},
                "theme_tags.json": {"tags": [{"code": "space", "label": "Space", "description": "t", "is_active": True}]},
            }
            for name, payload in import_payloads.items():
                (seed_dir / name).write_text(json.dumps(payload), encoding="utf-8")

            import os

            os.environ["CUSTOM_TAGS_SEED_DIR"] = str(seed_dir)

            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            initial = client.get("/v1/track-catalog/custom-tags/catalog", headers=h)
            self.assertEqual(initial.status_code, 200)
            self.assertEqual(initial.json(), {"tags": []})

            created = client.post(
                "/v1/track-catalog/custom-tags/catalog",
                headers=h,
                json={
                    "code": "mist",
                    "label": "Mist",
                    "category": "VISUAL",
                    "description": "foggy",
                    "is_active": True,
                },
            )
            self.assertEqual(created.status_code, 200)
            tag_id = int(created.json()["tag"]["id"])

            patched = client.patch(
                f"/v1/track-catalog/custom-tags/catalog/{tag_id}",
                headers=h,
                json={"label": "Night Mist", "is_active": False},
            )
            self.assertEqual(patched.status_code, 200)
            self.assertEqual(patched.json()["tag"]["label"], "Night Mist")
            self.assertEqual(patched.json()["tag"]["is_active"], False)

            bad_patch = client.patch(
                f"/v1/track-catalog/custom-tags/catalog/{tag_id}",
                headers=h,
                json={"category": "MOOD"},
            )
            self.assertEqual(bad_patch.status_code, 400)
            self.assertEqual(bad_patch.json()["error"]["code"], "CTA_INVALID_INPUT")

            imported = client.post("/v1/track-catalog/custom-tags/catalog/import", headers=h)
            self.assertEqual(imported.status_code, 200)
            self.assertEqual(imported.json()["imported"], 3)

            listed = client.get("/v1/track-catalog/custom-tags/catalog", headers=h)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(len(listed.json()["tags"]), 4)

            export_dir = Path(td.name) / "out" / "catalog"
            os.environ["CUSTOM_TAGS_SEED_DIR"] = str(export_dir)
            importlib.reload(mod)
            client = TestClient(mod.app)
            exported = client.post("/v1/track-catalog/custom-tags/catalog/export", headers=h)
            self.assertEqual(exported.status_code, 200)
            self.assertTrue((export_dir / "visual_tags.json").is_file())
            visual = json.loads((export_dir / "visual_tags.json").read_text(encoding="utf-8"))
            self.assertEqual(sorted(tag["code"] for tag in visual["tags"]), ["mist", "solar"])

    def test_import_missing_file_returns_typed_error(self) -> None:
        with temp_env() as (td, _env0):
            seed_dir = Path(td.name) / "seeds-missing"
            seed_dir.mkdir(parents=True, exist_ok=True)
            (seed_dir / "visual_tags.json").write_text('{"tags": []}', encoding="utf-8")
            (seed_dir / "mood_tags.json").write_text('{"tags": []}', encoding="utf-8")

            import os

            os.environ["CUSTOM_TAGS_SEED_DIR"] = str(seed_dir)
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post("/v1/track-catalog/custom-tags/catalog/import", headers=h)
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "CTA_SEED_VALIDATION_FAILED")


if __name__ == "__main__":
    unittest.main()
