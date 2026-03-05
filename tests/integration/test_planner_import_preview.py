from __future__ import annotations

import importlib
import json
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerImportPreview(unittest.TestCase):
    def _insert_release(
        self,
        env: Env,
        *,
        channel_slug: str,
        content_type: str,
        title: str,
        publish_at: str | None,
        notes: str = "seed",
    ) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'PLANNED', ?, ?)
                """,
                (channel_slug, content_type, title, publish_at, notes, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_import_preview_csv_success_with_conflict_duplicate_and_flags(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            existing_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Existing",
                publish_at="2025-02-01T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            csv_text = (
                "channel_slug,content_type,title,publish_at,notes\n"
                "darkwood-reverie,LONG,Title A,2025-02-01T10:00:00,Note A\n"
                "darkwood-reverie,LONG,Title B,2025-02-01T10:00:00,Note B\n"
                "missing-channel,LONG,Title C,2025-02-01T11:00:00,Note C\n"
            )

            resp = client.post(
                "/v1/planner/import/preview",
                headers=auth,
                files={"file": ("preview.csv", csv_text.encode("utf-8"), "text/csv")},
            )

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertTrue(body["preview_id"])
            self.assertEqual(body["summary"]["total_rows"], 3)
            self.assertEqual(body["summary"]["error_rows"], 2)
            self.assertEqual(body["summary"]["conflict_rows"], 2)
            self.assertFalse(body["can_confirm_strict"])
            self.assertFalse(body["can_confirm_replace"])

            row1, row2, row3 = body["rows"]
            self.assertEqual(row1["existing_release_id"], existing_id)
            self.assertTrue(row1["conflict"])
            self.assertEqual(row1["errors"], [])
            self.assertEqual(row1["normalized"]["publish_at"], "2025-02-01T10:00:00+02:00")

            self.assertTrue(row2["conflict"])
            self.assertIn("DUPLICATE_IN_FILE", row2["errors"])
            self.assertEqual(row2["existing_release_id"], existing_id)

            self.assertIn("CHANNEL_NOT_FOUND", row3["errors"])

    def test_import_preview_json_success_can_confirm(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            payload = [
                {
                    "channel_slug": "darkwood-reverie",
                    "content_type": "LONG",
                    "title": "Json A",
                    "publish_at": "2025-03-01T15:30:00",
                    "notes": "n",
                },
                {
                    "channel_slug": "darkwood-reverie",
                    "content_type": "SHORT",
                    "title": "Json B",
                    "publish_at": None,
                    "notes": "",
                },
            ]

            resp = client.post(
                "/v1/planner/import/preview",
                headers=auth,
                files={"file": ("preview.json", json.dumps(payload).encode("utf-8"), "application/json")},
            )

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["summary"], {"total_rows": 2, "error_rows": 0, "conflict_rows": 0})
            self.assertTrue(body["can_confirm_strict"])
            self.assertTrue(body["can_confirm_replace"])
            self.assertEqual(body["rows"][0]["normalized"]["publish_at"], "2025-03-01T15:30:00+02:00")
            self.assertIsNone(body["rows"][1]["normalized"]["publish_at"])

    def test_import_preview_parse_error(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/import/preview",
                headers=auth,
                files={"file": ("broken.json", b"{not json", "application/json")},
            )

            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "PLR_PARSE_ERROR")

    def test_import_preview_too_many_rows(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            header = "channel_slug,content_type,title,publish_at,notes\n"
            rows = ["darkwood-reverie,LONG,t,,n\n" for _ in range(5001)]
            csv_text = header + "".join(rows)

            resp = client.post(
                "/v1/planner/import/preview",
                headers=auth,
                files={"file": ("many.csv", csv_text.encode("utf-8"), "text/csv")},
            )

            self.assertEqual(resp.status_code, 413)
            self.assertEqual(resp.json()["error"]["code"], "PLR_TOO_MANY_ROWS")


if __name__ == "__main__":
    unittest.main()
