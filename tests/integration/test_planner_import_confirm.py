from __future__ import annotations

import importlib
import json
import sqlite3
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.planner.import_service import PlannerImportPreviewService
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerImportConfirm(unittest.TestCase):
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

    def _count_releases(self, env: Env) -> int:
        conn = dbm.connect(env)
        try:
            row = conn.execute("SELECT COUNT(*) AS cnt FROM planned_releases").fetchone()
            return int(row["cnt"])
        finally:
            conn.close()

    def _preview(self, client: TestClient, auth: dict[str, str], rows: list[dict[str, object]]) -> str:
        resp = client.post(
            "/v1/planner/import/preview",
            headers=auth,
            files={"file": ("preview.json", json.dumps(rows).encode("utf-8"), "application/json")},
        )
        self.assertEqual(resp.status_code, 200)
        return str(resp.json()["preview_id"])

    def test_confirm_strict_fail_all_on_conflict(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._insert_release(
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

            preview_id = self._preview(
                client,
                auth,
                [
                    {
                        "channel_slug": "darkwood-reverie",
                        "content_type": "LONG",
                        "title": "conflict",
                        "publish_at": "2025-02-01T10:00:00",
                        "notes": "n1",
                    },
                    {
                        "channel_slug": "channel-b",
                        "content_type": "LONG",
                        "title": "would insert",
                        "publish_at": "2025-02-01T11:00:00",
                        "notes": "n2",
                    },
                ],
            )

            before = self._count_releases(env)
            resp = client.post(
                "/v1/planner/import/confirm",
                headers=auth,
                json={"preview_id": preview_id, "mode": "strict"},
            )
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_PREVIEW_NOT_CONFIRMABLE")
            self.assertEqual(self._count_releases(env), before)

    def test_confirm_replace_upsert(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            existing_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Old",
                publish_at="2025-03-05T09:00:00+02:00",
                notes="old-note",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            preview_id = self._preview(
                client,
                auth,
                [
                    {
                        "channel_slug": "darkwood-reverie",
                        "content_type": "SHORT",
                        "title": "New Existing",
                        "publish_at": "2025-03-05T09:00:00",
                        "notes": "new-note",
                    },
                    {
                        "channel_slug": "channel-b",
                        "content_type": "LONG",
                        "title": "Inserted",
                        "publish_at": "2025-03-06T09:00:00",
                        "notes": "ins",
                    },
                ],
            )

            resp = client.post(
                "/v1/planner/import/confirm",
                headers=auth,
                json={"preview_id": preview_id, "mode": "replace"},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertTrue(body["ok"])
            self.assertEqual(body["updated"], 1)
            self.assertEqual(body["inserted"], 1)

            conn = dbm.connect(env)
            try:
                existing = conn.execute("SELECT * FROM planned_releases WHERE id = ?", (existing_id,)).fetchone()
                self.assertEqual(existing["content_type"], "SHORT")
                self.assertEqual(existing["title"], "New Existing")
                self.assertEqual(existing["notes"], "new-note")

                inserted = conn.execute(
                    "SELECT * FROM planned_releases WHERE channel_slug = ? AND publish_at = ?",
                    ("channel-b", "2025-03-06T09:00:00+02:00"),
                ).fetchone()
                self.assertIsNotNone(inserted)
                self.assertEqual(inserted["status"], "PLANNED")
            finally:
                conn.close()

    def test_confirm_preview_single_use(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            preview_id = self._preview(
                client,
                auth,
                [
                    {
                        "channel_slug": "channel-b",
                        "content_type": "LONG",
                        "title": "One",
                        "publish_at": "2025-04-01T09:00:00",
                        "notes": "a",
                    }
                ],
            )

            first = client.post(
                "/v1/planner/import/confirm",
                headers=auth,
                json={"preview_id": preview_id, "mode": "strict"},
            )
            self.assertEqual(first.status_code, 200)

            second = client.post(
                "/v1/planner/import/confirm",
                headers=auth,
                json={"preview_id": preview_id, "mode": "strict"},
            )
            self.assertEqual(second.status_code, 409)
            self.assertEqual(second.json()["error"]["code"], "PLR_PREVIEW_ALREADY_USED")

    def test_confirm_replace_atomic_rollback_on_insert_failure(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            existing_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Old",
                publish_at="2025-05-01T09:00:00+02:00",
                notes="old-note",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            preview_id = self._preview(
                client,
                auth,
                [
                    {
                        "channel_slug": "darkwood-reverie",
                        "content_type": "SHORT",
                        "title": "Updated But Rolled Back",
                        "publish_at": "2025-05-01T09:00:00",
                        "notes": "new-note",
                    },
                    {
                        "channel_slug": "channel-b",
                        "content_type": "LONG",
                        "title": "Insert Then Fail",
                        "publish_at": "2025-05-02T09:00:00",
                        "notes": "ins",
                    },
                ],
            )

            def fail_insert_once(self, normalized):
                raise sqlite3.IntegrityError("boom")

            with patch.object(PlannerImportPreviewService, "_insert_non_conflict_row", fail_insert_once):
                resp = client.post(
                    "/v1/planner/import/confirm",
                    headers=auth,
                    json={"preview_id": preview_id, "mode": "replace"},
                )
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_CONFLICT")

            conn = dbm.connect(env)
            try:
                existing = conn.execute("SELECT * FROM planned_releases WHERE id = ?", (existing_id,)).fetchone()
                self.assertEqual(existing["content_type"], "LONG")
                self.assertEqual(existing["title"], "Old")
                inserted = conn.execute(
                    "SELECT 1 FROM planned_releases WHERE channel_slug = ? AND publish_at = ?",
                    ("channel-b", "2025-05-02T09:00:00+02:00"),
                ).fetchone()
                self.assertIsNone(inserted)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
