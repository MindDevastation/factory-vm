from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerApiBulkCreate(unittest.TestCase):
    def _release_rows(self, env: Env, channel_slug: str) -> list[dict]:
        conn = dbm.connect(env)
        try:
            return conn.execute(
                """
                SELECT id, channel_slug, content_type, title, publish_at, notes
                FROM planned_releases
                WHERE channel_slug = ?
                ORDER BY publish_at ASC, id ASC
                """,
                (channel_slug,),
            ).fetchall()
        finally:
            conn.close()

    def _insert_release(self, env: Env, *, channel_slug: str, publish_at: str, title: str) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES (?, 'LONG', ?, ?, 'seed', 'PLANNED', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """,
                (channel_slug, title, publish_at),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def test_bulk_create_strict_conflict_is_atomic(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                publish_at="2025-01-10T10:00:00+02:00",
                title="already-there",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-create",
                headers=auth,
                json={
                    "channel_slug": "darkwood-reverie",
                    "content_type": "LONG",
                    "title": "new title",
                    "notes": "new notes",
                    "count": 2,
                    "start_publish_at": "2025-01-10T10:00:00+02:00",
                    "step": "PT1H",
                    "mode": "strict",
                },
            )

            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_CONFLICT")
            rows = self._release_rows(env, "darkwood-reverie")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["title"], "already-there")

    def test_bulk_create_replace_updates_and_inserts_atomically(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            existing_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                publish_at="2025-01-10T10:00:00+02:00",
                title="old title",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-create",
                headers=auth,
                json={
                    "channel_slug": "darkwood-reverie",
                    "content_type": "SHORT",
                    "title": "fresh title",
                    "notes": "fresh notes",
                    "count": 2,
                    "start_publish_at": "2025-01-10T10:00:00+02:00",
                    "step": "PT1H",
                    "mode": "replace",
                },
            )

            self.assertEqual(resp.status_code, 201)
            body = resp.json()
            self.assertEqual(body["created_count"], 1)
            self.assertEqual(body["updated_count"], 1)
            self.assertEqual(len(body["affected_ids"]), 2)
            self.assertIn(existing_id, body["affected_ids"])

            rows = self._release_rows(env, "darkwood-reverie")
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["publish_at"], "2025-01-10T10:00:00+02:00")
            self.assertEqual(rows[0]["title"], "fresh title")
            self.assertEqual(rows[0]["content_type"], "SHORT")
            self.assertEqual(rows[1]["publish_at"], "2025-01-10T11:00:00+02:00")
            self.assertEqual(rows[1]["title"], "fresh title")

    def test_bulk_create_rejects_unsupported_step_token(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-create",
                headers=auth,
                json={
                    "channel_slug": "darkwood-reverie",
                    "content_type": "LONG",
                    "title": "new title",
                    "notes": "new notes",
                    "count": 2,
                    "start_publish_at": "2025-01-10T10:00:00+02:00",
                    "step": "P1W",
                },
            )

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")


if __name__ == "__main__":
    unittest.main()
