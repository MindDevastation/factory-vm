from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerApiBulkCreate(unittest.TestCase):
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

    def _fetch_all(self, env: Env) -> list[dict]:
        conn = dbm.connect(env)
        try:
            return list(
                conn.execute(
                    "SELECT id, channel_slug, content_type, title, publish_at, notes FROM planned_releases ORDER BY id ASC"
                ).fetchall()
            )
        finally:
            conn.close()

    def test_bulk_create_strict_conflict_is_fail_all_atomic(self) -> None:
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

            resp = client.post(
                "/v1/planner/releases/bulk-create",
                headers=auth,
                json={
                    "channel_slug": "darkwood-reverie",
                    "content_type": "LONG",
                    "title": "Batch",
                    "count": 2,
                    "start_publish_at": "2025-02-01T10:00:00",
                    "step": "PT1H",
                    "mode": "strict",
                },
            )

            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_CONFLICT")

            rows = self._fetch_all(env)
            self.assertEqual(len(rows), 1)
            self.assertEqual(int(rows[0]["id"]), existing_id)
            self.assertEqual(rows[0]["title"], "Existing")

    def test_bulk_create_replace_updates_and_inserts(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            existing_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Old",
                publish_at="2025-02-01T10:00:00+02:00",
                notes="old-notes",
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
                    "title": "New Shared",
                    "notes": "new-notes",
                    "count": 2,
                    "start_publish_at": "2025-02-01T10:00:00",
                    "step": "PT30M",
                    "mode": "replace",
                },
            )

            self.assertEqual(resp.status_code, 201)
            body = resp.json()
            self.assertEqual(body["created_count"], 1)
            self.assertEqual(body["updated_count"], 1)
            self.assertEqual(len(body["affected_ids"]), 2)

            rows = self._fetch_all(env)
            self.assertEqual(len(rows), 2)
            updated = next(r for r in rows if int(r["id"]) == existing_id)
            created = next(r for r in rows if int(r["id"]) != existing_id)
            self.assertEqual(updated["content_type"], "SHORT")
            self.assertEqual(updated["title"], "New Shared")
            self.assertEqual(updated["notes"], "new-notes")
            self.assertEqual(created["publish_at"], "2025-02-01T10:30:00+02:00")

    def test_bulk_create_requires_step_for_series_when_start_set(self) -> None:
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
                    "count": 2,
                    "start_publish_at": "2025-02-01T10:00:00",
                },
            )

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_bulk_create_start_publish_at_null_step_not_required(self) -> None:
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
                    "count": 3,
                    "start_publish_at": None,
                    "mode": "strict",
                },
            )

            self.assertEqual(resp.status_code, 201)
            body = resp.json()
            self.assertEqual(body["created_count"], 3)
            self.assertEqual(body["updated_count"], 0)

            rows = self._fetch_all(env)
            self.assertEqual(len(rows), 3)
            self.assertTrue(all(row["publish_at"] is None for row in rows))

    def test_bulk_create_strict_large_count_works(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            count = 1200
            start_publish_at = "2025-02-01T10:00:00"
            resp = client.post(
                "/v1/planner/releases/bulk-create",
                headers=auth,
                json={
                    "channel_slug": "darkwood-reverie",
                    "content_type": "LONG",
                    "title": "Big Batch",
                    "count": count,
                    "start_publish_at": start_publish_at,
                    "step": "PT1M",
                    "mode": "strict",
                },
            )

            self.assertEqual(resp.status_code, 201)
            body = resp.json()
            self.assertEqual(body["created_count"], count)
            self.assertEqual(body["updated_count"], 0)

            conn = dbm.connect(env)
            try:
                total = conn.execute("SELECT COUNT(1) AS c FROM planned_releases").fetchone()["c"]
                self.assertEqual(int(total), count)
                first_row = conn.execute(
                    "SELECT id FROM planned_releases WHERE channel_slug = ? AND publish_at = ?",
                    ("darkwood-reverie", "2025-02-01T10:00:00+02:00"),
                ).fetchone()
                last_row = conn.execute(
                    "SELECT id FROM planned_releases WHERE channel_slug = ? AND publish_at = ?",
                    ("darkwood-reverie", "2025-02-02T05:59:00+02:00"),
                ).fetchone()
                self.assertIsNotNone(first_row)
                self.assertIsNotNone(last_row)
            finally:
                conn.close()

    def test_bulk_create_count_bounds_invalid(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            for invalid_count in (0, 5001):
                resp = client.post(
                    "/v1/planner/releases/bulk-create",
                    headers=auth,
                    json={
                        "channel_slug": "darkwood-reverie",
                        "content_type": "LONG",
                        "count": invalid_count,
                    },
                )
                self.assertEqual(resp.status_code, 400)
                self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_bulk_create_rejects_week_duration_token(self) -> None:
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
                    "count": 2,
                    "start_publish_at": "2025-02-01T10:00:00",
                    "step": "P1W",
                },
            )

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")


if __name__ == "__main__":
    unittest.main()
