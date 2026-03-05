from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerBulkDeleteApi(unittest.TestCase):
    def _insert_release(
        self,
        env: Env,
        *,
        status: str = "PLANNED",
        title: str = "Seed",
        publish_at: str = "2025-02-01T10:00:00+02:00",
    ) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "darkwood-reverie",
                    "LONG",
                    title,
                    publish_at,
                    "seed",
                    status,
                    "2025-01-01T00:00:00Z",
                    "2025-01-01T00:00:00Z",
                ),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def _count_releases(self, env: Env) -> int:
        conn = dbm.connect(env)
        try:
            return int(conn.execute("SELECT COUNT(1) AS c FROM planned_releases").fetchone()["c"])
        finally:
            conn.close()

    def test_bulk_delete_fail_all_when_any_id_missing(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first = self._insert_release(env, title="first")
            second = self._insert_release(env, title="second", publish_at="2025-02-01T11:00:00+02:00")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-delete",
                headers=auth,
                json={"ids": [first, 999999, second]},
            )

            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "PLR_NOT_FOUND")
            self.assertEqual(self._count_releases(env), 2)

    def test_bulk_delete_fail_all_when_any_release_locked(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first = self._insert_release(env, title="first")
            second = self._insert_release(env, status="LOCKED", title="locked", publish_at="2025-02-01T11:00:00+02:00")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-delete",
                headers=auth,
                json={"ids": [first, second]},
            )

            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_RELEASE_LOCKED")
            self.assertEqual(self._count_releases(env), 2)

    def test_bulk_delete_success(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first = self._insert_release(env, title="first")
            second = self._insert_release(env, title="second", publish_at="2025-02-01T11:00:00+02:00")
            third = self._insert_release(env, title="third", publish_at="2025-02-01T12:00:00+02:00")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-delete",
                headers=auth,
                json={"ids": [first, second]},
            )

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"deleted_count": 2})
            self.assertEqual(self._count_releases(env), 1)

            conn = dbm.connect(env)
            try:
                remaining = conn.execute("SELECT id FROM planned_releases").fetchall()
                self.assertEqual([int(row["id"]) for row in remaining], [third])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
