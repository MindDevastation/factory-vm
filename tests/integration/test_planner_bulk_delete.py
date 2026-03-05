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
        channel_slug: str,
        content_type: str,
        title: str,
        publish_at: str,
        status: str = "PLANNED",
    ) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (channel_slug, content_type, title, publish_at, "seed", status, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
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

    def _has_release(self, env: Env, release_id: int) -> bool:
        conn = dbm.connect(env)
        try:
            row = conn.execute("SELECT 1 FROM planned_releases WHERE id = ?", (release_id,)).fetchone()
            return row is not None
        finally:
            conn.close()

    def test_bulk_delete_fail_all_when_any_id_not_found(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Keep",
                publish_at="2025-01-01T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post("/v1/planner/releases/bulk-delete", headers=auth, json={"ids": [rid, rid + 9999]})

            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "PLR_NOT_FOUND")
            self.assertTrue(self._has_release(env, rid))
            self.assertEqual(self._count_releases(env), 1)

    def test_bulk_delete_fail_all_when_any_release_locked(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Planned",
                publish_at="2025-01-01T10:00:00+02:00",
                status="PLANNED",
            )
            locked_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Locked",
                publish_at="2025-01-02T10:00:00+02:00",
                status="LOCKED",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-delete",
                headers=auth,
                json={"ids": [planned_id, locked_id]},
            )

            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_RELEASE_LOCKED")
            self.assertTrue(self._has_release(env, planned_id))
            self.assertTrue(self._has_release(env, locked_id))
            self.assertEqual(self._count_releases(env), 2)

    def test_bulk_delete_success(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="One",
                publish_at="2025-01-01T10:00:00+02:00",
            )
            second_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Two",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/planner/releases/bulk-delete",
                headers=auth,
                json={"ids": [first_id, second_id]},
            )

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"deleted_count": 2})
            self.assertFalse(self._has_release(env, first_id))
            self.assertFalse(self._has_release(env, second_id))
            self.assertEqual(self._count_releases(env), 0)


if __name__ == "__main__":
    unittest.main()
