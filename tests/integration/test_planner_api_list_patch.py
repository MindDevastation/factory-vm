from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerApiListPatch(unittest.TestCase):
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
                (channel_slug, content_type, title, publish_at, "notes", status, "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def _get_release_channel_slug(self, env: Env, release_id: int) -> str:
        conn = dbm.connect(env)
        try:
            row = conn.execute("SELECT channel_slug FROM planned_releases WHERE id = ?", (release_id,)).fetchone()
            assert row is not None
            return str(row["channel_slug"])
        finally:
            conn.close()

    def test_list_filters_sort_search_and_pagination(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Dream Sequence",
                publish_at="2025-01-02T10:00:00+02:00",
            )
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="SHORT",
                title="Night Pulse",
                publish_at="2025-01-03T10:00:00+02:00",
            )
            self._insert_release(
                env,
                channel_slug="channel-b",
                content_type="LONG",
                title="Dawn Echo",
                publish_at="2025-01-04T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/planner/releases?channel_slug=darkwood-reverie&content_type=LONG&q=dream&sort_by=publish_at&sort_dir=asc&page=1&page_size=1",
                headers=auth,
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["pagination"], {"page": 1, "page_size": 1, "total": 1})
            self.assertEqual([item["title"] for item in body["items"]], ["Dream Sequence"])

    def test_patch_locked_returns_409(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
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

            resp = client.patch(f"/v1/planner/releases/{rid}", json={"title": "Updated"}, headers=auth)
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_RELEASE_LOCKED")

    def test_patch_rejects_status_field(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Editable",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(f"/v1/planner/releases/{rid}", json={"status": "LOCKED"}, headers=auth)
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_FIELD_NOT_EDITABLE")

    def test_patch_uniqueness_conflict_returns_409(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="First",
                publish_at="2025-01-02T10:00:00+02:00",
            )
            second = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Second",
                publish_at="2025-01-03T10:00:00+02:00",
            )
            self.assertNotEqual(first, second)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(
                f"/v1/planner/releases/{second}",
                json={"publish_at": "2025-01-02T10:00:00"},
                headers=auth,
            )
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_CONFLICT")

    def test_patch_malformed_json_returns_400_invalid_input(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Malformed",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(
                f"/v1/planner/releases/{rid}",
                data='{"title": ',
                headers={**auth, "Content-Type": "application/json"},
            )
            self.assertEqual(resp.status_code, 400)
            self.assertIn("error", resp.json())
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_patch_empty_or_non_object_payload_returns_400_invalid_input(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Payload",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            empty_resp = client.patch(
                f"/v1/planner/releases/{rid}",
                data="",
                headers={**auth, "Content-Type": "application/json"},
            )
            self.assertEqual(empty_resp.status_code, 400)
            self.assertIn("error", empty_resp.json())
            self.assertEqual(empty_resp.json()["error"]["code"], "PLR_INVALID_INPUT")

            list_resp = client.patch(f"/v1/planner/releases/{rid}", json=["not", "object"], headers=auth)
            self.assertEqual(list_resp.status_code, 400)
            self.assertIn("error", list_resp.json())
            self.assertEqual(list_resp.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_patch_channel_slug_empty_string_rejected_and_not_saved(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Empty Slug",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            before = self._get_release_channel_slug(env, rid)
            resp = client.patch(f"/v1/planner/releases/{rid}", json={"channel_slug": ""}, headers=auth)
            after = self._get_release_channel_slug(env, rid)

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")
            self.assertEqual(before, "darkwood-reverie")
            self.assertEqual(after, "darkwood-reverie")

    def test_patch_channel_slug_whitespace_rejected_and_not_saved(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Whitespace Slug",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            before = self._get_release_channel_slug(env, rid)
            resp = client.patch(f"/v1/planner/releases/{rid}", json={"channel_slug": "   "}, headers=auth)
            after = self._get_release_channel_slug(env, rid)

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")
            self.assertEqual(before, "darkwood-reverie")
            self.assertEqual(after, "darkwood-reverie")


if __name__ == "__main__":
    unittest.main()
