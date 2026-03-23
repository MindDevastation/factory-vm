from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerMetadataBulkPreviewApi(unittest.TestCase):
    def _insert_planner_item(self, env, *, publish_at: str, link_release_id: int | None = None) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES('darkwood-reverie', 'LONG', 'P title', ?, 'P notes', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (publish_at,),
            )
            pid = int(cur.lastrowid)
            if link_release_id is not None:
                conn.execute(
                    "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')",
                    (pid, link_release_id),
                )
            conn.commit()
            return pid
        finally:
            conn.close()

    def test_context_preview_and_session_roundtrip(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-int', 0)
                    """,
                    (channel_id,),
                )
                release_id = int(cur.lastrowid)
                conn.commit()
            finally:
                conn.close()
            p1 = self._insert_planner_item(env, publish_at="2026-01-01T00:00:00Z", link_release_id=release_id)
            p2 = self._insert_planner_item(env, publish_at="2026-01-01T01:00:00Z", link_release_id=None)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            ctx = client.get(f"/v1/planner/metadata-bulk/context?planner_item_ids={p1},{p2}", headers=auth)
            self.assertEqual(ctx.status_code, 200)
            self.assertEqual(ctx.json()["selected_item_count"], 2)

            preview = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [p1, p2], "fields": ["title", "description", "tags"], "overrides": {}},
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertEqual(body["summary"]["selected_item_count"], 2)
            unresolved = next(item for item in body["items"] if item["planner_item_id"] == p2)
            self.assertEqual(unresolved["mapping_status"], "UNRESOLVED_NO_TARGET")

            sid = body["session_id"]
            sess = client.get(f"/v1/planner/metadata-bulk/sessions/{sid}", headers=auth)
            self.assertEqual(sess.status_code, 200)
            self.assertEqual(sess.json()["session_id"], sid)


if __name__ == "__main__":
    unittest.main()
