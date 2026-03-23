from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner import metadata_bulk_preview_service as svc
from tests._helpers import seed_minimal_db, temp_env


class TestMetadataBulkPreviewService(unittest.TestCase):
    def _insert_planner_item(self, conn, *, channel_slug: str = "darkwood-reverie", status: str = "PLANNED", publish_at: str = "2026-01-01T00:00:00Z") -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES(?, 'LONG', 'P title', ?, 'P notes', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (channel_slug, publish_at, status),
        )
        return int(cur.lastrowid)

    def test_preview_persists_only_bulk_session_and_marks_unresolved(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_id = self._insert_planner_item(conn)
                unresolved_id = self._insert_planner_item(conn, publish_at="2026-01-01T01:00:00Z")
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-1', 0)
                    """,
                    (channel_id,),
                )
                rel_id = int(cur.lastrowid)
                conn.execute(
                    "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')",
                    (planner_id, rel_id),
                )
                conn.commit()

                out = svc.create_bulk_preview_session(
                    conn,
                    planner_item_ids=[planner_id, unresolved_id],
                    fields=["title", "description", "tags"],
                    overrides={},
                    created_by="tester",
                    ttl_seconds=1800,
                )
                self.assertEqual(out["session_status"], "OPEN")
                self.assertEqual(len(out["items"]), 2)
                unresolved = next(item for item in out["items"] if item["planner_item_id"] == unresolved_id)
                self.assertEqual(unresolved["mapping_status"], "UNRESOLVED_NO_TARGET")

                bulk_rows = conn.execute("SELECT COUNT(*) AS c FROM metadata_bulk_preview_sessions").fetchone()["c"]
                self.assertEqual(int(bulk_rows), 1)
                nested_rows = conn.execute("SELECT COUNT(*) AS c FROM metadata_preview_sessions").fetchone()["c"]
                self.assertEqual(int(nested_rows), 0)
            finally:
                conn.close()

    def test_duplicate_release_target_is_deduped(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planner_item(conn)
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-2', 0)
                    """,
                    (channel_id,),
                )
                rid = int(cur.lastrowid)
                conn.execute("INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')", (p1, rid))
                conn.commit()

                out = svc.create_bulk_preview_session(
                    conn,
                    planner_item_ids=[p1, p1],
                    fields=["title"],
                    overrides={},
                    created_by="tester",
                    ttl_seconds=1800,
                )
                self.assertEqual(out["summary"]["selected_item_count"], 2)
                self.assertEqual(out["summary"]["resolved_target_count"], 1)
                self.assertEqual(out["summary"]["deduped_target_count"], 1)
                dup = out["items"][1]
                self.assertEqual(dup["mapping_status"], "DUPLICATE_TARGET")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
