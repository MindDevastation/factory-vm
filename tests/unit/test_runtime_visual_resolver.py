from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner.release_job_creation_service import ReleaseJobCreationService
from services.planner.runtime_visual_resolver import (
    apply_release_visual_package,
    resolve_runtime_visual_bindings_for_release,
)
from tests._helpers import seed_minimal_db, temp_env


class TestRuntimeVisualResolver(unittest.TestCase):
    def _channel_id(self, conn: object) -> int:
        row = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
        assert row is not None
        return int(row["id"])

    def test_deferred_resolution_binds_when_job_is_created_later(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = self._channel_id(conn)
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r', 'd', '[]', NULL, NULL, 'origin-later', NULL, 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="bg-later",
                    name="later-bg.png",
                    path="/tmp/later-bg.png",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="cover-later",
                    name="later-cover.png",
                    path="/tmp/later-cover.png",
                )

                first = apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="tester",
                )
                self.assertTrue(first.deferred)

                created = ReleaseJobCreationService(conn).create_or_select(release_id=release_id)
                self.assertEqual(created.result, "CREATED_NEW_JOB")

                rows = conn.execute(
                    "SELECT role, asset_id FROM job_inputs WHERE job_id = ? ORDER BY role ASC",
                    (created.job["id"],),
                ).fetchall()
                self.assertEqual([(str(r["role"]), int(r["asset_id"])) for r in rows], [("BACKGROUND", bg_asset_id), ("COVER", cover_asset_id)])
            finally:
                conn.close()

    def test_resolver_is_noop_without_applied_decision(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = self._channel_id(conn)
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r2', 'd2', '[]', NULL, NULL, 'origin-noop', NULL, 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                out = resolve_runtime_visual_bindings_for_release(conn, release_id=release_id)
                self.assertFalse(out.release_decision_written)
                self.assertFalse(out.runtime_bound)
                self.assertFalse(out.deferred)
                self.assertIsNone(out.job_id)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
