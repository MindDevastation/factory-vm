from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner.background_assignment_service import (
    BackgroundAssignmentError,
    apply_background_assignment,
    approve_background_assignment,
    preview_background_assignment,
)
from services.planner.runtime_visual_resolver import apply_release_visual_package
from tests._helpers import seed_minimal_db, temp_env


class TestRuntimeVisualResolverIntegration(unittest.TestCase):
    def _channel_id(self, conn: object) -> int:
        row = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
        assert row is not None
        return int(row["id"])

    def test_apply_writes_release_decision_and_runtime_bindings_when_job_exists(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = self._channel_id(conn)
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=channel_id,
                    title="runtime visual",
                    description="desc",
                    tags_csv="one,two",
                    cover_name="old-cover.png",
                    cover_ext="png",
                    background_name="old-bg.jpg",
                    background_ext="jpg",
                    audio_ids_text="1,2",
                    job_type="UI",
                )
                job = dbm.get_job(conn, job_id)
                assert job is not None
                release_id = int(job["release_id"])
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
                release_before = conn.execute(
                    "SELECT title, description, tags_json FROM releases WHERE id = ?",
                    (release_id,),
                ).fetchone()
                assert release_before is not None

                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="bg-new",
                    name="new-background.jpeg",
                    path="/tmp/new-background.jpeg",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="cover-new",
                    name="new-cover.webp",
                    path="/tmp/new-cover.webp",
                )

                out = apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="tester",
                )
                self.assertTrue(out.release_decision_written)
                self.assertTrue(out.runtime_bound)
                self.assertFalse(out.deferred)
                self.assertEqual(out.job_id, job_id)

                applied = conn.execute(
                    "SELECT background_asset_id, cover_asset_id FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert applied is not None
                self.assertEqual(int(applied["background_asset_id"]), bg_asset_id)
                self.assertEqual(int(applied["cover_asset_id"]), cover_asset_id)

                role_rows = conn.execute(
                    "SELECT role, asset_id FROM job_inputs WHERE job_id = ? AND role IN ('BACKGROUND','COVER') ORDER BY role ASC",
                    (job_id,),
                ).fetchall()
                self.assertEqual([(str(r["role"]), int(r["asset_id"])) for r in role_rows], [("BACKGROUND", bg_asset_id), ("COVER", cover_asset_id)])

                draft = dbm.get_ui_job_draft(conn, job_id)
                assert draft is not None
                self.assertEqual(str(draft["background_name"]), "new-background.jpeg")
                self.assertEqual(str(draft["background_ext"]), "jpeg")
                self.assertEqual(str(draft["cover_name"]), "new-cover.webp")
                self.assertEqual(str(draft["cover_ext"]), "webp")

                release_after = conn.execute(
                    "SELECT title, description, tags_json FROM releases WHERE id = ?",
                    (release_id,),
                ).fetchone()
                assert release_after is not None
                self.assertEqual(str(release_after["title"]), str(release_before["title"]))
                self.assertEqual(str(release_after["description"]), str(release_before["description"]))
                self.assertEqual(str(release_after["tags_json"]), str(release_before["tags_json"]))
            finally:
                conn.close()

    def test_apply_writes_release_decision_only_when_open_job_absent(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = self._channel_id(conn)
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r', 'd', '[]', NULL, NULL, 'origin-1', NULL, 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="bg-only",
                    name="bg-only.png",
                    path="/tmp/bg-only.png",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="cover-only",
                    name="cover-only.png",
                    path="/tmp/cover-only.png",
                )

                out = apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="tester",
                )
                self.assertTrue(out.release_decision_written)
                self.assertFalse(out.runtime_bound)
                self.assertTrue(out.deferred)
                self.assertIsNone(out.job_id)

                applied = conn.execute(
                    "SELECT background_asset_id, cover_asset_id FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert applied is not None
                self.assertEqual(int(applied["background_asset_id"]), bg_asset_id)
                self.assertEqual(int(applied["cover_asset_id"]), cover_asset_id)

                inputs_count = conn.execute("SELECT COUNT(*) AS c FROM job_inputs").fetchone()
                assert inputs_count is not None
                self.assertEqual(int(inputs_count["c"]), 0)
            finally:
                conn.close()

    def test_background_only_apply_preserves_existing_applied_cover(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = self._channel_id(conn)
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r3', 'd3', '[]', NULL, NULL, 'origin-s2-preserve-cover', NULL, 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                previous_bg = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="bg-prev",
                    name="bg-prev.png",
                    path="/tmp/bg-prev.png",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="cover-prev",
                    name="cover-prev.png",
                    path="/tmp/cover-prev.png",
                )
                apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=previous_bg,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="seed",
                )
                next_bg = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="MANAGED",
                    origin_id="managed://next-bg",
                    name="next-bg.png",
                    path="/tmp/next-bg.png",
                )

                preview = preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=next_bg,
                    source_family="managed_library",
                    source_reference="managed://next-bg",
                    template_assisted=False,
                    selected_by="tester",
                )
                approve_background_assignment(conn, release_id=release_id, preview_id=str(preview["preview_id"]), approved_by="tester")
                apply_background_assignment(conn, release_id=release_id, applied_by="tester")

                applied = conn.execute(
                    "SELECT background_asset_id, cover_asset_id FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert applied is not None
                self.assertEqual(int(applied["background_asset_id"]), next_bg)
                self.assertEqual(int(applied["cover_asset_id"]), cover_asset_id)
            finally:
                conn.close()

    def test_background_only_apply_forbidden_when_no_canonical_cover(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = self._channel_id(conn)
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r4', 'd4', '[]', NULL, NULL, 'origin-s2-no-cover', NULL, 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="bg-only-s2",
                    name="bg-only-s2.png",
                    path="/tmp/bg-only-s2.png",
                )
                preview = preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    source_family="operator_imported",
                    source_reference="bg-only-s2",
                    template_assisted=False,
                    selected_by="tester",
                )
                approve_background_assignment(conn, release_id=release_id, preview_id=str(preview["preview_id"]), approved_by="tester")
                with self.assertRaises(BackgroundAssignmentError) as ctx:
                    apply_background_assignment(conn, release_id=release_id, applied_by="tester")
                self.assertEqual(ctx.exception.code, "VBG_CANONICAL_COVER_REQUIRED")
                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert row is not None
                self.assertEqual(int(row["c"]), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
