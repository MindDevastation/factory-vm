from __future__ import annotations

import json
import unittest

from services.common import db as dbm
from services.planner import background_assignment_service as bg_svc
from tests._helpers import seed_minimal_db, temp_env


class TestVisualScopedPreviewApproval(unittest.TestCase):
    def _seed_release_and_assets(self, conn: object) -> tuple[int, int, int, int]:
        channel_row = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
        assert channel_row is not None
        channel_id = int(channel_row["id"])
        release_id = int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                VALUES(?, 'Scoped release', 'desc', '[]', NULL, NULL, 'origin-scoped', NULL, 1.0)
                """,
                (channel_id,),
            ).lastrowid
        )
        background_asset_id = dbm.create_asset(
            conn,
            channel_id=channel_id,
            kind="IMAGE",
            origin="LOCAL",
            origin_id="bg-local-1",
            name="bg-local-1.png",
            path="/tmp/bg-local-1.png",
        )
        cover_asset_id = dbm.create_asset(
            conn,
            channel_id=channel_id,
            kind="IMAGE",
            origin="LOCAL",
            origin_id="cover-local-1",
            name="cover-local-1.png",
            path="/tmp/cover-local-1.png",
        )
        alt_cover_asset_id = dbm.create_asset(
            conn,
            channel_id=channel_id,
            kind="IMAGE",
            origin="LOCAL",
            origin_id="cover-local-2",
            name="cover-local-2.png",
            path="/tmp/cover-local-2.png",
        )
        return release_id, background_asset_id, cover_asset_id, alt_cover_asset_id

    def _insert_preview(self, conn: object, *, release_id: int, preview_id: str, preview_scope: str) -> None:
        conn.execute(
            """
            INSERT INTO release_visual_preview_snapshots(
                id, release_id, preview_scope, intent_snapshot_json, preview_package_json, created_by, created_at
            ) VALUES(?, ?, ?, ?, ?, 'tester', '2026-03-31T00:00:00+00:00')
            """,
            (
                preview_id,
                release_id,
                preview_scope,
                "{}",
                '{"background_asset_id":1,"cover_asset_id":2}',
            ),
        )

    def test_scoped_approvals_coexist_without_clobbering_cross_scope(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, _, _, _ = self._seed_release_and_assets(conn)
                now = "2026-03-31T00:00:00+00:00"
                self._insert_preview(conn, release_id=release_id, preview_id="pv-bg-1", preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND)
                self._insert_preview(conn, release_id=release_id, preview_id="pv-cover-1", preview_scope=dbm.VISUAL_PREVIEW_SCOPE_COVER)
                self._insert_preview(conn, release_id=release_id, preview_id="pv-package-1", preview_scope=dbm.VISUAL_PREVIEW_SCOPE_PACKAGE)
                dbm.upsert_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND,
                    preview_id="pv-bg-1",
                    approved_by="alice",
                    approved_at=now,
                )
                dbm.upsert_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_COVER,
                    preview_id="pv-cover-1",
                    approved_by="bob",
                    approved_at=now,
                )
                dbm.upsert_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_PACKAGE,
                    preview_id="pv-package-1",
                    approved_by="carol",
                    approved_at=now,
                )
                bg = dbm.get_release_visual_approved_preview(
                    conn, release_id=release_id, preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND
                )
                cover = dbm.get_release_visual_approved_preview(
                    conn, release_id=release_id, preview_scope=dbm.VISUAL_PREVIEW_SCOPE_COVER
                )
                package = dbm.get_release_visual_approved_preview(
                    conn, release_id=release_id, preview_scope=dbm.VISUAL_PREVIEW_SCOPE_PACKAGE
                )
                self.assertEqual(str(bg["preview_id"]), "pv-bg-1")
                self.assertEqual(str(cover["preview_id"]), "pv-cover-1")
                self.assertEqual(str(package["preview_id"]), "pv-package-1")
            finally:
                conn.close()

    def test_same_scope_intentional_replace_only_replaces_that_scope(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, _, _, _ = self._seed_release_and_assets(conn)
                now = "2026-03-31T00:00:00+00:00"
                self._insert_preview(conn, release_id=release_id, preview_id="pv-bg-1", preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND)
                self._insert_preview(conn, release_id=release_id, preview_id="pv-bg-2", preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND)
                self._insert_preview(conn, release_id=release_id, preview_id="pv-cover-1", preview_scope=dbm.VISUAL_PREVIEW_SCOPE_COVER)
                dbm.upsert_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND,
                    preview_id="pv-bg-1",
                    approved_by="alice",
                    approved_at=now,
                )
                dbm.upsert_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_COVER,
                    preview_id="pv-cover-1",
                    approved_by="bob",
                    approved_at=now,
                )
                dbm.upsert_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND,
                    preview_id="pv-bg-2",
                    approved_by="alice",
                    approved_at=now,
                )
                bg = dbm.get_release_visual_approved_preview(
                    conn, release_id=release_id, preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND
                )
                cover = dbm.get_release_visual_approved_preview(
                    conn, release_id=release_id, preview_scope=dbm.VISUAL_PREVIEW_SCOPE_COVER
                )
                self.assertEqual(str(bg["preview_id"]), "pv-bg-2")
                self.assertEqual(str(cover["preview_id"]), "pv-cover-1")
            finally:
                conn.close()

    def test_background_apply_uses_scoped_approval_and_runtime_contract_stays_compatible(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, background_asset_id, cover_asset_id, _ = self._seed_release_and_assets(conn)
                conn.execute(
                    """
                    INSERT INTO release_visual_applied_packages(
                        release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at
                    ) VALUES(?, ?, ?, NULL, 'seed', '2026-03-31T00:00:00+00:00')
                    """,
                    (release_id, background_asset_id, cover_asset_id),
                )
                preview = bg_svc.preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=background_asset_id,
                    source_family=None,
                    source_reference=None,
                    template_assisted=False,
                    selected_by="operator",
                )
                bg_svc.approve_background_assignment(
                    conn,
                    release_id=release_id,
                    preview_id=str(preview["preview_id"]),
                    approved_by="operator",
                )
                scoped_approval = dbm.get_release_visual_approved_preview(
                    conn,
                    release_id=release_id,
                    preview_scope=dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND,
                )
                self.assertEqual(str(scoped_approval["preview_id"]), str(preview["preview_id"]))

                legacy_approval = conn.execute(
                    "SELECT preview_id FROM release_visual_approved_previews WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                self.assertIsNotNone(legacy_approval)
                self.assertEqual(str(legacy_approval["preview_id"]), str(preview["preview_id"]))

                applied = bg_svc.apply_background_assignment(conn, release_id=release_id, applied_by="operator")
                self.assertEqual(int(applied["background_asset_id"]), background_asset_id)
                self.assertEqual(int(applied["cover_asset_id"]), cover_asset_id)
                self.assertEqual(applied["summary"]["thumbnail_source"]["source_kind"], "cover_asset")

                snapshot = conn.execute(
                    "SELECT preview_scope, preview_package_json FROM release_visual_preview_snapshots WHERE id = ?",
                    (str(preview["preview_id"]),),
                ).fetchone()
                self.assertEqual(str(snapshot["preview_scope"]), dbm.VISUAL_PREVIEW_SCOPE_BACKGROUND)
                parsed = json.loads(str(snapshot["preview_package_json"]))
                self.assertEqual(int(parsed["background_asset_id"]), background_asset_id)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
