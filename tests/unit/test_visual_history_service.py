from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner import cover_assignment_service as cover_svc
from services.planner import visual_history_service as history_svc
from services.planner.runtime_visual_resolver import apply_release_visual_package
from tests._helpers import seed_minimal_db, temp_env


class TestVisualHistoryService(unittest.TestCase):
    def test_exact_reuse_detection_returns_prior_usage_context(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])
                bg = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="bg://1", name="bg1.png", path="/tmp/bg1.png")
                cover = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="cover://1", name="cover1.png", path="/tmp/cover1.png")
                old_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at) VALUES(?, 'old', 'd', '[]', NULL, NULL, 'origin-old-vh', NULL, 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                new_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at) VALUES(?, 'new', 'd', '[]', NULL, NULL, 'origin-new-vh', NULL, 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                apply_release_visual_package(conn, release_id=old_release, background_asset_id=bg, cover_asset_id=cover, source_preview_id=None, applied_by="seed")
                reuse = history_svc.lookup_exact_reuse_warnings(
                    conn,
                    release_id=new_release,
                    background_asset_id=bg,
                    cover_asset_id=cover,
                )
                self.assertTrue(reuse["requires_override"])
                self.assertIn("same visual package identity already applied in channel", reuse["warnings"])
                self.assertEqual(int(reuse["prior_usage"][0]["release_id"]), old_release)
            finally:
                conn.close()

    def test_soft_warning_override_required_but_apply_possible_with_override(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])
                bg = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="bg://2", name="bg2.png", path="/tmp/bg2.png")
                cover = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="cover://2", name="cover2.png", path="/tmp/cover2.png")
                old_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at) VALUES(?, 'old2', 'd', '[]', NULL, NULL, 'origin-old2-vh', NULL, 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                new_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at) VALUES(?, 'new2', 'd', '[]', NULL, NULL, 'origin-new2-vh', NULL, 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                apply_release_visual_package(conn, release_id=old_release, background_asset_id=bg, cover_asset_id=cover, source_preview_id=None, applied_by="seed")
                apply_release_visual_package(conn, release_id=new_release, background_asset_id=bg, cover_asset_id=cover, source_preview_id=None, applied_by="seed")
                candidate = cover_svc.create_cover_candidate_reference(
                    conn,
                    release_id=new_release,
                    cover_asset_id=cover,
                    source_provider_family="manual",
                    source_reference="manual://same",
                    input_payload_id=None,
                    selection_mode="manual",
                    template_ref=None,
                    created_by="tester",
                )
                cover_svc.select_cover_candidate_for_approval(conn, release_id=new_release, candidate_id=str(candidate["candidate_id"]), selected_by="tester")
                approved = cover_svc.approve_cover_candidate(conn, release_id=new_release, candidate_id=None, approved_by="tester")
                with self.assertRaises(Exception) as ctx:
                    cover_svc.apply_cover_candidate(
                        conn,
                        release_id=new_release,
                        applied_by="tester",
                        stale_token=str(approved["stale_token"]),
                        conflict_token=str(approved["conflict_token"]),
                    )
                self.assertEqual(getattr(ctx.exception, "code", ""), "VCOVER_REUSE_OVERRIDE_REQUIRED")
                applied = cover_svc.apply_cover_candidate(
                    conn,
                    release_id=new_release,
                    applied_by="tester",
                    reuse_override_confirmed=True,
                    stale_token=str(approved["stale_token"]),
                    conflict_token=str(approved["conflict_token"]),
                )
                self.assertTrue(applied["reuse"]["requires_override"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
