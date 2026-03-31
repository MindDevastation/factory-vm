from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner import background_assignment_service
from tests._helpers import seed_minimal_db, temp_env


class TestReleaseVisualConfigsContract(unittest.TestCase):
    def test_operational_visual_flow_does_not_use_release_visual_configs_as_canonical_state(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])

                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'visual-contract', 'desc', '[]', NULL, NULL, 'origin-visual-contract', NULL, 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )

                background_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://contract-bg",
                    name="contract-bg.png",
                    path="/tmp/contract-bg.png",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://contract-cover",
                    name="contract-cover.png",
                    path="/tmp/contract-cover.png",
                )
                conn.execute(
                    "INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at) VALUES(?, ?, ?, NULL, 'seed', '2026-01-01T00:00:00+00:00')",
                    (release_id, background_asset_id, cover_asset_id),
                )

                # Seed legacy table with intentionally conflicting payload.
                conn.execute(
                    "INSERT INTO release_visual_configs(release_id, intent_config_json, created_at, updated_at) VALUES(?, ?, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')",
                    (release_id, dbm.json_dumps({"background": {"asset_id": 999999}, "cover": {"asset_id": 888888}})),
                )

                preview = background_assignment_service.preview_background_assignment(
                    conn,
                    release_id=release_id,
                    background_asset_id=background_asset_id,
                    source_family=None,
                    source_reference="local://contract-bg",
                    template_assisted=False,
                    selected_by="tester",
                )
                approved = background_assignment_service.approve_background_assignment(
                    conn,
                    release_id=release_id,
                    preview_id=str(preview["preview_id"]),
                    approved_by="tester",
                )
                applied = background_assignment_service.apply_background_assignment(
                    conn,
                    release_id=release_id,
                    applied_by="tester",
                    stale_token=str(approved["stale_token"]),
                    conflict_token=str(approved["conflict_token"]),
                    reuse_override_confirmed=True,
                )
                self.assertEqual(int(applied["background_asset_id"]), background_asset_id)
                self.assertEqual(int(applied["cover_asset_id"]), cover_asset_id)

                # Operational rows were written in active Epic-5 tables.
                snapshot_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_preview_snapshots WHERE release_id = ?", (release_id,)).fetchone()
                scoped_approval_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM release_visual_approved_previews_scoped WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                applied_row = conn.execute(
                    "SELECT source_preview_id FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert snapshot_count is not None
                assert scoped_approval_count is not None
                assert applied_row is not None
                self.assertGreaterEqual(int(snapshot_count["c"]), 1)
                self.assertGreaterEqual(int(scoped_approval_count["c"]), 1)
                self.assertTrue(str(applied_row["source_preview_id"]))

                # Legacy release_visual_configs row stays untouched and non-canonical.
                legacy = conn.execute(
                    "SELECT intent_config_json, created_at, updated_at FROM release_visual_configs WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert legacy is not None
                self.assertEqual(
                    dbm.json_loads(str(legacy["intent_config_json"])),
                    {"background": {"asset_id": 999999}, "cover": {"asset_id": 888888}},
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
