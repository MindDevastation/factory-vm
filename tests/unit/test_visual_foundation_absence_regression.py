from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestVisualFoundationAbsenceRegression(unittest.TestCase):
    def test_existing_release_job_draft_and_asset_binding_work_without_visual_rows(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])

                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=channel_id,
                    title="Visual absence regression",
                    description="desc",
                    tags_csv="tag-a,tag-b",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="11,22",
                )

                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "DRAFT")

                draft = dbm.get_ui_job_draft(conn, job_id)
                self.assertEqual(str(draft["background_name"]), "bg")

                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="image",
                    origin="local",
                    origin_id=None,
                    name="bg.jpg",
                    path="/tmp/bg.jpg",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="image",
                    origin="local",
                    origin_id=None,
                    name="cover.png",
                    path="/tmp/cover.png",
                )
                dbm.link_job_input(conn, job_id, bg_asset_id, "background", 0)
                dbm.link_job_input(conn, job_id, cover_asset_id, "cover", 1)

                input_count = conn.execute("SELECT COUNT(*) AS c FROM job_inputs WHERE job_id = ?", (job_id,)).fetchone()
                self.assertEqual(int(input_count["c"]), 2)

                visual_config_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_configs").fetchone()
                visual_snapshot_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_preview_snapshots").fetchone()
                visual_approved_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_approved_previews").fetchone()
                visual_applied_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_applied_packages").fetchone()
                self.assertEqual(int(visual_config_count["c"]), 0)
                self.assertEqual(int(visual_snapshot_count["c"]), 0)
                self.assertEqual(int(visual_approved_count["c"]), 0)
                self.assertEqual(int(visual_applied_count["c"]), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
