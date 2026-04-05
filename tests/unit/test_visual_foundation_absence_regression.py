from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api.ui_jobs_enqueue import enqueue_ui_render_job
from services.ui_jobs.retry_service import retry_failed_ui_job
from tests._helpers import seed_minimal_db, temp_env


class TestVisualFoundationAbsenceRegression(unittest.TestCase):
    def test_retry_runtime_like_path_works_without_visual_rows(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])

                source_job_id = dbm.create_ui_job_draft(
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

                enqueue_result = enqueue_ui_render_job(
                    conn,
                    job_id=source_job_id,
                    channel_id=channel_id,
                    tracks=[
                        {"file_id": "track-1", "filename": "track-1.wav"},
                        {"file_id": "track-2", "filename": "track-2.wav"},
                    ],
                    background_file_id="bg-1",
                    background_filename="bg-1.jpg",
                    cover_file_id="cover-1",
                    cover_filename="cover-1.png",
                )
                self.assertTrue(enqueue_result.enqueued)

                dbm.update_job_state(conn, source_job_id, state="FAILED", stage="RENDER", error_reason="boom")
                retry = retry_failed_ui_job(conn, source_job_id=source_job_id)
                self.assertTrue(retry.created)

                retry_job = dbm.get_job(conn, retry.retry_job_id)
                assert retry_job is not None
                self.assertEqual(str(retry_job["state"]), "READY_FOR_RENDER")
                self.assertEqual(str(retry_job["stage"]), "FETCH")

                role_rows = conn.execute(
                    """
                    SELECT role, COUNT(*) AS c
                    FROM job_inputs
                    WHERE job_id = ?
                    GROUP BY role
                    """,
                    (retry.retry_job_id,),
                ).fetchall()
                role_counts = {str(r["role"]): int(r["c"]) for r in role_rows}
                self.assertEqual(role_counts.get("TRACK", 0), 2)
                self.assertEqual(role_counts.get("BACKGROUND", 0), 1)
                self.assertEqual(role_counts.get("COVER", 0), 1)

                visual_config_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_configs").fetchone()
                visual_snapshot_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_preview_snapshots").fetchone()
                visual_approved_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_approved_previews").fetchone()
                visual_approved_scoped_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_approved_previews_scoped").fetchone()
                visual_applied_count = conn.execute("SELECT COUNT(*) AS c FROM release_visual_applied_packages").fetchone()
                self.assertEqual(int(visual_config_count["c"]), 0)
                self.assertEqual(int(visual_snapshot_count["c"]), 0)
                self.assertEqual(int(visual_approved_count["c"]), 0)
                self.assertEqual(int(visual_approved_scoped_count["c"]), 0)
                self.assertEqual(int(visual_applied_count["c"]), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
