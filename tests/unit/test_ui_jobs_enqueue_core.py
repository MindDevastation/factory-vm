from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api.ui_jobs_enqueue import enqueue_ui_render_job

from tests._helpers import seed_minimal_db, temp_env


class TestUiJobsEnqueueCore(unittest.TestCase):
    def _create_ui_draft_job(self, env) -> int:
        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert channel is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(channel["id"]),
                title="UI Draft",
                description="desc",
                tags_csv="tag1,tag2",
                cover_name="cover.png",
                cover_ext="png",
                background_name="bg.png",
                background_ext="png",
                audio_ids_text="a1,a2",
                job_type="UI",
            )
        finally:
            conn.close()

    def test_enqueue_sets_ready_for_render_for_draft_without_inputs(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_ui_draft_job(env)

            conn = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn, job_id)
                assert draft is not None
                result = enqueue_ui_render_job(
                    conn,
                    job_id=job_id,
                    channel_id=int(draft["channel_id"]),
                    tracks=[{"file_id": "track1", "filename": "track1.wav"}],
                    background_file_id="bg1",
                    background_filename="bg1.png",
                    cover_file_id="cover1",
                    cover_filename="cover1.png",
                )
                job = dbm.get_job(conn, job_id)
                inputs = conn.execute("SELECT COUNT(*) AS c FROM job_inputs WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert job is not None
            assert inputs is not None
            self.assertTrue(result.enqueued)
            self.assertEqual(result.reason, "enqueued")
            self.assertEqual(job["state"], "READY_FOR_RENDER")
            self.assertEqual(int(inputs["c"]), 3)

    def test_enqueue_is_noop_when_inputs_already_exist(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_ui_draft_job(env)

            conn = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn, job_id)
                assert draft is not None
                aid = dbm.create_asset(
                    conn,
                    channel_id=int(draft["channel_id"]),
                    kind="AUDIO",
                    origin="LOCAL",
                    origin_id="local_track",
                    name="local.wav",
                    path="/tmp/local.wav",
                )
                dbm.link_job_input(conn, job_id, aid, "TRACK", 0)

                result = enqueue_ui_render_job(
                    conn,
                    job_id=job_id,
                    channel_id=int(draft["channel_id"]),
                    tracks=[{"file_id": "track1", "filename": "track1.wav"}],
                    background_file_id="bg1",
                    background_filename="bg1.png",
                    cover_file_id="",
                    cover_filename="",
                )
                job = dbm.get_job(conn, job_id)
                inputs = conn.execute("SELECT COUNT(*) AS c FROM job_inputs WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert job is not None
            assert inputs is not None
            self.assertFalse(result.enqueued)
            self.assertEqual(result.reason, "already_in_progress")
            self.assertEqual(job["state"], "DRAFT")
            self.assertEqual(int(inputs["c"]), 1)

    def test_enqueue_rejects_non_draft_job(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_ui_draft_job(env)

            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, job_id, state="READY_FOR_RENDER", stage="FETCH")
                draft = dbm.get_ui_job_draft(conn, job_id)
                assert draft is not None
                result = enqueue_ui_render_job(
                    conn,
                    job_id=job_id,
                    channel_id=int(draft["channel_id"]),
                    tracks=[{"file_id": "track1", "filename": "track1.wav"}],
                    background_file_id="bg1",
                    background_filename="bg1.png",
                    cover_file_id="",
                    cover_filename="",
                )
                job = dbm.get_job(conn, job_id)
            finally:
                conn.close()

            assert job is not None
            self.assertFalse(result.enqueued)
            self.assertEqual(result.reason, "not_allowed")
            self.assertEqual(job["state"], "READY_FOR_RENDER")

    def test_enqueue_multi_track_job(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_ui_draft_job(env)

            conn = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn, job_id)
                assert draft is not None
                result = enqueue_ui_render_job(
                    conn,
                    job_id=job_id,
                    channel_id=int(draft["channel_id"]),
                    tracks=[
                        {"file_id": "track1", "filename": "track1.wav"},
                        {"file_id": "track2", "filename": "track2.wav"},
                        {"file_id": "track3", "filename": "track3.wav"},
                    ],
                    background_file_id="bg1",
                    background_filename="bg1.png",
                    cover_file_id="cover1",
                    cover_filename="cover1.png",
                )
                job = dbm.get_job(conn, job_id)
                role_counts = conn.execute(
                    """
                    SELECT role, COUNT(*) AS c
                    FROM job_inputs
                    WHERE job_id=?
                    GROUP BY role
                    """,
                    (job_id,),
                ).fetchall()
                track_positions = conn.execute(
                    """
                    SELECT order_index
                    FROM job_inputs
                    WHERE job_id=? AND role='TRACK'
                    ORDER BY order_index ASC
                    """,
                    (job_id,),
                ).fetchall()
            finally:
                conn.close()

            assert job is not None
            counts = {str(r["role"]): int(r["c"]) for r in role_counts}
            self.assertTrue(result.enqueued)
            self.assertEqual(result.reason, "enqueued")
            self.assertEqual(job["state"], "READY_FOR_RENDER")
            self.assertEqual(counts.get("TRACK", 0), 3)
            self.assertEqual(counts.get("BACKGROUND", 0), 1)
            self.assertEqual(counts.get("COVER", 0), 1)
            self.assertEqual([int(r["order_index"]) for r in track_positions], [0, 1, 2])


if __name__ == "__main__":
    unittest.main()
