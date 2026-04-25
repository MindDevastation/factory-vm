from __future__ import annotations

import unittest

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import seed_minimal_db, temp_env


class TestVideoLanguageMigrationBackfill(unittest.TestCase):
    def test_migrate_backfills_legacy_language_labels(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="Legacy",
                    description="desc",
                    tags_csv="one,two",
                    video_language="English",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="1",
                )
                conn.execute(
                    "UPDATE releases SET video_language='English' WHERE id=(SELECT release_id FROM jobs WHERE id=?)",
                    (job_id,),
                )
                conn.execute("UPDATE ui_job_drafts SET video_language='English' WHERE job_id=?", (job_id,))
                conn.commit()

                dbm.migrate(conn)
                dbm.migrate(conn)

                release_row = conn.execute(
                    "SELECT video_language FROM releases WHERE id=(SELECT release_id FROM jobs WHERE id=?)",
                    (job_id,),
                ).fetchone()
                draft_row = conn.execute("SELECT video_language FROM ui_job_drafts WHERE job_id=?", (job_id,)).fetchone()
            finally:
                conn.close()

            assert release_row is not None
            assert draft_row is not None
            self.assertEqual(release_row["video_language"], "en")
            self.assertEqual(draft_row["video_language"], "en")


if __name__ == "__main__":
    unittest.main()
