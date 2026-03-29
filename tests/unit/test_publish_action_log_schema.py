from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestPublishActionLogSchema(unittest.TestCase):
    def test_migrate_creates_publish_action_log_with_unique_key(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='publish_action_log'"
                ).fetchone()
                self.assertIsNotNone(table)

                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                self.assertIsNotNone(ch)
                ts = dbm.now_ts()
                release_id = int(conn.execute(
                    "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                    (int(ch["id"]), "release", "desc", "[]", ts),
                ).lastrowid)
                job_id = dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=release_id,
                    job_type="UI",
                    state="UPLOADED",
                    stage="PUBLISH",
                    priority=1,
                    attempt=0,
                    created_at=ts,
                    updated_at=ts,
                )

                conn.execute(
                    """
                    INSERT INTO publish_action_log(action_type, request_id, job_id, actor_identity, reason, response_json, created_at)
                    VALUES('retry','req-1',?,'admin','x','{}','2026-01-01T00:00:00Z')
                    """,
                    (job_id,),
                )
                with self.assertRaises(Exception):
                    conn.execute(
                        """
                        INSERT INTO publish_action_log(action_type, request_id, job_id, actor_identity, reason, response_json, created_at)
                        VALUES('retry','req-1',?,'admin','x','{}','2026-01-01T00:00:00Z')
                        """,
                        (job_id,),
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
