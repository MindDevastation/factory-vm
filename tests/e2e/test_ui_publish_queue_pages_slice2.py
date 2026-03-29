from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPublishQueuePagesSlice2(unittest.TestCase):
    def test_queue_detail_health_pages_render(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch
                ts = dbm.now_ts()
                release_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                        (int(ch["id"]), "Publish UI", "d", "[]", ts),
                    ).lastrowid
                )
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
                conn.execute("UPDATE jobs SET publish_state = 'ready_to_publish' WHERE id = ?", (job_id,))
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            queue_page = client.get("/ui/publish/queue", headers=h)
            self.assertEqual(queue_page.status_code, 200)
            self.assertIn("Publish Queue", queue_page.text)
            self.assertIn("/v1/publish/queue", queue_page.text)

            detail_page = client.get(f"/ui/publish/jobs/{job_id}", headers=h)
            self.assertEqual(detail_page.status_code, 200)
            self.assertIn("Publish Job Detail", detail_page.text)
            self.assertIn(f"Job #{job_id}", detail_page.text)
            self.assertIn("/v1/publish/jobs/' + encodeURIComponent(jobId)", detail_page.text)

            health_page = client.get("/ui/publish/health", headers=h)
            self.assertEqual(health_page.status_code, 200)
            self.assertIn("View: health", health_page.text)


if __name__ == "__main__":
    unittest.main()
