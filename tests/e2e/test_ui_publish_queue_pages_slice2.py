from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPublishQueuePagesSlice2(unittest.TestCase):
    def test_queue_blocked_failed_manual_detail_health_pages_render(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch
                ts = dbm.now_ts()

                def _seed(state: str) -> int:
                    release_id = int(
                        conn.execute(
                            "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                            (int(ch["id"]), f"Publish UI {state}", "d", "[]", ts),
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
                    conn.execute("UPDATE jobs SET publish_state = ? WHERE id = ?", (state, job_id))
                    return int(job_id)

                ready_id = _seed("ready_to_publish")
                _seed("policy_blocked")
                _seed("publish_failed_terminal")
                _seed("manual_handoff_pending")
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            for path, view in (
                ("/ui/publish/queue", "queue"),
                ("/ui/publish/blocked", "blocked"),
                ("/ui/publish/failed", "failed"),
                ("/ui/publish/manual", "manual"),
                ("/ui/publish/health", "health"),
            ):
                page = client.get(path, headers=h)
                self.assertEqual(page.status_code, 200)
                self.assertIn("Publish Queue", page.text)
                self.assertIn(f"View: {view}", page.text)
                self.assertIn("id=\"publish-bulk-controls\"", page.text)
                self.assertIn("id=\"publish-legend\"", page.text)
                self.assertIn("id=\"publish-health-summary\"", page.text)

            detail_page = client.get(f"/ui/publish/jobs/{ready_id}", headers=h)
            self.assertEqual(detail_page.status_code, 200)
            self.assertIn("Publish Job Detail", detail_page.text)
            self.assertIn(f"Job #{ready_id}", detail_page.text)
            self.assertIn("id=\"publish-detail-explain\"", detail_page.text)
            self.assertIn("id=\"publish-detail-actions\"", detail_page.text)
            self.assertIn("mark-completed", detail_page.text)
            self.assertIn("global_state_stage_summary", detail_page.text)
            self.assertIn("effective_decision", detail_page.text)
            self.assertNotIn("data.job ? data.job.state", detail_page.text)


if __name__ == "__main__":
    unittest.main()
