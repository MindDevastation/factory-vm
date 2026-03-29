from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPublishOperatorActionsSlice2(unittest.TestCase):
    def _seed_job(self, env, *, publish_state: str) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            ts = dbm.now_ts()
            release_id = int(
                conn.execute(
                    "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                    (int(ch["id"]), f"Seed {publish_state}", "d", "[]", ts),
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
            conn.execute("UPDATE jobs SET publish_state = ? WHERE id = ?", (publish_state, job_id))
            conn.commit()
            return int(job_id)
        finally:
            conn.close()

    def test_single_item_action_paths_are_present_and_triggerable(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            failed_job_id = self._seed_job(env, publish_state="publish_failed_terminal")

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            queue_page = client.get("/ui/publish/queue", headers=h)
            self.assertEqual(queue_page.status_code, 200)
            self.assertIn("/v1/publish/jobs/' + encodeURIComponent(jobId) + '/' + endpoint", queue_page.text)
            self.assertIn("data-endpoint=\"' + esc(action.endpoint) + '\"", queue_page.text)

            detail_page = client.get(f"/ui/publish/jobs/{failed_job_id}", headers=h)
            self.assertEqual(detail_page.status_code, 200)
            self.assertIn("data-endpoint=\"reset-failure\"", detail_page.text)
            self.assertIn("data-endpoint=\"retry\"", detail_page.text)

            action = client.post(
                f"/v1/publish/jobs/{failed_job_id}/reset-failure",
                headers=h,
                json={"confirm": True, "reason": "ui_test", "request_id": f"slice2-reset-{failed_job_id}"},
            )
            self.assertEqual(action.status_code, 200, action.text)
            self.assertEqual(action.json()["result"]["publish_state_after"], "retry_pending")


if __name__ == "__main__":
    unittest.main()
