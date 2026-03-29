from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPublishRegressionSmokeSlice2(unittest.TestCase):
    def _seed_retry_pending_job(self, env) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            ts = dbm.now_ts()
            release_id = int(
                conn.execute(
                    "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                    (int(ch["id"]), "Bulk Seed", "d", "[]", ts),
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
            conn.execute("UPDATE jobs SET publish_state = ?, publish_retry_at = ? WHERE id = ?", ("retry_pending", ts + 60, job_id))
            conn.commit()
            return int(job_id)
        finally:
            conn.close()

    def test_bulk_preview_execute_and_non_epic3_pages_render(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            retry_job = self._seed_retry_pending_job(env)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            queue_page = client.get("/ui/publish/queue", headers=h)
            self.assertEqual(queue_page.status_code, 200)
            self.assertIn("/v1/publish/bulk/preview", queue_page.text)
            self.assertIn("/v1/publish/bulk/execute", queue_page.text)

            preview = client.post(
                "/v1/publish/bulk/preview",
                headers=h,
                json={"action": "retry", "selected_job_ids": [retry_job]},
            )
            self.assertEqual(preview.status_code, 200, preview.text)
            body = preview.json()
            self.assertEqual(body["selected_count"], 1)

            execute = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": body["preview_session_id"], "selection_fingerprint": body["selection_fingerprint"]},
            )
            self.assertEqual(execute.status_code, 200, execute.text)
            self.assertEqual(execute.json()["summary"]["succeeded_count"], 1)

            # Non-Epic-3 regression smoke
            home = client.get("/", headers=h)
            self.assertEqual(home.status_code, 200)
            planner = client.get("/ui/planner", headers=h)
            self.assertEqual(planner.status_code, 200)
            tags = client.get("/ui/tags", headers=h)
            self.assertEqual(tags.status_code, 200)


if __name__ == "__main__":
    unittest.main()
