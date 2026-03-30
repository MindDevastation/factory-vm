from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.factory_api.publish_job_actions import execute_publish_job_action
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestTelegramPublishActionParity(unittest.TestCase):
    def _seed_job(self, env, state: str) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            ts = dbm.now_ts()
            rel = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(ch["id"]), f"rel-{state}", "d", "[]", ts),
            )
            job_id = dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=int(rel.lastrowid),
                job_type="UI",
                state="UPLOADED",
                stage="PUBLISH",
                priority=1,
                attempt=0,
                created_at=ts,
                updated_at=ts,
            )
            conn.execute("UPDATE jobs SET publish_state = ?, publish_retry_at = ? WHERE id = ?", (state, (ts + 60 if state == "retry_pending" else None), job_id))
            conn.commit()
            return job_id
        finally:
            conn.close()

    def test_retry_equivalent_between_api_and_telegram_service(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            api_job = self._seed_job(env, "retry_pending")
            tg_job = self._seed_job(env, "retry_pending")

            mod = importlib.import_module("services.factory_api.app")
            client = TestClient(importlib.reload(mod).app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            api_resp = client.post(
                f"/v1/publish/jobs/{api_job}/retry",
                headers=h,
                json={"confirm": True, "reason": "r", "request_id": "parity-api"},
            )
            self.assertEqual(api_resp.status_code, 200)

            conn = dbm.connect(env)
            try:
                tg_result = execute_publish_job_action(
                    conn,
                    job_id=tg_job,
                    action_type="retry",
                    actor="telegram:1",
                    request_id="parity-tg",
                    reason="r",
                    extra_payload={},
                )
            finally:
                conn.close()

            self.assertFalse(tg_result["replayed"])
            self.assertEqual(api_resp.json()["result"]["publish_state_after"], tg_result["result"]["publish_state_after"])


if __name__ == "__main__":
    unittest.main()
