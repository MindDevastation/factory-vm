from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.bot.handlers import run_telegram_bulk_action
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestTelegramPublishBulkParity(unittest.TestCase):
    def _seed_retry_pending(self, env) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            ts = dbm.now_ts()
            rel = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(ch["id"]), "bulk", "d", "[]", ts),
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
            conn.execute("UPDATE jobs SET publish_state = 'retry_pending', publish_retry_at = ? WHERE id = ?", (ts + 60, job_id))
            conn.commit()
            return job_id
        finally:
            conn.close()

    def test_bulk_retry_preview_execute_equivalent(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            api_job = self._seed_retry_pending(env)
            tg_job = self._seed_retry_pending(env)

            mod = importlib.import_module("services.factory_api.app")
            client = TestClient(importlib.reload(mod).app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            api_preview = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "retry", "selected_job_ids": [api_job]})
            self.assertEqual(api_preview.status_code, 200)
            api_execute = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={
                    "preview_session_id": api_preview.json()["preview_session_id"],
                    "selection_fingerprint": api_preview.json()["selection_fingerprint"],
                },
            )
            self.assertEqual(api_execute.status_code, 200)

            tg_result = run_telegram_bulk_action(
                env=env,
                action="retry",
                selected_job_ids=[tg_job],
                actor="telegram:1",
            )

            self.assertEqual(api_execute.json()["summary"]["succeeded_count"], tg_result["execute"]["summary"]["succeeded_count"])


if __name__ == "__main__":
    unittest.main()
