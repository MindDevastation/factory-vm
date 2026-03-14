from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class OpsRecoveryReadonlyApiTests(unittest.TestCase):
    def test_recovery_listing_filters_and_detail_shape(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            failed_job = insert_release_and_job(env, state="FAILED", stage="RENDER", channel_slug="darkwood-reverie")
            published_job = insert_release_and_job(env, state="PUBLISHED", stage="APPROVAL", channel_slug="channel-b")

            conn = dbm.connect(env)
            try:
                ts = dbm.now_ts()
                conn.execute(
                    "UPDATE jobs SET delete_mp4_at = ?, progress_updated_at = ?, error_reason = ? WHERE id = ?",
                    (ts - 3.0, ts - 600.0, "render crash", published_job),
                )
                conn.execute(
                    "UPDATE jobs SET progress_updated_at = ?, error_reason = ? WHERE id = ?",
                    (ts - 10.0, "ffmpeg failed", failed_job),
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/ops/recovery/jobs?category=failed&actionability=has_actions&q=ffmpeg", headers=h)
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertIn("summary", payload)
            self.assertEqual(payload["total"], 1)
            item = payload["items"][0]
            self.assertEqual(item["job_id"], failed_job)
            self.assertIn("available_actions", item)
            self.assertIn("category_reasons", item)

            detail = client.get(f"/v1/ops/recovery/jobs/{published_job}", headers=h)
            self.assertEqual(detail.status_code, 200)
            detail_item = detail.json()["item"]
            self.assertIn("cleanup", detail_item)
            self.assertIn("artifacts", detail_item)
            self.assertIn("recent_audit_entries", detail_item)
            self.assertEqual(detail_item["allowed_stage_tokens"], [])
            self.assertIn("allowed_stage_tokens_fallback", detail_item)

    def test_recovery_audit_migration_columns_exist(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                cols = conn.execute("PRAGMA table_info(recovery_action_audit)").fetchall()
            finally:
                conn.close()
            names = {str(col["name"]) for col in cols}
            self.assertIn("action", names)
            self.assertIn("phase", names)
            self.assertIn("result_payload_json", names)
            self.assertIn("created_at", names)


if __name__ == "__main__":
    unittest.main()
