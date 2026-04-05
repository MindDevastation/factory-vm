from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestPublishPolicyBaselineRegression(unittest.TestCase):
    def test_empty_policy_audit_global_tables_keep_manual_baseline(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="UPLOAD")

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/publish/policy/resolve", headers=h, params={"job_id": job_id})
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["effective_publish_mode"], "manual_only")
            self.assertEqual(body["decision"], "hold")
            self.assertEqual(body["effective_reason_code"], "audit_not_approved")
            self.assertEqual(body["effective_audit_status"], "unknown")
            self.assertFalse(body["global_auto_publish_paused"])
            self.assertFalse(body["job_publish_hold_active"])


if __name__ == "__main__":
    unittest.main()
