from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestPublishPolicyDecisionResolution(unittest.TestCase):
    def test_resolve_combines_policy_audit_global_pause_and_job_hold(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="UPLOAD")

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            put_policy = client.put(
                "/v1/publish/policy/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "policy baseline",
                    "request_id": "req-pol-1",
                    "publish_mode": "auto",
                    "target_visibility": "public",
                    "reason_code": "policy_requires_manual",
                },
            )
            self.assertEqual(put_policy.status_code, 200)

            put_audit = client.put(
                "/v1/publish/audit-status/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "approved",
                    "request_id": "req-aud-1",
                    "status": "approved",
                },
            )
            self.assertEqual(put_audit.status_code, 200)

            resp = client.get("/v1/publish/policy/resolve", headers=h, params={"job_id": job_id})
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["effective_publish_mode"], "auto")
            self.assertEqual(body["effective_audit_status"], "approved")
            self.assertFalse(body["global_auto_publish_paused"])
            self.assertFalse(body["job_publish_hold_active"])
            self.assertEqual(body["decision"], "auto")

            put_controls = client.put(
                "/v1/publish/controls",
                headers=h,
                json={"auto_publish_paused": True, "reason": "incident"},
            )
            self.assertEqual(put_controls.status_code, 200)

            resp_paused = client.get("/v1/publish/policy/resolve", headers=h, params={"job_id": job_id})
            self.assertEqual(resp_paused.status_code, 200)
            paused = resp_paused.json()
            self.assertEqual(paused["decision"], "hold")
            self.assertEqual(paused["effective_reason_code"], "global_pause_active")

    def test_resolve_rejects_invalid_job_hold_without_reason_code(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="UPLOAD")

            from services.common import db as dbm

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET publish_hold_active = 1, publish_hold_reason_code = NULL WHERE id = ?", (job_id,))
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/publish/policy/resolve", headers=h, params={"job_id": job_id})
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "PPP_INVALID_JOB_HOLD")


if __name__ == "__main__":
    unittest.main()
