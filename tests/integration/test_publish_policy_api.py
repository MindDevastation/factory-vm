from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestPublishPolicyApi(unittest.TestCase):
    def test_put_policy_endpoints_and_get_resolve(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="UPLOAD")

            from services.common import db as dbm

            conn = dbm.connect(env)
            try:
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r1 = client.put(
                "/v1/publish/policy/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "project default",
                    "request_id": "req-p1",
                    "publish_mode": "manual_only",
                    "target_visibility": "unlisted",
                    "reason_code": "policy_requires_manual",
                },
            )
            self.assertEqual(r1.status_code, 200)

            r2 = client.put(
                "/v1/publish/policy/channels/darkwood-reverie",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "channel block",
                    "request_id": "req-c1",
                    "publish_mode": "hold",
                    "target_visibility": None,
                    "reason_code": "channel_policy_block",
                },
            )
            self.assertEqual(r2.status_code, 200)

            r3 = client.put(
                f"/v1/publish/policy/items/{release_id}",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "item allow",
                    "request_id": "req-i1",
                    "publish_mode": "auto",
                    "target_visibility": "public",
                    "reason_code": "item_override_block",
                },
            )
            self.assertEqual(r3.status_code, 200)

            resolved = client.get(
                "/v1/publish/policy/resolve",
                headers=h,
                params={"job_id": job_id, "release_id": release_id, "channel_slug": "darkwood-reverie"},
            )
            self.assertEqual(resolved.status_code, 200)
            body = resolved.json()
            self.assertEqual(body["resolved_scope"], "item")
            self.assertEqual(body["effective_publish_mode"], "auto")
            self.assertEqual(body["effective_target_visibility"], "public")
            self.assertEqual(body["effective_reason_code"], "item_override_block")
            self.assertEqual(body["decision"], "auto")
            self.assertEqual(body["effective_audit_status"], "unknown")
            self.assertIn("effective_audit_status", body)
            self.assertIn("job_publish_hold_active", body)

    def test_policy_validation_contracts(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            bad_mode = client.put(
                "/v1/publish/policy/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "x",
                    "request_id": "req-bad-1",
                    "publish_mode": "AUTO",
                    "target_visibility": "public",
                    "reason_code": "policy_requires_manual",
                },
            )
            self.assertEqual(bad_mode.status_code, 422)
            self.assertEqual(bad_mode.json()["error"]["code"], "PPP_INVALID_PUBLISH_MODE")

            bad_visibility = client.put(
                "/v1/publish/policy/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "x",
                    "request_id": "req-bad-2",
                    "publish_mode": "auto",
                    "target_visibility": "private",
                    "reason_code": "policy_requires_manual",
                },
            )
            self.assertEqual(bad_visibility.status_code, 422)
            self.assertEqual(bad_visibility.json()["error"]["code"], "PPP_INVALID_TARGET_VISIBILITY")

            bad_reason = client.put(
                "/v1/publish/policy/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "x",
                    "request_id": "req-bad-3",
                    "publish_mode": "auto",
                    "target_visibility": "public",
                    "reason_code": "",
                },
            )
            self.assertEqual(bad_reason.status_code, 422)
            self.assertEqual(bad_reason.json()["error"]["code"], "PPP_INVALID_REASON_CODE")


if __name__ == "__main__":
    unittest.main()
