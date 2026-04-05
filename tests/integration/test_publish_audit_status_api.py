from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPublishAuditStatusApi(unittest.TestCase):
    def test_put_endpoints_write_current_and_history_with_actor_and_get_contracts(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            put_default = client.put(
                "/v1/publish/audit-status/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "baseline policy",
                    "request_id": "req-default-1",
                    "status": "pending",
                },
            )
            self.assertEqual(put_default.status_code, 200)
            body_default = put_default.json()
            self.assertEqual(body_default["scope_type"], "project_default")
            self.assertEqual(body_default["status"], "pending")
            self.assertEqual(body_default["actor_identity"], env.basic_user)

            put_override = client.put(
                "/v1/publish/audit-status/channels/darkwood-reverie",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "manual channel control",
                    "request_id": "req-override-1",
                    "status": "manual-only",
                },
            )
            self.assertEqual(put_override.status_code, 200)
            body_override = put_override.json()
            self.assertEqual(body_override["scope_type"], "channel_override")
            self.assertEqual(body_override["channel_slug"], "darkwood-reverie")
            self.assertEqual(body_override["status"], "manual-only")
            self.assertEqual(body_override["actor_identity"], env.basic_user)

            get_effective = client.get("/v1/publish/audit-status/effective", headers=h, params={"channel_slug": "darkwood-reverie"})
            self.assertEqual(get_effective.status_code, 200)
            effective = get_effective.json()
            self.assertEqual(
                set(effective.keys()),
                {"channel_slug", "project_default_status", "channel_override_status", "effective_status"},
            )
            self.assertEqual(effective["channel_slug"], "darkwood-reverie")
            self.assertEqual(effective["project_default_status"], "pending")
            self.assertEqual(effective["channel_override_status"], "manual-only")
            self.assertEqual(effective["effective_status"], "manual-only")

            get_history = client.get("/v1/publish/audit-status/history", headers=h)
            self.assertEqual(get_history.status_code, 200)
            history_body = get_history.json()
            self.assertEqual(set(history_body.keys()), {"items", "limit"})
            self.assertEqual(history_body["limit"], 50)
            self.assertGreaterEqual(len(history_body["items"]), 2)
            first = history_body["items"][0]
            self.assertEqual(
                set(first.keys()),
                {
                    "id",
                    "scope_type",
                    "channel_slug",
                    "previous_status",
                    "status",
                    "reason",
                    "request_id",
                    "actor_identity",
                    "created_at",
                },
            )

            default_rows = [r for r in history_body["items"] if r["scope_type"] == "project_default" and r["request_id"] == "req-default-1"]
            channel_rows = [
                r
                for r in history_body["items"]
                if r["scope_type"] == "channel_override" and r["request_id"] == "req-override-1" and r["channel_slug"] == "darkwood-reverie"
            ]
            self.assertEqual(len(default_rows), 1)
            self.assertEqual(len(channel_rows), 1)
            self.assertEqual(default_rows[0]["actor_identity"], env.basic_user)
            self.assertEqual(channel_rows[0]["actor_identity"], env.basic_user)

            # append-only history check (second mutation appends, does not overwrite)
            put_default_2 = client.put(
                "/v1/publish/audit-status/project-default",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "tighten policy",
                    "request_id": "req-default-2",
                    "status": "approved",
                },
            )
            self.assertEqual(put_default_2.status_code, 200)

            history_after = client.get("/v1/publish/audit-status/history", headers=h).json()
            default_rows_after = [r for r in history_after["items"] if r["scope_type"] == "project_default"]
            request_ids = {row["request_id"] for row in default_rows_after}
            self.assertIn("req-default-1", request_ids)
            self.assertIn("req-default-2", request_ids)

    def test_effective_fallback_unknown_when_no_default(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/publish/audit-status/effective", headers=h, params={"channel_slug": "darkwood-reverie"})
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["project_default_status"], "unknown")
            self.assertIsNone(body["channel_override_status"])
            self.assertEqual(body["effective_status"], "unknown")

    def test_mutation_envelope_and_status_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            bad_confirm = client.put(
                "/v1/publish/audit-status/project-default",
                headers=h,
                json={"confirm": False, "reason": "x", "request_id": "req-1", "status": "approved"},
            )
            self.assertEqual(bad_confirm.status_code, 422)
            self.assertEqual(bad_confirm.json()["error"]["code"], "PAS_CONFIRM_REQUIRED")

            bad_reason = client.put(
                "/v1/publish/audit-status/project-default",
                headers=h,
                json={"confirm": True, "reason": "", "request_id": "req-2", "status": "approved"},
            )
            self.assertEqual(bad_reason.status_code, 422)
            self.assertEqual(bad_reason.json()["error"]["code"], "PAS_REASON_REQUIRED")

            bad_request_id = client.put(
                "/v1/publish/audit-status/project-default",
                headers=h,
                json={"confirm": True, "reason": "x", "request_id": "", "status": "approved"},
            )
            self.assertEqual(bad_request_id.status_code, 422)
            self.assertEqual(bad_request_id.json()["error"]["code"], "PAS_REQUEST_ID_REQUIRED")

            bad_status = client.put(
                "/v1/publish/audit-status/channels/darkwood-reverie",
                headers=h,
                json={"confirm": True, "reason": "x", "request_id": "req-3", "status": "APPROVED"},
            )
            self.assertEqual(bad_status.status_code, 422)
            self.assertEqual(bad_status.json()["error"]["code"], "PAS_INVALID_STATUS")


if __name__ == "__main__":
    unittest.main()
