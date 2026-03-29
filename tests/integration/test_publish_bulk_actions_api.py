from __future__ import annotations

import importlib
import unittest
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPublishBulkActionsApi(unittest.TestCase):
    def _seed_job(self, env, *, publish_state: str, state: str = "UPLOADED", hold_active: bool = False) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            ts = dbm.now_ts()
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(ch["id"]), f"release-{publish_state}", "desc", "[]", ts),
            )
            release_id = int(cur.lastrowid)
            job_id = dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=release_id,
                job_type="UI",
                state=state,
                stage="PUBLISH",
                priority=1,
                attempt=0,
                created_at=ts,
                updated_at=ts,
            )
            conn.execute(
                "UPDATE jobs SET publish_state = ?, publish_retry_at = ?, publish_hold_active = ?, publish_hold_reason_code = ? WHERE id = ?",
                (
                    publish_state,
                    (ts + 60 if publish_state == "retry_pending" else None),
                    1 if hold_active else 0,
                    ("operator_forced_manual" if hold_active else None),
                    job_id,
                ),
            )
            conn.commit()
            return job_id
        finally:
            conn.close()

    def _client(self, env) -> tuple[TestClient, dict[str, str]]:
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)
        client = TestClient(mod.app)
        return client, basic_auth_header(env.basic_user, env.basic_pass)

    def test_preview_and_execute_happy_path_retry(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            j1 = self._seed_job(env, publish_state="retry_pending")
            client, h = self._client(env)

            preview = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "retry", "selected_job_ids": [j1]})
            self.assertEqual(preview.status_code, 200, preview.text)
            body = preview.json()
            self.assertEqual(body["selected_count"], 1)
            self.assertEqual(body["rejected_count"], 0)

            execute = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": body["preview_session_id"], "selection_fingerprint": body["selection_fingerprint"]},
            )
            self.assertEqual(execute.status_code, 200, execute.text)
            self.assertEqual(execute.json()["summary"]["succeeded_count"], 1)

    def test_execute_expired_session(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            j1 = self._seed_job(env, publish_state="retry_pending")
            client, h = self._client(env)
            preview = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "retry", "selected_job_ids": [j1]})
            sid = preview.json()["preview_session_id"]
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE publish_bulk_action_sessions SET expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?", (sid,))
                conn.commit()
            finally:
                conn.close()

            execute = client.post("/v1/publish/bulk/execute", headers=h, json={"preview_session_id": sid})
            self.assertEqual(execute.status_code, 409)
            self.assertEqual(execute.json()["error"]["code"], "PBA_SESSION_EXPIRED")
            self.assertEqual(execute.json()["error"]["details"]["invalidation"]["kind"], "expired")

    def test_execute_scope_mismatch_and_outside_snapshot(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            j1 = self._seed_job(env, publish_state="retry_pending")
            j2 = self._seed_job(env, publish_state="retry_pending")
            client, h = self._client(env)
            preview = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "retry", "selected_job_ids": [j1]})
            b = preview.json()

            mismatch = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": b["preview_session_id"], "selection_fingerprint": "bad"},
            )
            self.assertEqual(mismatch.status_code, 409)
            self.assertEqual(mismatch.json()["error"]["code"], "PBA_SCOPE_MISMATCH")

            preview2 = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "retry", "selected_job_ids": [j1]})
            outside = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": preview2.json()["preview_session_id"], "selected_job_ids": [j1, j2]},
            )
            self.assertEqual(outside.status_code, 409)
            self.assertEqual(outside.json()["error"]["code"], "PBA_EXECUTE_OUTSIDE_SNAPSHOT")

    def test_mixed_allowed_forbidden_and_hold(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            allowed = self._seed_job(env, publish_state="policy_blocked")
            forbidden = self._seed_job(env, publish_state="manual_handoff_pending")
            hold_ok = self._seed_job(env, publish_state="ready_to_publish", hold_active=False)
            hold_bad = self._seed_job(env, publish_state="ready_to_publish", hold_active=True)
            client, h = self._client(env)

            preview = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "unblock", "selected_job_ids": [allowed, forbidden]})
            self.assertEqual(preview.status_code, 200)
            self.assertEqual(preview.json()["selected_count"], 1)
            self.assertEqual(preview.json()["rejected_count"], 1)

            execute = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": preview.json()["preview_session_id"], "selection_fingerprint": preview.json()["selection_fingerprint"]},
            )
            self.assertEqual(execute.status_code, 200)
            self.assertEqual(execute.json()["summary"]["succeeded_count"], 1)
            self.assertEqual(execute.json()["summary"]["skipped_count"], 0)

            hold_preview = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "hold", "selected_job_ids": [hold_ok, hold_bad]})
            self.assertEqual(hold_preview.status_code, 200)
            self.assertEqual(hold_preview.json()["selected_count"], 1)
            self.assertEqual(hold_preview.json()["rejected_count"], 1)

            hold_execute = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": hold_preview.json()["preview_session_id"], "selection_fingerprint": hold_preview.json()["selection_fingerprint"]},
            )
            self.assertEqual(hold_execute.status_code, 200)
            items = hold_execute.json()["items"]
            self.assertEqual(items[0]["result_kind"], "SUCCESS_UPDATED")
            self.assertEqual(items[0]["hold_after"], True)

    def test_reschedule_requires_explicit_future_and_executes_previewed_target(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="ready_to_publish")
            client, h = self._client(env)

            missing = client.post("/v1/publish/bulk/preview", headers=h, json={"action": "reschedule", "selected_job_ids": [job_id]})
            self.assertEqual(missing.status_code, 422)
            self.assertEqual(missing.json()["error"]["code"], "PJA_INVALID_DATETIME")

            target = (datetime.now(timezone.utc) + timedelta(days=3)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            preview = client.post(
                "/v1/publish/bulk/preview",
                headers=h,
                json={"action": "reschedule", "selected_job_ids": [job_id], "scheduled_at": target},
            )
            self.assertEqual(preview.status_code, 200, preview.text)
            body = preview.json()
            self.assertEqual(body["action_payload"]["scheduled_at"], target)
            self.assertEqual(body["items"][0]["scheduled_at"], target)

            execute = client.post(
                "/v1/publish/bulk/execute",
                headers=h,
                json={"preview_session_id": body["preview_session_id"], "selection_fingerprint": body["selection_fingerprint"]},
            )
            self.assertEqual(execute.status_code, 200, execute.text)
            self.assertEqual(execute.json()["items"][0]["scheduled_at"], target)

            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT publish_scheduled_at FROM jobs WHERE id = ?", (job_id,)).fetchone()
                self.assertIsNotNone(row)
                self.assertAlmostEqual(float(row["publish_scheduled_at"]), datetime.fromisoformat(target.replace("Z", "+00:00")).timestamp(), places=3)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
