from __future__ import annotations

import importlib
import unittest
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPublishJobActionsApi(unittest.TestCase):
    def _seed_job(self, env, *, publish_state: str, state: str = "UPLOADED") -> int:
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
                "UPDATE jobs SET publish_state = ?, publish_scheduled_at = ?, publish_retry_at = ?, publish_last_error_code = ?, publish_last_error_message = ? WHERE id = ?",
                (
                    publish_state,
                    ts + 3600,
                    (ts + 120 if publish_state == "retry_pending" else None),
                    ("ERR" if publish_state == "publish_failed_terminal" else None),
                    ("failed" if publish_state == "publish_failed_terminal" else None),
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

    def test_idempotent_retry_replays_original_result(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="retry_pending")
            client, h = self._client(env)
            body = {"confirm": True, "reason": "retry now", "request_id": "req-retry-1"}

            first = client.post(f"/v1/publish/jobs/{job_id}/retry", headers=h, json=body)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(first.json()["replayed"], False)
            self.assertEqual(first.json()["result"]["publish_state_after"], "ready_to_publish")

            second = client.post(f"/v1/publish/jobs/{job_id}/retry", headers=h, json=body)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(second.json()["replayed"], True)
            self.assertEqual(second.json()["result"], first.json()["result"])

    def test_move_to_manual_forbidden_from_publish_in_progress(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="publish_in_progress")
            client, h = self._client(env)

            resp = client.post(
                f"/v1/publish/jobs/{job_id}/move-to-manual",
                headers=h,
                json={"confirm": True, "reason": "operator handoff", "request_id": "req-mtm-1"},
            )
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PJA_ACTION_FORBIDDEN_STATE")

    def test_manual_acknowledge_and_mark_completed_flow(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="manual_handoff_pending")
            client, h = self._client(env)

            ack = client.post(
                f"/v1/publish/jobs/{job_id}/acknowledge",
                headers=h,
                json={"confirm": True, "reason": "I will handle", "request_id": "req-ack-1"},
            )
            self.assertEqual(ack.status_code, 200)
            self.assertEqual(ack.json()["result"]["publish_state_after"], "manual_handoff_acknowledged")

            completed = client.post(
                f"/v1/publish/jobs/{job_id}/mark-completed",
                headers=h,
                json={
                    "confirm": True,
                    "reason": "published manually",
                    "request_id": "req-done-1",
                    "actual_published_at": "2026-03-29T00:00:00Z",
                    "video_id": "yt-123",
                },
            )
            self.assertEqual(completed.status_code, 200)
            self.assertEqual(completed.json()["result"]["publish_state_after"], "manual_publish_completed")

    def test_mark_completed_validation_guards(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="manual_handoff_acknowledged")
            client, h = self._client(env)

            missing_dt = client.post(
                f"/v1/publish/jobs/{job_id}/mark-completed",
                headers=h,
                json={"confirm": True, "reason": "x", "request_id": "req-mc-1", "video_id": "yt-1"},
            )
            self.assertEqual(missing_dt.status_code, 422)

            missing_media = client.post(
                f"/v1/publish/jobs/{job_id}/mark-completed",
                headers=h,
                json={"confirm": True, "reason": "x", "request_id": "req-mc-2", "actual_published_at": "2026-03-29T00:00:00Z"},
            )
            self.assertEqual(missing_media.status_code, 422)
            self.assertEqual(missing_media.json()["error"]["code"], "PJA_MARK_COMPLETED_MEDIA_REQUIRED")

    def test_reschedule_validation_and_allowed_state(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="ready_to_publish")
            client, h = self._client(env)

            invalid = client.post(
                f"/v1/publish/jobs/{job_id}/reschedule",
                headers=h,
                json={"confirm": True, "reason": "delay", "request_id": "req-rs-1", "scheduled_at": "not-a-date"},
            )
            self.assertEqual(invalid.status_code, 422)

            future_ts = (datetime.now(timezone.utc) + timedelta(days=2)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            ok = client.post(
                f"/v1/publish/jobs/{job_id}/reschedule",
                headers=h,
                json={"confirm": True, "reason": "delay", "request_id": "req-rs-2", "scheduled_at": future_ts},
            )
            self.assertEqual(ok.status_code, 200)
            self.assertEqual(ok.json()["result"]["publish_state_after"], "waiting_for_schedule")

            forbidden_state_job_id = self._seed_job(env, publish_state="manual_handoff_pending")
            forbidden = client.post(
                f"/v1/publish/jobs/{forbidden_state_job_id}/reschedule",
                headers=h,
                json={"confirm": True, "reason": "delay", "request_id": "req-rs-3", "scheduled_at": future_ts},
            )
            self.assertEqual(forbidden.status_code, 409)

    def test_cancel_blocks_future_publish_actions(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_job(env, publish_state="retry_pending")
            client, h = self._client(env)

            cancelled = client.post(
                f"/v1/publish/jobs/{job_id}/cancel",
                headers=h,
                json={"confirm": True, "reason": "abandon", "request_id": "req-cancel-1"},
            )
            self.assertEqual(cancelled.status_code, 200)
            self.assertEqual(cancelled.json()["result"]["state_after"], "CANCELLED")

            retry_after_cancel = client.post(
                f"/v1/publish/jobs/{job_id}/retry",
                headers=h,
                json={"confirm": True, "reason": "retry", "request_id": "req-cancel-2"},
            )
            self.assertEqual(retry_after_cancel.status_code, 409)
            self.assertEqual(retry_after_cancel.json()["error"]["code"], "PJA_JOB_CANCELLED")

    def test_action_state_matrix_samples(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            client, h = self._client(env)

            jobs = {
                "reset_ok": self._seed_job(env, publish_state="publish_failed_terminal"),
                "reset_bad": self._seed_job(env, publish_state="ready_to_publish"),
                "unblock_ok": self._seed_job(env, publish_state="policy_blocked"),
                "unblock_bad": self._seed_job(env, publish_state="manual_handoff_pending"),
            }

            reset_ok = client.post(
                f"/v1/publish/jobs/{jobs['reset_ok']}/reset-failure",
                headers=h,
                json={"confirm": True, "reason": "reset", "request_id": "req-rf-1"},
            )
            self.assertEqual(reset_ok.status_code, 200)

            reset_bad = client.post(
                f"/v1/publish/jobs/{jobs['reset_bad']}/reset-failure",
                headers=h,
                json={"confirm": True, "reason": "reset", "request_id": "req-rf-2"},
            )
            self.assertEqual(reset_bad.status_code, 409)

            unblock_ok = client.post(
                f"/v1/publish/jobs/{jobs['unblock_ok']}/unblock",
                headers=h,
                json={"confirm": True, "reason": "clear block", "request_id": "req-ub-1"},
            )
            self.assertEqual(unblock_ok.status_code, 200)

            unblock_bad = client.post(
                f"/v1/publish/jobs/{jobs['unblock_bad']}/unblock",
                headers=h,
                json={"confirm": True, "reason": "clear block", "request_id": "req-ub-2"},
            )
            self.assertEqual(unblock_bad.status_code, 409)


if __name__ == "__main__":
    unittest.main()
