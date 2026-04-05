from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.publish_runtime.events import append_publish_lifecycle_event
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPublishQueueReadApi(unittest.TestCase):
    def _seed_publish_job(
        self,
        env,
        *,
        channel_slug: str,
        publish_state: str,
        scheduled_at: float | None,
        hold_active: bool = False,
    ) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, channel_slug)
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
                state="UPLOADED",
                stage="PUBLISH",
                priority=1,
                attempt=0,
                created_at=ts,
                updated_at=ts,
            )
            conn.execute(
                """
                UPDATE jobs
                SET publish_state = ?,
                    publish_scheduled_at = ?,
                    publish_attempt_count = 2,
                    publish_last_error_code = ?,
                    publish_last_error_message = ?,
                    publish_hold_active = ?,
                    publish_hold_reason_code = ?,
                    publish_delivery_mode_effective = ?,
                    publish_resolved_scope = ?,
                    publish_reason_code = ?,
                    publish_manual_ack_at = ?,
                    publish_manual_video_id = ?,
                    publish_manual_url = ?,
                    publish_drift_detected_at = ?,
                    publish_observed_visibility = ?
                WHERE id = ?
                """,
                (
                    publish_state,
                    scheduled_at,
                    "ERR_X" if publish_state == "publish_failed_terminal" else None,
                    "failure" if publish_state == "publish_failed_terminal" else None,
                    1 if hold_active else 0,
                    "global_pause_active" if hold_active else None,
                    "manual" if publish_state.startswith("manual_") else "automatic",
                    "channel",
                    "policy_requires_manual" if publish_state.startswith("manual_") else None,
                    ts,
                    "vid-1" if publish_state.startswith("manual_") else None,
                    "https://example.test/v/1" if publish_state.startswith("manual_") else None,
                    ts if publish_state == "publish_state_drift_detected" else None,
                    "private" if publish_state == "publish_state_drift_detected" else None,
                    job_id,
                ),
            )
            conn.commit()
            return job_id
        finally:
            conn.close()

    def test_queue_filters_and_views(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            base_ts = 1_800_000_000.0
            self._seed_publish_job(env, channel_slug="darkwood-reverie", publish_state="ready_to_publish", scheduled_at=base_ts)
            self._seed_publish_job(env, channel_slug="channel-b", publish_state="policy_blocked", scheduled_at=base_ts + 10)
            self._seed_publish_job(env, channel_slug="channel-c", publish_state="publish_failed_terminal", scheduled_at=base_ts + 20)
            self._seed_publish_job(env, channel_slug="channel-d", publish_state="manual_handoff_pending", scheduled_at=base_ts + 30)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            queue = client.get("/v1/publish/queue", headers=h)
            self.assertEqual(queue.status_code, 200)
            queue_body = queue.json()
            self.assertEqual(queue_body["view"], "queue")
            self.assertEqual(len(queue_body["items"]), 1)
            self.assertEqual(queue_body["items"][0]["publish_state"], "ready_to_publish")

            filtered = client.get("/v1/publish/queue", headers=h, params={"channel_slug": "darkwood-reverie"})
            self.assertEqual(filtered.status_code, 200)
            self.assertEqual(len(filtered.json()["items"]), 1)

            by_state = client.get("/v1/publish/queue", headers=h, params={"publish_state": "policy_blocked", "view": "health"})
            self.assertEqual(by_state.status_code, 200)
            self.assertEqual(len(by_state.json()["items"]), 1)

            blocked = client.get("/v1/publish/queue", headers=h, params={"view": "blocked"})
            self.assertEqual(blocked.status_code, 200)
            self.assertEqual([item["publish_state"] for item in blocked.json()["items"]], ["policy_blocked"])

            failed = client.get("/v1/publish/queue", headers=h, params={"view": "failed"})
            self.assertEqual(failed.status_code, 200)
            self.assertEqual([item["publish_state"] for item in failed.json()["items"]], ["publish_failed_terminal"])

            manual = client.get("/v1/publish/queue", headers=h, params={"view": "manual"})
            self.assertEqual(manual.status_code, 200)
            self.assertEqual([item["publish_state"] for item in manual.json()["items"]], ["manual_handoff_pending"])

            health = client.get("/v1/publish/queue", headers=h, params={"view": "health"})
            self.assertEqual(health.status_code, 200)
            self.assertIn("health", health.json())

            before = client.get("/v1/publish/queue", headers=h, params={"view": "health", "scheduled_before": str(base_ts + 15)})
            self.assertEqual(before.status_code, 200)
            self.assertEqual(len(before.json()["items"]), 2)

            after = client.get("/v1/publish/queue", headers=h, params={"view": "health", "scheduled_after": str(base_ts + 15)})
            self.assertEqual(after.status_code, 200)
            self.assertEqual(len(after.json()["items"]), 2)

    def test_job_detail_shape_and_audit_path(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            ts = 1_800_000_010.0
            job_id = self._seed_publish_job(
                env,
                channel_slug="darkwood-reverie",
                publish_state="manual_handoff_pending",
                scheduled_at=ts,
                hold_active=True,
            )
            other_job_id = self._seed_publish_job(
                env,
                channel_slug="channel-b",
                publish_state="ready_to_publish",
                scheduled_at=ts + 1,
            )

            append_publish_lifecycle_event(
                storage_root=env.storage_root,
                event={
                    "event_name": "publish.transition",
                    "job_id": job_id,
                    "occurred_at": "2026-03-29T01:00:00Z",
                    "publish_state_before": "ready_to_publish",
                    "publish_state_after": "manual_handoff_pending",
                    "changed_fields": ["publish_state"],
                    "actor": "system_automatic",
                },
            )
            append_publish_lifecycle_event(
                storage_root=env.storage_root,
                event={
                    "event_name": "publish.transition",
                    "job_id": other_job_id,
                    "occurred_at": "2026-03-29T01:05:00Z",
                },
            )
            append_publish_lifecycle_event(
                storage_root=env.storage_root,
                event={"event_name": "publish.transition", "occurred_at": "2026-03-29T01:10:00Z"},
            )

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            detail = client.get(f"/v1/publish/jobs/{job_id}", headers=h)
            self.assertEqual(detail.status_code, 200)
            body = detail.json()
            self.assertEqual(body["job_id"], job_id)
            self.assertIn("global_state_stage_summary", body)
            self.assertIn("publish_state", body)
            self.assertIn("effective_decision", body)
            self.assertIn("schedule", body)
            self.assertIn("attempts", body)
            self.assertIn("last_error", body)
            self.assertIn("audit_trail_summary", body)
            self.assertIn("manual_handoff", body)
            self.assertIn("drift", body)

            audit = client.get(f"/v1/publish/jobs/{job_id}/audit", headers=h)
            self.assertEqual(audit.status_code, 200)
            audit_body = audit.json()
            self.assertEqual(audit_body["job_id"], job_id)
            self.assertEqual(audit_body["limit"], 50)
            self.assertEqual(len(audit_body["items"]), 1)
            self.assertEqual(audit_body["items"][0]["event_name"], "publish.transition")

            no_match = client.get(f"/v1/publish/jobs/{other_job_id}/audit", headers=h, params={"limit": 1})
            self.assertEqual(no_match.status_code, 200)
            self.assertEqual(no_match.json()["limit"], 1)

    def test_job_detail_effective_decision_uses_current_resolution(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            ts = 1_800_000_200.0
            job_id = self._seed_publish_job(
                env,
                channel_slug="darkwood-reverie",
                publish_state="ready_to_publish",
                scheduled_at=ts,
            )

            conn = dbm.connect(env)
            try:
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                conn.execute(
                    """
                    INSERT INTO publish_policy_project_defaults(singleton_key, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id)
                    VALUES(1,'auto','public','policy_requires_manual','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','seed','seed','req-seed-policy')
                    ON CONFLICT(singleton_key) DO UPDATE SET
                        publish_mode=excluded.publish_mode,
                        target_visibility=excluded.target_visibility,
                        reason_code=excluded.reason_code,
                        updated_at=excluded.updated_at,
                        updated_by=excluded.updated_by,
                        last_reason=excluded.last_reason,
                        last_request_id=excluded.last_request_id
                    """
                )
                conn.execute(
                    """
                    INSERT INTO publish_audit_status_project_defaults(singleton_key, status, created_at, updated_at, updated_by, last_reason, last_request_id)
                    VALUES(1,'approved','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','seed','seed','req-seed-audit')
                    ON CONFLICT(singleton_key) DO UPDATE SET
                        status=excluded.status,
                        updated_at=excluded.updated_at,
                        updated_by=excluded.updated_by,
                        last_reason=excluded.last_reason,
                        last_request_id=excluded.last_request_id
                    """
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            baseline = client.get(f"/v1/publish/jobs/{job_id}", headers=h)
            self.assertEqual(baseline.status_code, 200)
            baseline_effective = baseline.json()["effective_decision"]
            self.assertEqual(baseline_effective["decision"], "auto")
            self.assertEqual(baseline_effective["effective_audit_status"], "approved")

            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO publish_global_controls(singleton_key, auto_publish_paused, reason, updated_at, updated_by)
                    VALUES(1,1,'maintenance','2026-01-02T00:00:00Z','seed')
                    ON CONFLICT(singleton_key) DO UPDATE SET
                        auto_publish_paused=excluded.auto_publish_paused,
                        reason=excluded.reason,
                        updated_at=excluded.updated_at,
                        updated_by=excluded.updated_by
                    """
                )
                conn.commit()
            finally:
                conn.close()
            paused = client.get(f"/v1/publish/jobs/{job_id}", headers=h).json()["effective_decision"]
            self.assertEqual(paused["decision"], "hold")
            self.assertEqual(paused["reason_code"], "global_pause_active")
            self.assertTrue(paused["global_auto_publish_paused"])

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE publish_global_controls SET auto_publish_paused = 0, reason = NULL WHERE singleton_key = 1")
                conn.execute("UPDATE publish_audit_status_project_defaults SET status = 'pending', updated_at = '2026-01-03T00:00:00Z'")
                conn.commit()
            finally:
                conn.close()
            audit_pending = client.get(f"/v1/publish/jobs/{job_id}", headers=h).json()["effective_decision"]
            self.assertEqual(audit_pending["decision"], "hold")
            self.assertEqual(audit_pending["reason_code"], "audit_not_approved")
            self.assertEqual(audit_pending["effective_audit_status"], "pending")

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET publish_hold_active = 1, publish_hold_reason_code = 'operator_forced_manual' WHERE id = ?", (job_id,))
                conn.execute("UPDATE publish_audit_status_project_defaults SET status = 'approved', updated_at = '2026-01-04T00:00:00Z'")
                conn.commit()
            finally:
                conn.close()
            hold_active = client.get(f"/v1/publish/jobs/{job_id}", headers=h).json()["effective_decision"]
            self.assertEqual(hold_active["decision"], "hold")
            self.assertEqual(hold_active["reason_code"], "operator_forced_manual")
            self.assertTrue(hold_active["job_publish_hold_active"])

            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET publish_hold_active = 0, publish_hold_reason_code = NULL WHERE id = ?", (job_id,))
                conn.execute(
                    """
                    INSERT INTO publish_policy_item_overrides(release_id, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id)
                    VALUES(?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(release_id) DO UPDATE SET
                        publish_mode=excluded.publish_mode,
                        target_visibility=excluded.target_visibility,
                        reason_code=excluded.reason_code,
                        updated_at=excluded.updated_at,
                        updated_by=excluded.updated_by,
                        last_reason=excluded.last_reason,
                        last_request_id=excluded.last_request_id
                    """,
                    (
                        release_id,
                        "manual_only",
                        "public",
                        "policy_requires_manual",
                        "2026-01-05T00:00:00Z",
                        "2026-01-05T00:00:00Z",
                        "seed",
                        "seed",
                        "req-seed-item",
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            manual_mode = client.get(f"/v1/publish/jobs/{job_id}", headers=h).json()["effective_decision"]
            self.assertEqual(manual_mode["decision"], "manual_only")
            self.assertEqual(manual_mode["delivery_mode"], "manual_only")

    def test_job_detail_returns_controlled_error_for_invalid_job_hold(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            job_id = self._seed_publish_job(
                env,
                channel_slug="darkwood-reverie",
                publish_state="ready_to_publish",
                scheduled_at=1_800_000_400.0,
            )
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET publish_hold_active = 1, publish_hold_reason_code = NULL WHERE id = ?", (job_id,))
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/publish/jobs/{job_id}", headers=h)
            self.assertEqual(resp.status_code, 422)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "E3_POLICY_RESOLUTION_FAILED")
            self.assertEqual(body["error"]["legacy_code"], "PPP_INVALID_JOB_HOLD")
            self.assertEqual(body["error"]["message"], "publish_hold_active requires publish_hold_reason_code")


if __name__ == "__main__":
    unittest.main()
