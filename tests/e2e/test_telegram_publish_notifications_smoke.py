from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.bot.telegram_publish_notifications import send_critical_publish_notifications
from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class _FakeBot:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_message(self, *, chat_id: int, text: str):
        self.messages.append(f"{chat_id}:{text}")


class TestTelegramPublishNotificationsSmoke(unittest.IsolatedAsyncioTestCase):
    def _seed_state(self, env, state: str, reason: str | None = None, scheduled_offset: float | None = None) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            ts = dbm.now_ts()
            rel = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(ch["id"]), f"notif-{state}", "d", "[]", ts),
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
            conn.execute(
                "UPDATE jobs SET publish_state = ?, publish_reason_code = ?, publish_last_transition_at = ?, publish_scheduled_at = ?, publish_drift_detected_at = ? WHERE id = ?",
                (state, reason, ts, (ts + scheduled_offset if scheduled_offset is not None else None), (ts if state == "publish_state_drift_detected" else None), job_id),
            )
            conn.commit()
            return job_id
        finally:
            conn.close()

    async def test_smoke_all_required_families(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_state(env, "policy_blocked")
            self._seed_state(env, "published_public")
            self._seed_state(env, "publish_failed_terminal")
            self._seed_state(env, "manual_handoff_pending", reason="retries_exhausted")
            self._seed_state(env, "manual_handoff_pending", reason="invalid_configuration")
            self._seed_state(env, "waiting_for_schedule", reason="missed_schedule_operator_review", scheduled_offset=-60)
            self._seed_state(env, "publish_state_drift_detected")
            mod = importlib.import_module("services.factory_api.app")
            client = TestClient(importlib.reload(mod).app)
            h = basic_auth_header(env.basic_user, env.basic_pass)
            pause = client.put("/v1/publish/controls", headers=h, json={"auto_publish_paused": True, "reason": "pause"})
            self.assertEqual(pause.status_code, 200)

            bot = _FakeBot()
            sent = await send_critical_publish_notifications(bot=bot, env=env)
            self.assertGreaterEqual(sent, 8)
            blob = "\n".join(bot.messages)
            self.assertIn("policy block", blob)
            self.assertIn("publish success", blob)
            self.assertIn("publish failed", blob)
            self.assertIn("retries exhausted", blob)
            self.assertIn("manual handoff required", blob)
            self.assertIn("missed schedule", blob)
            self.assertIn("drift detected", blob)
            self.assertIn("critical global pause", blob)

            unpause = client.put("/v1/publish/controls", headers=h, json={"auto_publish_paused": False, "reason": "resume"})
            self.assertEqual(unpause.status_code, 200)
            sent2 = await send_critical_publish_notifications(bot=bot, env=env)
            self.assertGreaterEqual(sent2, 1)
            self.assertIn("critical global unblock", "\n".join(bot.messages))


if __name__ == "__main__":
    unittest.main()
