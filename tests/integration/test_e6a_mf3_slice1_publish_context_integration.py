from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_publish import load_publish_decision_context
from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job


class TestE6AMf3Slice1PublishContextIntegration(unittest.TestCase):
    def _seed_publish_job(self, conn, *, publish_state: str, reason_code: str | None = None, reason_detail: str | None = None) -> int:
        # seed_minimal_db already created base rows; use helper-created job row and then annotate publish columns
        # helper opens a separate connection, so read latest row by max id in current conn.
        row = conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
        if row is None:
            raise AssertionError("job seed missing")
        job_id = int(row["id"])
        conn.execute(
            "UPDATE jobs SET publish_state=?, publish_reason_code=?, publish_reason_detail=? WHERE id=?",
            (publish_state, reason_code, reason_detail, job_id),
        )
        return job_id

    def test_publish_approval_reject_context_load(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="UPLOAD")
            conn = dbm.connect(env)
            try:
                job_id = self._seed_publish_job(conn, publish_state="ready_to_publish")
                ctx = load_publish_decision_context(conn, job_id=job_id)
                self.assertIsNotNone(ctx)
                self.assertIn("approve", ctx["available_next_actions"])
                self.assertIn("reject", ctx["available_next_actions"])
            finally:
                conn.close()

    def test_manual_handoff_context_load_and_problem_explanation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            insert_release_and_job(env, state="UPLOADED", stage="UPLOAD")
            conn = dbm.connect(env)
            try:
                job_id = self._seed_publish_job(
                    conn,
                    publish_state="manual_handoff_pending",
                    reason_code="policy_requires_manual",
                    reason_detail="manual handoff required",
                )
                ctx = load_publish_decision_context(conn, job_id=job_id)
                self.assertEqual(ctx["publish_state"], "manual_handoff_pending")
                self.assertEqual(ctx["reason"]["code"], "policy_requires_manual")
                self.assertIn("ack_manual_handoff", ctx["available_next_actions"])
                self.assertIn("/jobs/", ctx["web_link"])
                self.assertEqual(ctx["action_surface_safety"], "transition_safe")
                self.assertIn("full timeline", ctx["full_context_hint"])
                self.assertIn("manual handoff", ctx["reason"]["explanation"])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
