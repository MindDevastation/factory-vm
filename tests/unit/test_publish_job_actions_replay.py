from __future__ import annotations

import unittest

from services.factory_api.publish_job_actions import replay_logged_mutation


class TestPublishJobActionReplay(unittest.TestCase):
    def test_replay_returns_stable_shape(self) -> None:
        row = {
            "action_type": "retry",
            "request_id": "req-1",
            "job_id": 7,
            "response_json": '{"ok": true, "publish_state_after": "ready_to_publish", "publish_state_before": "retry_pending"}',
        }
        replayed = replay_logged_mutation(row)
        self.assertEqual(replayed["replayed"], True)
        self.assertEqual(replayed["action_type"], "retry")
        self.assertEqual(replayed["request_id"], "req-1")
        self.assertEqual(replayed["job_id"], 7)
        self.assertEqual(replayed["result"]["ok"], True)
        self.assertEqual(replayed["result"]["publish_state_before"], "retry_pending")
        self.assertEqual(replayed["result"]["publish_state_after"], "ready_to_publish")


if __name__ == "__main__":
    unittest.main()
