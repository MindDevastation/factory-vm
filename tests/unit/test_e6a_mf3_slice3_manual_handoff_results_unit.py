from __future__ import annotations

import unittest

from services.telegram_publish import render_publish_action_result


class TestE6AMf3Slice3ManualHandoffResultsUnit(unittest.TestCase):
    def test_result_renderer_success_and_stale(self) -> None:
        ok = render_publish_action_result(
            telegram_action="ack_manual_handoff",
            gateway_result="ALLOWED",
            ok=True,
            result={"result": {"publish_state_after": "manual_handoff_acknowledged"}},
            error=None,
        )
        self.assertEqual(ok["status"], "SUCCESS")
        self.assertEqual(ok["continuity"]["what_changed"], "manual_handoff_acknowledged")

        stale = render_publish_action_result(
            telegram_action="ack_manual_handoff",
            gateway_result="STALE",
            ok=False,
            result=None,
            error={"code": "E6A_TARGET_STALE", "message": "changed"},
        )
        self.assertEqual(stale["status"], "STALE")
        self.assertEqual(stale["continuity"]["what_failed"], "E6A_TARGET_STALE")


if __name__ == "__main__":
    unittest.main()
