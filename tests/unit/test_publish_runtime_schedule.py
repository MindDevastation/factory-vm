from __future__ import annotations

import unittest
from datetime import datetime, timezone

from services.publish_runtime.schedule import evaluate_publish_schedule


class TestPublishRuntimeSchedule(unittest.TestCase):
    def test_absent_schedule_is_absent(self) -> None:
        out = evaluate_publish_schedule(planned_at=None)
        self.assertEqual(out.eligibility, "absent")
        self.assertIsNone(out.normalized_publish_at)
        self.assertIsNone(out.publish_scheduled_at_ts)

    def test_offsetless_input_is_normalized_with_kyiv_timezone(self) -> None:
        out = evaluate_publish_schedule(planned_at="2026-03-29T10:00:00", now=datetime(2026, 3, 29, 6, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(out.normalized_publish_at, "2026-03-29T10:00:00+03:00")
        self.assertEqual(out.eligibility, "future")

    def test_explicit_utc_input_preserved_and_future(self) -> None:
        out = evaluate_publish_schedule(planned_at="2026-03-29T12:00:00Z", now=datetime(2026, 3, 29, 11, 59, 0, tzinfo=timezone.utc))
        self.assertEqual(out.normalized_publish_at, "2026-03-29T12:00:00+00:00")
        self.assertEqual(out.eligibility, "future")

    def test_past_due_is_detected(self) -> None:
        out = evaluate_publish_schedule(planned_at="2026-03-29T12:00:00+00:00", now=datetime(2026, 3, 29, 12, 1, 0, tzinfo=timezone.utc))
        self.assertEqual(out.eligibility, "past_due")
        self.assertIsNotNone(out.publish_scheduled_at_ts)


if __name__ == "__main__":
    unittest.main()
