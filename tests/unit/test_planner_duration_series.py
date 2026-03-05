from datetime import timedelta
import unittest

from services.planner.duration import DurationValidationError, parse_duration
from services.planner.series import generate_series_publish_at


class PlannerDurationParserTests(unittest.TestCase):
    def test_parse_days(self):
        self.assertEqual(parse_duration("P2D"), timedelta(days=2))

    def test_parse_hours(self):
        self.assertEqual(parse_duration("PT3H"), timedelta(hours=3))

    def test_parse_minutes(self):
        self.assertEqual(parse_duration("PT45M"), timedelta(minutes=45))

    def test_parse_hours_and_minutes(self):
        self.assertEqual(parse_duration("PT1H30M"), timedelta(hours=1, minutes=30))

    def test_reject_unsupported_tokens(self):
        for value in ("P1W", "P1M", "P1Y", "PT1S", "P1DT1H", "P"):
            with self.assertRaises(DurationValidationError):
                parse_duration(value)


class PlannerSeriesPublishAtTests(unittest.TestCase):
    def test_requires_step_for_multi_item_series_with_start(self):
        with self.assertRaises(ValueError):
            generate_series_publish_at(count=2, start_publish_at="2025-01-15T10:00", step=None)

    def test_generates_publish_at_in_kyiv_then_normalizes(self):
        self.assertEqual(
            generate_series_publish_at(
                count=3,
                start_publish_at="2025-01-15T10:00",
                step=timedelta(hours=1, minutes=30),
            ),
            [
                "2025-01-15T10:00:00+02:00",
                "2025-01-15T11:30:00+02:00",
                "2025-01-15T13:00:00+02:00",
            ],
        )


if __name__ == "__main__":
    unittest.main()
