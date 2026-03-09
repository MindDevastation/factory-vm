import unittest

from services.planner.time_normalization import (
    PublishAtValidationError,
    normalize_publish_at,
)


class PlannerTimeNormalizationTests(unittest.TestCase):
    def test_naive_datetime_uses_kyiv_winter_offset(self):
        self.assertEqual(
            normalize_publish_at("2025-01-15T10:30"),
            "2025-01-15T10:30:00+02:00",
        )

    def test_naive_datetime_uses_kyiv_summer_offset(self):
        self.assertEqual(
            normalize_publish_at("2025-07-15T10:30"),
            "2025-07-15T10:30:00+03:00",
        )

    def test_offset_and_z_inputs_are_kept_with_explicit_offset(self):
        self.assertEqual(
            normalize_publish_at("2025-03-02T14:05:07+05:30"),
            "2025-03-02T14:05:07+05:30",
        )
        self.assertEqual(
            normalize_publish_at("2025-03-02T14:05Z"),
            "2025-03-02T14:05:00+00:00",
        )

    def test_invalid_input_raises_validation_error(self):
        with self.assertRaises(PublishAtValidationError):
            normalize_publish_at("03/02/2025 14:05")


if __name__ == "__main__":
    unittest.main()
