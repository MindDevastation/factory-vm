from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.recommendation_runtime import validate_lifecycle_transition


class TestMf5RecommendationRuntime(unittest.TestCase):
    def test_lifecycle_transition_validation(self) -> None:
        validate_lifecycle_transition(current="OPEN", target="ACKNOWLEDGED")
        validate_lifecycle_transition(current="OPEN", target="DISMISSED")
        with self.assertRaises(AnalyticsDomainError):
            validate_lifecycle_transition(current="DISMISSED", target="OPEN")


if __name__ == "__main__":
    unittest.main()
