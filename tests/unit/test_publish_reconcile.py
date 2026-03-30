from __future__ import annotations

import unittest

from services.factory_api.publish_reconcile import classify_drift


class TestPublishReconcileClassification(unittest.TestCase):
    def test_classify_drift_detected_when_expected_and_observed_mismatch(self) -> None:
        result = classify_drift(expected_visibility="public", observed_visibility="unlisted")
        self.assertEqual(result.classification, "drift_detected")
        self.assertEqual(result.expected_visibility, "public")
        self.assertEqual(result.observed_visibility, "unlisted")

    def test_classify_no_drift_when_expected_and_observed_match(self) -> None:
        result = classify_drift(expected_visibility="public", observed_visibility="public")
        self.assertEqual(result.classification, "no_drift")

    def test_classify_source_unavailable_when_observed_visibility_is_missing(self) -> None:
        result = classify_drift(expected_visibility="public", observed_visibility="")
        self.assertEqual(result.classification, "source_unavailable")


if __name__ == "__main__":
    unittest.main()
