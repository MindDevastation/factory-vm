from __future__ import annotations

import unittest

from services.analytics_center.errors import AnalyticsDomainError
from services.analytics_center.reporting import build_related_domain_jump, validate_report_request


class TestMf6ReportingHelpers(unittest.TestCase):
    def test_report_scope_and_artifact_validation(self) -> None:
        validate_report_request(report_scope_type="OVERVIEW", artifact_type="XLSX")
        with self.assertRaises(AnalyticsDomainError):
            validate_report_request(report_scope_type="BAD", artifact_type="XLSX")
        with self.assertRaises(AnalyticsDomainError):
            validate_report_request(report_scope_type="OVERVIEW", artifact_type="BAD")

    def test_related_domain_jump_builder(self) -> None:
        jump = build_related_domain_jump(target_domain="PUBLISH", scope_ref="darkwood", next_action="open")
        self.assertIn("path", jump)
        with self.assertRaises(AnalyticsDomainError):
            build_related_domain_jump(target_domain="UNKNOWN", scope_ref="x", next_action="open")


if __name__ == "__main__":
    unittest.main()
