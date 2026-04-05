from __future__ import annotations

import unittest

from services.factory_api.page_templates import (
    DETAIL_PAGE,
    OVERVIEW_PAGE,
    PROBLEM_LIST_PAGE,
    SUMMARY_REPORT_PAGE,
    WORKSPACE_PAGE,
    classify_page_template,
    page_template_contract,
)
from services.factory_api.ux_registry import route_metadata_for_path


class TestE6Mf2S1PageTemplates(unittest.TestCase):
    def test_route_metadata_for_path_contract(self) -> None:
        meta = route_metadata_for_path(current_path="/ui/publish/queue")
        self.assertIsNotNone(meta)
        self.assertEqual(meta["route_key"], "PUBLISH_QUEUE")
        self.assertEqual(meta["owner_group"], "workspaces")

    def test_overview_workspace_problem_detail_summary_templates(self) -> None:
        self.assertEqual(classify_page_template(current_path="/"), OVERVIEW_PAGE)
        self.assertEqual(classify_page_template(current_path="/ui/planner"), WORKSPACE_PAGE)
        self.assertEqual(classify_page_template(current_path="/ui/publish/failed"), PROBLEM_LIST_PAGE)
        self.assertEqual(classify_page_template(current_path="/ui/publish/jobs/11"), DETAIL_PAGE)
        self.assertEqual(classify_page_template(current_path="/ui/track-catalog/analysis-report"), SUMMARY_REPORT_PAGE)

    def test_unknown_route_defaults_to_overview(self) -> None:
        self.assertEqual(classify_page_template(current_path="/unknown"), OVERVIEW_PAGE)

    def test_page_template_contract_baseline_tag(self) -> None:
        contract = page_template_contract(current_path="/ui/planner")
        self.assertEqual(contract["template"], WORKSPACE_PAGE)
        self.assertEqual(contract["state_contract"], "MF2_S1_BASELINE")


if __name__ == "__main__":
    unittest.main()
