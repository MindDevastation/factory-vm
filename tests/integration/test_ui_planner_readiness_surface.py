from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestUiPlannerReadinessSurface(unittest.TestCase):
    def _load_ui_assets(self) -> tuple[str, str]:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            html_resp = client.get("/ui/planner", headers=headers)
            self.assertEqual(html_resp.status_code, 200)

            js_resp = client.get("/static/planner_bulk_releases.js", headers=headers)
            self.assertEqual(js_resp.status_code, 200)

            return html_resp.text, js_resp.text

    def test_readiness_badge_visible_in_planner_row_surface(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn(">Readiness<", html)
        self.assertIn("data-readiness-open", js)
        self.assertIn("renderReadinessBadge", js)

    def test_readiness_filter_control_and_query_wiring_present(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="filter-readiness-status"', html)
        self.assertIn('id="filter-readiness-problem"', html)
        self.assertIn("push('readiness_status', $('filter-readiness-status').value);", js)
        self.assertIn("push('readiness_problem', $('filter-readiness-problem').value);", js)
        self.assertIn("p.set('include_readiness_summary', 'true');", js)
        self.assertIn('id="sort-by"', html)
        self.assertIn('id="readiness-priority"', html)
        self.assertIn("if ($('sort-by').value === 'readiness_priority') {", js)
        self.assertIn("push('readiness_priority', $('readiness-priority').value);", js)

    def test_readiness_summary_strip_visible(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="readiness-summary-strip"', html)
        self.assertIn('id="readiness-summary-attention"', html)
        self.assertIn("function renderReadinessSummary(summary)", js)
        self.assertIn("$('readiness-summary-attention').textContent", js)

    def test_refresh_readiness_action_reissues_current_request(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="refresh-readiness-btn"', html)
        self.assertIn("$('refresh-readiness-btn').addEventListener('click', async () => { try { await loadList(); }", js)

    def test_details_dialog_contains_all_domains_reasons_and_remediation(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="readiness-dialog"', html)
        self.assertIn('id="readiness-dialog-computed-at"', html)
        self.assertIn('id="readiness-dialog-primary-reason"', html)
        self.assertIn('id="readiness-dialog-primary-remediation"', html)
        self.assertIn("READINESS_DOMAINS = ['planning_identity', 'scheduling', 'metadata', 'playlist', 'visual_assets']", js)
        self.assertIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness", js)
        self.assertIn("Remediation:", js)

    def test_actionable_only_toggle_exists_and_hides_pass_checks(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="readiness-actionable-only"', html)
        self.assertIn("function checkIsActionable(check)", js)
        self.assertIn("return String(check?.status || '') !== 'PASS';", js)
        self.assertIn("function domainStatusRank(status)", js)
        self.assertIn("function orderedDomains(readiness)", js)
        self.assertIn("if (status === 'BLOCKED') return 0;", js)
        self.assertIn("if (status === 'NOT_READY') return 1;", js)
        self.assertIn("return 2;", js)
        self.assertIn("const visibleChecks = actionableOnly ? checks.filter(checkIsActionable) : checks;", js)

    def test_compact_reason_preview_affordance_exists(self) -> None:
        _, js = self._load_ui_assets()
        self.assertIn("const compactPreview =", js)
        self.assertIn("title=\"${esc(compactPreview)}\"", js)
        self.assertIn("title=\"${esc(summary.title)}\"", js)


    def test_unavailable_row_and_empty_state_copy_hooks_exist(self) -> None:
        _, js = self._load_ui_assets()
        self.assertIn("PRS_READINESS_UNAVAILABLE", js)
        self.assertIn("const aggregate = hasUnavailableError ? 'UNAVAILABLE'", js)
        self.assertIn("function emptyPlannerMessage()", js)
        self.assertIn("No BLOCKED items in current planner scope.", js)
        self.assertIn("No READY_FOR_MATERIALIZATION items in current planner scope.", js)
        self.assertIn("No items match the selected readiness filter.", js)
        self.assertIn("No planned releases in current planner scope.", js)

    def test_freshness_copy_is_explicit_when_timestamp_missing(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="readiness-summary-computed-at"', html)
        self.assertIn('id="readiness-dialog-computed-at"', html)
        self.assertIn("Not available", js)

    def test_readiness_ui_actions_use_get_endpoints_only(self) -> None:
        _, js = self._load_ui_assets()
        self.assertIn("include_readiness_summary", js)
        self.assertIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness", js)
        self.assertNotIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness`, {\n      method: 'POST'", js)
        self.assertNotIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness`, {\n      method: 'PATCH'", js)
        self.assertNotIn("materialize", js[js.find("openReadinessDialog"): js.find("openReadinessDialog") + 700])
        self.assertNotIn("/apply", js[js.find("openReadinessDialog"): js.find("openReadinessDialog") + 700])

    def test_materialization_row_surface_and_filter_hooks_exist(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="filter-materialized-state"', html)
        self.assertIn("push('materialized_state', $('filter-materialized-state').value);", js)
        self.assertIn(">Materialization<", html)
        self.assertIn("data-materialize-item", js)
        self.assertIn("data-materialization-detail", js)
        self.assertIn("materialization_state_summary", js)
        self.assertIn("binding_diagnostics", js)

    def test_materialization_dialog_and_side_effect_copy_exist(self) -> None:
        html, js = self._load_ui_assets()
        self.assertIn('id="materialization-dialog"', html)
        self.assertIn('id="materialization-summary-body"', html)
        self.assertIn('id="materialization-diagnostics-body"', html)
        self.assertIn('id="materialization-open-release-cta"', html)
        self.assertIn("does not create jobs and does not start render/upload/publish", html)
        self.assertIn("Created new canonical release", js)
        self.assertIn("Returned existing linked release", js)
        self.assertIn("Materialization failed", js)
        self.assertIn("Open release:", js)


if __name__ == "__main__":
    unittest.main()
