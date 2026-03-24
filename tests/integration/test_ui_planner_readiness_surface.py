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
        self.assertIn("push('readiness_status', $('filter-readiness-status').value);", js)
        self.assertIn("p.set('include_readiness_summary', 'true');", js)

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
        self.assertIn("function orderedDomains(readiness)", js)
        self.assertIn("if (status === 'BLOCKED') return 0;", js)
        self.assertIn("if (status === 'NOT_READY') return 1;", js)
        self.assertIn("const visibleChecks = actionableOnly ? checks.filter(checkIsActionable) : checks;", js)

    def test_compact_reason_preview_affordance_exists(self) -> None:
        _, js = self._load_ui_assets()
        self.assertIn("const compactPreview =", js)
        self.assertIn("title=\"${esc(compactPreview)}\"", js)
        self.assertIn("title=\"${esc(summary.title)}\"", js)

    def test_readiness_ui_actions_use_get_endpoints_only(self) -> None:
        _, js = self._load_ui_assets()
        self.assertIn("include_readiness_summary", js)
        self.assertIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness", js)
        self.assertNotIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness`, {\n      method: 'POST'", js)
        self.assertNotIn("/v1/planner/planned-releases/${plannedReleaseId}/readiness`, {\n      method: 'PATCH'", js)
        self.assertNotIn("materialize", js[js.find("openReadinessDialog"): js.find("openReadinessDialog") + 700])
        self.assertNotIn("/apply", js[js.find("openReadinessDialog"): js.find("openReadinessDialog") + 700])


if __name__ == "__main__":
    unittest.main()
