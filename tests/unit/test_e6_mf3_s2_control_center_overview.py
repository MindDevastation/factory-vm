from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf3S2ControlCenterOverview(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_dashboard_renders_grouped_overview_blocks(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            html = client.get("/", headers=h).text
            self.assertIn("Control Center Overview", html)
            self.assertIn("Factory summary", html)
            self.assertIn("Attention summary", html)
            self.assertIn("Task routing", html)
            self.assertIn("Attention routing", html)
            self.assertIn("next destination", html)
            self.assertIn("Recent jobs overview (expand for table)", html)
            self.assertIn("Continue current work", html)
            self.assertIn("Recently changed", html)
            drawer_start = html.index('id="secondary-surfaces-drawer"')
            overview_idx = html.index("Control Center Overview")
            drawer_close_idx = html.index('id="secondary-surfaces-close-btn"')
            self.assertGreater(overview_idx, drawer_start)
            self.assertLess(overview_idx, drawer_close_idx)


    def test_dashboard_shows_return_to_context_when_token_valid(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            seed = client.get("/ui/planner", headers=h)
            self.assertEqual(seed.status_code, 200)
            marker = "data-context-token='"
            token = seed.text.split(marker, 1)[1].split("'", 1)[0]
            html = client.get(f"/?ctx={token}", headers=h).text
            self.assertIn("Return to previous context", html)


if __name__ == "__main__":
    unittest.main()
