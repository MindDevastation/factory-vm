from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.factory_api.problem_readiness_surface import build_grouped_problem_surface
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestE6Mf4S2ProblemGroupedSurface(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_grouping_and_priority_sorting(self) -> None:
        jobs = [
            {"id": 3, "state": "STALE", "stage": "PUBLISH", "error_reason": "stale"},
            {"id": 2, "state": "FAILED", "stage": "RENDER", "error_reason": "ffmpeg"},
            {"id": 1, "state": "BLOCKED", "stage": "PUBLISH", "error_reason": "token"},
        ]
        surface = build_grouped_problem_surface(jobs=jobs)
        blockers = surface["groups"]["blockers"]
        self.assertEqual([item["job_id"] for item in blockers], [2, 1])
        self.assertEqual(surface["summary"]["stale"], 1)
        self.assertEqual(blockers[0]["routing_targets"][0]["kind"], "entity_workspace")

    def test_grouped_endpoint_and_ui_page(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE jobs SET state='FAILED', stage='RENDER' WHERE id=(SELECT id FROM jobs ORDER BY id DESC LIMIT 1)")
                conn.commit()
            finally:
                conn.close()
            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = client.get("/v1/problems/readiness/grouped", headers=h).json()
            self.assertIn("groups", payload)
            html = client.get("/ui/problems/readiness", headers=h).text
            self.assertIn("Problems & Readiness", html)
            self.assertIn("Current status", html)


if __name__ == "__main__":
    unittest.main()
