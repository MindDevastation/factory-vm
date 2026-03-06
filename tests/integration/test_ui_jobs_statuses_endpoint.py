from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from services.track_analyzer import track_jobs_db
from tests._helpers import basic_auth_header, temp_env


class TestUiJobsStatusesEndpoint(unittest.TestCase):
    def test_requires_basic_auth(self) -> None:
        with temp_env() as (_, _):
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get("/v1/ui/jobs/statuses")

            self.assertEqual(response.status_code, 401)

    def test_returns_source_of_truth_statuses(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get(
                "/v1/ui/jobs/statuses",
                headers=basic_auth_header(env.basic_user, env.basic_pass),
            )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertIn("statuses", body)
            self.assertTrue(body["statuses"])
            expected = []
            for status in track_jobs_db.RUNNING_STATUSES + track_jobs_db.TERMINAL_STATUSES:
                if status not in expected:
                    expected.append(status)
            self.assertEqual(body["statuses"], expected)


if __name__ == "__main__":
    unittest.main()
