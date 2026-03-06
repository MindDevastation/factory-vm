from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from services.common.ui_job_statuses import UI_JOB_STATUSES
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
            self.assertEqual(body["statuses"], list(UI_JOB_STATUSES))


if __name__ == "__main__":
    unittest.main()
