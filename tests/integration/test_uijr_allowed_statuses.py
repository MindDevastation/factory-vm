from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestUiJobsRenderAllowedStatusesEndpoint(unittest.TestCase):
    def test_requires_basic_auth(self) -> None:
        with temp_env() as (_, _):
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get("/v1/ui/jobs/render_allowed_statuses")

            self.assertEqual(response.status_code, 401)

    def test_returns_draft_only_allowlist(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get(
                "/v1/ui/jobs/render_allowed_statuses",
                headers=basic_auth_header(env.basic_user, env.basic_pass),
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json(),
                {"render_allowed_statuses": ["Draft"]},
            )


if __name__ == "__main__":
    unittest.main()
