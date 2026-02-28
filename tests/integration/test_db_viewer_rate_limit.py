from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestDbViewerRateLimitIntegration(unittest.TestCase):
    def test_read_endpoint_enforces_51st_request_limit(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()

            db_viewer_mod = importlib.import_module("services.factory_api.db_viewer")
            importlib.reload(db_viewer_mod)
            original_now_fn = db_viewer_mod._limiter._now_fn
            db_viewer_mod._limiter._requests.clear()
            db_viewer_mod._limiter._now_fn = lambda: 1730000000.0

            app_mod = importlib.import_module("services.factory_api.app")
            importlib.reload(app_mod)
            client = TestClient(app_mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            try:
                for _ in range(50):
                    response = client.get("/v1/db-viewer/tables", headers=auth)
                    self.assertEqual(response.status_code, 200)

                limited = client.get("/v1/db-viewer/tables", headers=auth)
                self.assertEqual(limited.status_code, 429)
                payload = limited.json()
                self.assertEqual(payload["error"]["code"], "DBV_RATE_LIMITED")
                self.assertTrue(payload["error"]["request_id"])
            finally:
                db_viewer_mod._limiter._requests.clear()
                db_viewer_mod._limiter._now_fn = original_now_fn


if __name__ == "__main__":
    unittest.main()
