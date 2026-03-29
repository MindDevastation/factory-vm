from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPublishControlsApi(unittest.TestCase):
    def test_get_put_controls_minimal_surface(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            initial = client.get("/v1/publish/controls", headers=h)
            self.assertEqual(initial.status_code, 200)
            self.assertEqual(initial.json(), {"auto_publish_paused": False, "reason": None})

            put_resp = client.put(
                "/v1/publish/controls",
                headers=h,
                json={"auto_publish_paused": True, "reason": "maintenance"},
            )
            self.assertEqual(put_resp.status_code, 200)
            self.assertEqual(put_resp.json()["ok"], True)

            after = client.get("/v1/publish/controls", headers=h)
            self.assertEqual(after.status_code, 200)
            self.assertEqual(after.json(), {"auto_publish_paused": True, "reason": "maintenance"})

            clear = client.put(
                "/v1/publish/controls",
                headers=h,
                json={"auto_publish_paused": False, "reason": None},
            )
            self.assertEqual(clear.status_code, 200)

            final = client.get("/v1/publish/controls", headers=h)
            self.assertEqual(final.status_code, 200)
            self.assertEqual(final.json(), {"auto_publish_paused": False, "reason": None})

    def test_controls_reject_empty_reason(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            bad = client.put(
                "/v1/publish/controls",
                headers=h,
                json={"auto_publish_paused": True, "reason": "   "},
            )
            self.assertEqual(bad.status_code, 422)
            self.assertEqual(bad.json()["error"]["code"], "PPP_REASON_EMPTY")


if __name__ == "__main__":
    unittest.main()
