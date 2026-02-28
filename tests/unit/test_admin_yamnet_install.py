from __future__ import annotations

import importlib
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestAdminYamnetInstall(unittest.TestCase):
    def test_install_returns_already_installed(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with mock.patch("services.factory_api.app._yamnet_is_installed", return_value=True):
                response = client.post("/v1/admin/yamnet/install", headers=auth)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), {"ok": True, "status": "already_installed"})

    def test_install_runs_installer_when_missing(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with (
                mock.patch("services.factory_api.app._yamnet_is_installed", return_value=False),
                mock.patch("services.factory_api.app._run_yamnet_installer", return_value=(True, "yamnet dependencies installed")),
            ):
                response = client.post("/v1/admin/yamnet/install", headers=auth)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json(),
                {"ok": True, "status": "installed", "message": "yamnet dependencies installed"},
            )


if __name__ == "__main__":
    unittest.main()
