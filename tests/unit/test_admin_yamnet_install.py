from __future__ import annotations

import importlib
import os
import sys
import unittest
from unittest import mock

from fastapi.testclient import TestClient

from services.common.env import Env
from services.common.pydeps import ensure_py_deps_on_sys_path
from tests._helpers import basic_auth_header, temp_env
from tests._pydeps_helpers import make_persistent_pydeps_dir, write_dummy_tf_modules


class TestAdminYamnetInstall(unittest.TestCase):
    def test_install_returns_already_installed(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with mock.patch("services.factory_api.app._yamnet_import_status", return_value={"installed": True, "target_dir": "data/pydeps", "import_tf": True, "import_hub": True, "error": None}):
                response = client.post("/v1/admin/yamnet/install", headers=auth)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["ok"], True)
            self.assertEqual(response.json()["installed"], True)

    def test_install_runs_installer_when_missing(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            with (
                mock.patch("services.factory_api.app._yamnet_import_status", side_effect=[
                    {"installed": False, "target_dir": "data/pydeps", "import_tf": False, "import_hub": False, "error": "missing"},
                    {"installed": True, "target_dir": "data/pydeps", "import_tf": True, "import_hub": True, "error": None},
                ]),
                mock.patch("services.factory_api.app._run_yamnet_installer", return_value=(True, "done")),
            ):
                response = client.post("/v1/admin/yamnet/install", headers=auth)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                response.json(),
                {"ok": True, "target_dir": "data/pydeps", "installed": True, "output_tail": "done"},
            )

    def test_status_uses_shared_pydeps_with_dummy_modules(self) -> None:
        pydeps = make_persistent_pydeps_dir()
        write_dummy_tf_modules(pydeps)

        with temp_env() as (_, env):
            os.environ["FACTORY_PY_DEPS_DIR"] = pydeps
            ensure_py_deps_on_sys_path(os.environ)
            importlib.invalidate_caches()
            sys.modules.pop("tensorflow", None)
            sys.modules.pop("tensorflow_hub", None)

            env = Env.load()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            response = client.get("/v1/admin/yamnet/status", headers=auth)

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            self.assertEqual(payload["installed"], True)
            self.assertEqual(payload["import_tf"], True)
            self.assertEqual(payload["import_hub"], True)
            self.assertEqual(payload["target_dir"], pydeps)

    def test_run_yamnet_installer_returns_stderr_tail_on_failure(self) -> None:
        mod = importlib.import_module("services.factory_api.app")
        mod = importlib.reload(mod)

        with mock.patch("services.factory_api.app.subprocess.run", return_value=mock.Mock(returncode=1, stdout="line1", stderr="line2")):
            ok, tail = mod._run_yamnet_installer(target_dir="data/pydeps")

        self.assertFalse(ok)
        self.assertIn("line1", tail)
        self.assertIn("line2", tail)



if __name__ == "__main__":
    unittest.main()
