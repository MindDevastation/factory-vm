from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

from services.common.env import Env
from services.common.pydeps import ensure_py_deps_on_sys_path
from tests._helpers import basic_auth_header, temp_env


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
        with temp_env() as (_, env):
            pydeps = Path(env.storage_root) / "pydeps_test"
            os.environ["FACTORY_PY_DEPS_DIR"] = str(pydeps)
            (pydeps / "tensorflow").mkdir(parents=True, exist_ok=True)
            (pydeps / "tensorflow_hub").mkdir(parents=True, exist_ok=True)
            (pydeps / "tensorflow" / "__init__.py").write_text('__version__ = "0.0-test"\n', encoding="utf-8")
            (pydeps / "tensorflow_hub" / "__init__.py").write_text("", encoding="utf-8")
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
            self.assertEqual(payload["target_dir"], str(pydeps))


if __name__ == "__main__":
    unittest.main()
