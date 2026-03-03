from __future__ import annotations

import importlib
import os
import sys
import unittest
from unittest import mock
from pathlib import Path

from services.common.pydeps import ensure_py_deps_on_sys_path, get_py_deps_dir
from tests._pydeps_helpers import make_persistent_pydeps_dir, write_dummy_tf_modules
from services.workers.yamnet_support import YamnetDepsUnavailableError, assert_yamnet_available
from tests._helpers import temp_env


class TestPydepsYamnetSupport(unittest.TestCase):
    def test_default_pydeps_dir_uses_data_dir_next_to_default_db(self) -> None:
        out = get_py_deps_dir({"FACTORY_DB_PATH": "data/factory.sqlite3"})
        self.assertEqual(out, str(Path("data") / "pydeps"))

    def test_assert_yamnet_available_with_dummy_modules(self) -> None:
        pydeps = make_persistent_pydeps_dir()
        write_dummy_tf_modules(pydeps)

        with temp_env() as (_, env):
            os.environ["FACTORY_PY_DEPS_DIR"] = pydeps
            ensure_py_deps_on_sys_path(os.environ)
            importlib.invalidate_caches()
            sys.modules.pop("tensorflow", None)
            sys.modules.pop("tensorflow_hub", None)

            target = assert_yamnet_available(env)
            self.assertEqual(target, pydeps)

    def test_assert_yamnet_available_reports_pkg_resources_import_error(self) -> None:
        pydeps = make_persistent_pydeps_dir()
        write_dummy_tf_modules(pydeps)

        with temp_env() as (_, env):
            os.environ["FACTORY_PY_DEPS_DIR"] = pydeps
            ensure_py_deps_on_sys_path(os.environ)
            importlib.invalidate_caches()
            sys.modules.pop("tensorflow", None)
            sys.modules.pop("tensorflow_hub", None)

            original_import = __import__

            def side_effect(name, *args, **kwargs):
                if name == "tensorflow_hub":
                    raise ModuleNotFoundError("pkg_resources")
                return original_import(name, *args, **kwargs)

            with mock.patch("builtins.__import__", side_effect=side_effect):
                with self.assertRaisesRegex(YamnetDepsUnavailableError, "pkg_resources") as ctx:
                    assert_yamnet_available(env)

            msg = str(ctx.exception)
            self.assertIn("setuptools<71", msg)
            self.assertIn("now included in requirements-yamnet.txt", msg)
            self.assertIn("target_dir=", msg)
            self.assertIn("python_executable=", msg)


    def test_assert_yamnet_available_includes_tensorflow_io_guidance_when_tf_missing(self) -> None:
        pydeps = make_persistent_pydeps_dir()

        with temp_env() as (_, env):
            os.environ["FACTORY_PY_DEPS_DIR"] = pydeps
            ensure_py_deps_on_sys_path(os.environ)
            importlib.invalidate_caches()
            sys.modules.pop("tensorflow", None)
            sys.modules.pop("tensorflow_hub", None)

            original_import = __import__

            def side_effect(name, *args, **kwargs):
                if name == "tensorflow":
                    raise ModuleNotFoundError("tensorflow")
                return original_import(name, *args, **kwargs)

            with mock.patch("builtins.__import__", side_effect=side_effect):
                with self.assertRaisesRegex(YamnetDepsUnavailableError, "requirements-yamnet.txt") as ctx:
                    assert_yamnet_available(env)

            msg = str(ctx.exception)
            self.assertIn("tensorflow-io", msg)
            self.assertIn("Install Yamnet", msg)



if __name__ == "__main__":
    unittest.main()
