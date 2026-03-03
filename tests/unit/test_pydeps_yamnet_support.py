from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path

from services.common.pydeps import ensure_py_deps_on_sys_path, get_py_deps_dir
from tests._pydeps_helpers import make_persistent_pydeps_dir, write_dummy_tf_modules
from services.workers.yamnet_support import assert_yamnet_available
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


if __name__ == "__main__":
    unittest.main()
