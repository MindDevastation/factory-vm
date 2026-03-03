from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path

from services.common.pydeps import ensure_py_deps_on_sys_path, get_py_deps_dir
from services.workers.yamnet_support import assert_yamnet_available
from tests._helpers import temp_env


class TestPydepsYamnetSupport(unittest.TestCase):
    def test_default_pydeps_dir_uses_data_dir_next_to_default_db(self) -> None:
        out = get_py_deps_dir({"FACTORY_DB_PATH": "data/factory.sqlite3"})
        self.assertEqual(out, str(Path("data") / "pydeps"))

    def test_assert_yamnet_available_with_dummy_modules(self) -> None:
        with temp_env() as (_, env):
            pydeps = Path(env.storage_root) / "shared_pydeps"
            os.environ["FACTORY_PY_DEPS_DIR"] = str(pydeps)
            (pydeps / "tensorflow").mkdir(parents=True, exist_ok=True)
            (pydeps / "tensorflow_hub").mkdir(parents=True, exist_ok=True)
            (pydeps / "tensorflow" / "__init__.py").write_text('__version__ = "0.0-test"\n', encoding="utf-8")
            (pydeps / "tensorflow_hub" / "__init__.py").write_text("", encoding="utf-8")
            ensure_py_deps_on_sys_path(os.environ)
            importlib.invalidate_caches()
            sys.modules.pop("tensorflow", None)
            sys.modules.pop("tensorflow_hub", None)

            target = assert_yamnet_available(env)
            self.assertEqual(target, str(pydeps))


if __name__ == "__main__":
    unittest.main()
