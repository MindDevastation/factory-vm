from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from services.common.profile import load_profile_env


class TestProfileLoadEnv(unittest.TestCase):
    def test_load_profile_env_prefers_profile_file(self) -> None:
        old = os.getcwd()
        old_env = os.environ.copy()
        td = tempfile.TemporaryDirectory()
        try:
            os.chdir(td.name)
            Path("deploy").mkdir(parents=True, exist_ok=True)
            (Path("deploy") / "env.test").write_text("FACTORY_BASIC_AUTH_PASS=from_profile\n", encoding="utf-8")
            (Path("deploy") / "env").write_text("FACTORY_BASIC_AUTH_PASS=from_fallback\n", encoding="utf-8")

            os.environ["FACTORY_PROFILE"] = "test"
            os.environ.pop("FACTORY_BASIC_AUTH_PASS", None)

            loaded = load_profile_env()
            self.assertTrue(loaded.endswith(str(Path("deploy") / "env.test")))
            self.assertEqual(os.environ.get("FACTORY_BASIC_AUTH_PASS"), "from_profile")
        finally:
            os.environ.clear()
            os.environ.update(old_env)
            os.chdir(old)
            td.cleanup()


if __name__ == "__main__":
    unittest.main()
