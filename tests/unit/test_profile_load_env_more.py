from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from services.common.profile import load_profile_env


class TestProfileLoadEnvMore(unittest.TestCase):
    def test_load_profile_env_falls_back_to_deploy_env(self):
        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as td:
            os.chdir(td)
            try:
                Path("deploy").mkdir()
                Path("deploy/env").write_text("A=1", encoding="utf-8")
                os.environ.pop("FACTORY_PROFILE", None)
                p = load_profile_env()
                self.assertTrue(p.endswith("deploy/env"))
            finally:
                os.chdir(old_cwd)

    def test_load_profile_env_returns_empty_when_no_files(self):
        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as td:
            os.chdir(td)
            try:
                os.environ["FACTORY_PROFILE"] = "local"
                p = load_profile_env()
                self.assertEqual(p, "")
            finally:
                os.chdir(old_cwd)
