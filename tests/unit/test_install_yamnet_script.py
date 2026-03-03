from __future__ import annotations

import importlib
import subprocess
import unittest
from pathlib import Path
from unittest import mock


class TestInstallYamnetScript(unittest.TestCase):
    def test_main_installs_from_requirements_yamnet_txt(self) -> None:
        mod = importlib.import_module("scripts.install_yamnet")
        mod = importlib.reload(mod)
        req_file = str((Path(__file__).resolve().parents[2] / "requirements-yamnet.txt").resolve())

        captured = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

        with (
            mock.patch.object(mod, "_parse_args", return_value=mock.Mock(target="/tmp/pydeps")),
            mock.patch.object(mod, "get_py_deps_dir", return_value="/tmp/pydeps"),
            mock.patch("scripts.install_yamnet.subprocess.run", side_effect=fake_run),
        ):
            rc = mod.main()

        self.assertEqual(rc, 0)
        self.assertIn("-r", captured["cmd"])
        self.assertIn(req_file, captured["cmd"])


if __name__ == "__main__":
    unittest.main()
