from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from services.common import config as cfg
from services.common import ffmpeg as ffm
from services.common import logging_setup
from services.integrations import local_fs
from tests._helpers import temp_env


class TestMiscSmallCoverage(unittest.TestCase):
    def test_dump_json_indented(self):
        s = cfg.dump_json({"a": 1})
        self.assertIn("\n", s)
        self.assertEqual(json.loads(s)["a"], 1)

    def test_local_fs_load_meta_missing_and_invalid(self):
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            self.assertIsNone(local_fs.load_meta(folder))
            (folder / "meta.json").write_text("{bad", encoding="utf-8")
            self.assertIsNone(local_fs.load_meta(folder))

    def test_ffmpeg_run_uses_popen(self):
        p = Mock()
        p.communicate.return_value = ("OUT", "ERR")
        p.returncode = 7
        with patch("subprocess.Popen", Mock(return_value=p)):
            code, out, err = ffm.run(["x"])  # type: ignore[list-item]
        self.assertEqual(code, 7)
        self.assertEqual(out, "OUT")
        self.assertEqual(err, "ERR")

    def test_setup_logging_is_idempotent(self):
        with temp_env() as (_td, env):
            logging_setup.setup_logging(env, service="svc_once")
            logging_setup.setup_logging(env, service="svc_once")
