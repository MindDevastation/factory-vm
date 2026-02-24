from __future__ import annotations

import os
import unittest
from pathlib import Path

from services.common.utils import safe_slug
from services.common.logging_setup import safe_path_basename, append_job_log

from tests._helpers import temp_env


class TestUtilsAndLogging(unittest.TestCase):
    def test_safe_slug(self) -> None:
        self.assertEqual(safe_slug("Hello World"), "hello_world")
        self.assertEqual(safe_slug("   "), "item")
        self.assertEqual(safe_slug("A__B"), "a_b")
        self.assertTrue(len(safe_slug("x" * 999, max_len=10)) <= 10)

    def test_safe_path_basename(self) -> None:
        self.assertEqual(safe_path_basename("../x/y.png", fallback="f"), "y.png")
        self.assertEqual(safe_path_basename("", fallback="f"), "f")

    def test_append_job_log_writes_lines(self) -> None:
        with temp_env() as (td, env):
            append_job_log(env, 1, "hello")
            append_job_log(env, 1, "world")
            p = Path(env.storage_root) / "logs" / "job_1.log"
            self.assertTrue(p.exists())
            txt = p.read_text(encoding="utf-8")
            self.assertIn("hello\n", txt)
            self.assertIn("world\n", txt)


if __name__ == "__main__":
    unittest.main()
