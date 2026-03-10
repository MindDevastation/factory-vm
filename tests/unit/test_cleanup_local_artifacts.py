from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import cleanup_local_artifacts as script


class TestCleanupLocalArtifacts(unittest.TestCase):
    def setUp(self) -> None:
        self.td = tempfile.TemporaryDirectory()
        self.root = Path(self.td.name)

    def tearDown(self) -> None:
        self.td.cleanup()

    def _mkfile(self, rel: str) -> Path:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
        return path

    def _mkdir(self, rel: str) -> Path:
        path = self.root / rel
        path.mkdir(parents=True, exist_ok=True)
        return path

    def test_coverage_flag_removes_only_coverage_artifacts(self) -> None:
        coverage_file = self._mkfile(".coverage")
        cov_xml = self._mkfile("coverage.xml")
        htmlcov = self._mkdir("htmlcov")
        qa_file = self._mkfile(".qa_secret.log")

        with (
            mock.patch.object(script, "REPO_ROOT", self.root),
            mock.patch.object(script, "_parse_args", return_value=mock.Mock(qa=False, coverage=True, exports=False, pydeps=False, all_safe=False)),
        ):
            rc = script.main()

        self.assertEqual(rc, 0)
        self.assertFalse(coverage_file.exists())
        self.assertFalse(cov_xml.exists())
        self.assertFalse(htmlcov.exists())
        self.assertTrue(qa_file.exists())

    def test_all_safe_removes_selected_groups_and_keeps_db(self) -> None:
        qa_dir = self._mkdir("qa")
        exports_dir = self._mkdir("exports")
        pydeps_dir = self._mkdir("data/pydeps")
        db_file = self._mkfile("data/factory.sqlite3")

        with (
            mock.patch.object(script, "REPO_ROOT", self.root),
            mock.patch.object(script, "_parse_args", return_value=mock.Mock(qa=False, coverage=False, exports=False, pydeps=False, all_safe=True)),
        ):
            rc = script.main()

        self.assertEqual(rc, 0)
        self.assertFalse(qa_dir.exists())
        self.assertFalse(exports_dir.exists())
        self.assertFalse(pydeps_dir.exists())
        self.assertTrue(db_file.exists())


if __name__ == "__main__":
    unittest.main()
