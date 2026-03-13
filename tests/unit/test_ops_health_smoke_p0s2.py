from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.ops_health_smoke import runner


class TestOpsHealthSmokeP0S2(unittest.TestCase):
    def test_disk_threshold_evaluator(self) -> None:
        self.assertEqual(
            runner._evaluate_disk_status(
                free_percent=30,
                free_gib=100,
                warn_percent=15,
                warn_gib=20,
                fail_percent=8,
                fail_gib=10,
            ),
            "PASS",
        )
        self.assertEqual(
            runner._evaluate_disk_status(
                free_percent=14,
                free_gib=100,
                warn_percent=15,
                warn_gib=20,
                fail_percent=8,
                fail_gib=10,
            ),
            "WARN",
        )
        self.assertEqual(
            runner._evaluate_disk_status(
                free_percent=20,
                free_gib=9,
                warn_percent=15,
                warn_gib=20,
                fail_percent=8,
                fail_gib=10,
            ),
            "FAIL",
        )

    def test_db_accessibility_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "factory.sqlite3"
            conn = sqlite3.connect(db_path)
            conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            conn.commit()
            conn.close()

            env = SimpleNamespace(db_path=str(db_path))
            result = runner.DbAccessCheck().run(SimpleNamespace(env=env, profile="local"))
            self.assertEqual(result.result, "PASS")
            self.assertEqual(result.details["quick_check_result"].lower(), "ok")

            env_missing = SimpleNamespace(db_path=str(Path(tmp) / "missing.sqlite3"))
            missing = runner.DbAccessCheck().run(SimpleNamespace(env=env_missing, profile="local"))
            self.assertEqual(missing.result, "FAIL")

    def test_storage_path_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            storage = base / "storage"
            for sub in ["workspace", "outbox", "logs", "qa", "previews"]:
                (storage / sub).mkdir(parents=True, exist_ok=True)
            (base / "configs").mkdir(parents=True, exist_ok=True)
            seeds = base / "custom_seeds"
            seeds.mkdir(parents=True, exist_ok=True)
            db_parent = base / "data"
            db_parent.mkdir(parents=True, exist_ok=True)

            env = SimpleNamespace(
                db_path=str(db_parent / "factory.sqlite3"),
                storage_root=str(storage),
                custom_tags_seed_dir=str(seeds),
                origin_backend="local",
                origin_local_root=str(base / "missing_optional_origin"),
            )
            required = [
                db_parent,
                storage,
                storage / "workspace",
                storage / "outbox",
                storage / "logs",
                storage / "qa",
                storage / "previews",
                base / "configs",
                seeds,
            ]
            optional = [base / "missing_optional_origin"]
            with patch("services.ops_health_smoke.runner._resolve_storage_paths", return_value=(required, optional)):
                result = runner.StoragePathsCheck().run(SimpleNamespace(env=env, profile="local"))
            self.assertEqual(result.result, "WARN")
            self.assertEqual(len(result.details["missing_required"]), 0)
            self.assertGreaterEqual(len(result.details["missing_optional"]), 1)

    def test_ffmpeg_presence_behavior(self) -> None:
        check = runner.FfmpegAvailableCheck()
        context = SimpleNamespace(env=SimpleNamespace(), profile="local")

        with patch("services.ops_health_smoke.runner.shutil.which", return_value=None):
            missing = check.run(context)
        self.assertEqual(missing.result, "FAIL")

        proc = MagicMock(returncode=0, stdout="ffmpeg version 7.0\n", stderr="")
        with patch("services.ops_health_smoke.runner.shutil.which", return_value="/usr/bin/ffmpeg"):
            with patch("services.ops_health_smoke.runner.subprocess.run", return_value=proc):
                present = check.run(context)
        self.assertEqual(present.result, "PASS")
        self.assertIn("ffmpeg version", present.details["version_first_line"])

    def test_health_check_mocked(self) -> None:
        check = runner.ApiHealthCheck()
        env = SimpleNamespace(bind="0.0.0.0", port=8080)
        context = SimpleNamespace(env=env, profile="local")

        class Response:
            status = 200

            def read(self) -> bytes:
                return b'{"ok": true, "db": "ok"}'

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("services.ops_health_smoke.runner.urllib.request.urlopen", return_value=Response()):
            result = check.run(context)
        self.assertEqual(result.result, "PASS")
        self.assertEqual(result.details["http_status"], 200)


if __name__ == "__main__":
    unittest.main()
