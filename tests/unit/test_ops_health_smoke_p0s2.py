from __future__ import annotations

import shutil
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
            db_parent = base / "data"
            db_parent.mkdir(parents=True, exist_ok=True)

            env = SimpleNamespace(
                db_path=str(db_parent / "factory.sqlite3"),
                storage_root=str(storage),
                origin_backend="local",
                origin_local_root=str(base / "missing_optional_origin"),
            )
            result = runner.StoragePathsCheck().run(SimpleNamespace(env=env, profile="local"))
            self.assertEqual(result.result, "WARN")
            self.assertEqual(len(result.details["missing_required"]), 0)
            self.assertGreaterEqual(len(result.details["missing_optional"]), 1)

    def test_storage_paths_excludes_seed_and_config_only_paths(self) -> None:
        env = SimpleNamespace(
            db_path="/tmp/factory/db.sqlite3",
            storage_root="/tmp/factory/storage",
            custom_tags_seed_dir="/tmp/factory/custom_seeds",
            origin_backend="remote",
            origin_local_root="/tmp/factory/origin_local",
        )
        required, optional = runner._resolve_storage_paths(env)
        required_paths = {str(path) for path in required}

        self.assertNotIn(str(Path("configs").resolve()), required_paths)
        self.assertNotIn(str(Path(env.custom_tags_seed_dir).expanduser().resolve()), required_paths)
        self.assertEqual(optional, [])

    def test_disk_space_missing_nested_paths_is_bounded_and_does_not_raise(self) -> None:
        check = runner.DiskSpaceCheck()
        env = SimpleNamespace(db_path="/nope/a/b/c/factory.sqlite3", storage_root="/also-missing/x/y/z")
        context = SimpleNamespace(env=env, profile="local")

        usage = shutil._ntuple_diskusage(total=1024**4, used=900 * 1024**3, free=400 * 1024**3)
        with patch("services.ops_health_smoke.runner.shutil.disk_usage", return_value=usage):
            result = check.run(context)

        self.assertEqual(result.result, "PASS")
        self.assertEqual(result.message, "Disk free space is within thresholds")
        self.assertGreaterEqual(len(result.details["paths"]), 1)
        self.assertTrue(all(p["status"] == "PASS" for p in result.details["paths"]))

    def test_disk_space_no_existing_ancestor_returns_deterministic_failure(self) -> None:
        check = runner.DiskSpaceCheck()
        context = SimpleNamespace(env=SimpleNamespace(db_path="/", storage_root="/"), profile="local")

        missing_path = Path("/does-not-exist/child/grandchild")
        with patch.object(check, "_nearest_existing_ancestor", return_value=None):
            with patch("services.ops_health_smoke.runner.Path", side_effect=lambda raw: missing_path if raw == "/" else Path(raw)):
                result = check.run(context)

        self.assertEqual(result.result, "FAIL")
        self.assertEqual(result.details["paths"][0]["status"], "FAIL")
        self.assertEqual(result.details["paths"][0]["error"], "no_existing_ancestor")

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
