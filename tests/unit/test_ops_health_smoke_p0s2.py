from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from services.ops_health_smoke import runner
from services.common.runtime_roles import RuntimeRoleInputs, persist_runtime_role_inputs


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

    def test_worker_heartbeat_evaluator_flags_stale(self) -> None:
        evaluated = runner._evaluate_worker_heartbeats(
            workers=[
                {"worker_id": "fresh-1", "role": "orchestrator", "last_seen": 195.0},
                {"worker_id": "stale-1", "role": "qa", "last_seen": 10.0},
                {"worker_id": "missing-ts", "role": "uploader", "last_seen": None},
            ],
            stale_after_sec=120,
            now_ts=200.0,
        )

        self.assertEqual(evaluated["active_workers"], ["fresh-1"])
        self.assertEqual(set(evaluated["stale_workers"]), {"missing-ts", "stale-1"})
        self.assertIn("qa", evaluated["stale_roles"])

    def test_required_runtime_roles_fails_when_required_missing(self) -> None:
        check = runner.RequiredRuntimeRolesCheck()
        env = SimpleNamespace(bind="127.0.0.1", port=8080, basic_user="admin", basic_pass="secret")
        context = SimpleNamespace(env=env, profile="prod")

        class Response:
            status = 200

            def read(self) -> bytes:
                payload = {"workers": [{"worker_id": "w1", "role": "orchestrator", "last_seen": 190.0}]}
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch.dict(os.environ, {"TRACK_CATALOG_ENABLED": "0", "IMPORTER_ENABLED": "0", "BOT_ENABLED": "0"}, clear=False):
            with patch("services.ops_health_smoke.runner.time.time", return_value=200.0):
                with patch("services.ops_health_smoke.runner.urllib.request.urlopen", return_value=Response()):
                    result = check.run(context)

        self.assertEqual(result.result, "FAIL")
        self.assertIn("qa", result.details["missing_roles"])

    def test_required_runtime_roles_passes_when_optional_disabled(self) -> None:
        check = runner.RequiredRuntimeRolesCheck()
        env = SimpleNamespace(bind="127.0.0.1", port=8080, basic_user="admin", basic_pass="secret")
        context = SimpleNamespace(env=env, profile="prod")

        class Response:
            status = 200

            def read(self) -> bytes:
                payload = {
                    "workers": [
                        {"worker_id": "w1", "role": "orchestrator", "last_seen": 190.0},
                        {"worker_id": "w2", "role": "qa", "last_seen": 190.0},
                        {"worker_id": "w3", "role": "uploader", "last_seen": 190.0},
                        {"worker_id": "w4", "role": "cleanup", "last_seen": 190.0},
                    ]
                }
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch.dict(os.environ, {"TRACK_CATALOG_ENABLED": "0", "IMPORTER_ENABLED": "0", "BOT_ENABLED": "0"}, clear=False):
            with patch("services.ops_health_smoke.runner.time.time", return_value=200.0):
                with patch("services.ops_health_smoke.runner.urllib.request.urlopen", return_value=Response()):
                    result = check.run(context)

        self.assertEqual(result.result, "PASS")
        self.assertIn("track_jobs", result.details["optional_roles"])
        self.assertEqual(result.details["missing_roles"], [])

    def test_worker_heartbeat_check_uses_workers_endpoint_fixture(self) -> None:
        check = runner.WorkerHeartbeatCheck()
        env = SimpleNamespace(bind="127.0.0.1", port=8080, basic_user="admin", basic_pass="secret")
        context = SimpleNamespace(env=env, profile="prod")

        class Response:
            status = 200

            def read(self) -> bytes:
                payload = {
                    "workers": [
                        {"worker_id": "w-fresh", "role": "orchestrator", "last_seen": 190.0},
                        {"worker_id": "w-stale", "role": "qa", "last_seen": 20.0},
                    ]
                }
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch("services.ops_health_smoke.runner.time.time", return_value=200.0):
            with patch("services.ops_health_smoke.runner.urllib.request.urlopen", return_value=Response()):
                result = check.run(context)

        self.assertEqual(result.result, "FAIL")
        self.assertIn("w-stale", result.details["stale_workers"])

    def test_required_runtime_roles_uses_runtime_input_flags(self) -> None:
        check = runner.RequiredRuntimeRolesCheck()
        env = SimpleNamespace(bind="127.0.0.1", port=8080, basic_user="admin", basic_pass="secret")
        context = SimpleNamespace(env=env, profile="prod")

        class Response:
            status = 200

            def read(self) -> bytes:
                payload = {
                    "workers": [
                        {"worker_id": "w1", "role": "orchestrator", "last_seen": 190.0},
                        {"worker_id": "w2", "role": "qa", "last_seen": 190.0},
                        {"worker_id": "w3", "role": "uploader", "last_seen": 190.0},
                        {"worker_id": "w4", "role": "cleanup", "last_seen": 190.0},
                        {"worker_id": "w5", "role": "bot", "last_seen": 190.0},
                    ]
                }
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch.dict(
            os.environ,
            {"FACTORY_RUNTIME_WITH_BOT": "1", "FACTORY_RUNTIME_NO_IMPORTER": "1", "TRACK_CATALOG_ENABLED": "0"},
            clear=False,
        ):
            with patch("services.ops_health_smoke.runner.time.time", return_value=200.0):
                with patch("services.ops_health_smoke.runner.urllib.request.urlopen", return_value=Response()):
                    result = check.run(context)

        self.assertEqual(result.result, "PASS")
        self.assertEqual(result.details["runtime_inputs"], {"no_importer_flag": True, "with_bot_flag": True})
        self.assertEqual(result.details["missing_roles"], [])

    def test_required_runtime_roles_reads_shared_runtime_inputs_file(self) -> None:
        check = runner.RequiredRuntimeRolesCheck()
        env = SimpleNamespace(bind="127.0.0.1", port=8080, basic_user="admin", basic_pass="secret")
        context = SimpleNamespace(env=env, profile="prod")

        class Response:
            status = 200

            def read(self) -> bytes:
                payload = {
                    "workers": [
                        {"worker_id": "w1", "role": "orchestrator", "last_seen": 190.0},
                        {"worker_id": "w2", "role": "qa", "last_seen": 190.0},
                        {"worker_id": "w3", "role": "uploader", "last_seen": 190.0},
                        {"worker_id": "w4", "role": "cleanup", "last_seen": 190.0},
                        {"worker_id": "w5", "role": "bot", "last_seen": 190.0},
                    ]
                }
                return json.dumps(payload).encode("utf-8")

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_inputs_file = Path(tmpdir) / "runtime-inputs.json"
            persist_runtime_role_inputs(
                RuntimeRoleInputs(profile="prod", no_importer_flag=True, with_bot_flag=True),
                environ={"FACTORY_RUNTIME_INPUTS_FILE": str(runtime_inputs_file)},
            )
            with patch.dict(
                os.environ,
                {"FACTORY_RUNTIME_INPUTS_FILE": str(runtime_inputs_file), "TRACK_CATALOG_ENABLED": "0"},
                clear=True,
            ):
                with patch("services.ops_health_smoke.runner.time.time", return_value=200.0):
                    with patch("services.ops_health_smoke.runner.urllib.request.urlopen", return_value=Response()):
                        result = check.run(context)

        self.assertEqual(result.result, "PASS")
        self.assertEqual(result.details["runtime_inputs"], {"no_importer_flag": True, "with_bot_flag": True})
        self.assertEqual(result.details["required_roles"], ["bot", "cleanup", "orchestrator", "qa", "uploader"])
        self.assertIn("track_jobs", result.details["optional_roles"])

    def test_worker_heartbeat_last_seen_epoch_contract(self) -> None:
        evaluated = runner._evaluate_worker_heartbeats(
            workers=[{"worker_id": "w1", "role": "orchestrator", "last_seen": "195.0"}],
            stale_after_sec=120,
            now_ts=200.0,
        )
        self.assertEqual(evaluated["active_workers"], ["w1"])


    def test_telegram_ready_severity_and_local_init(self) -> None:
        check = runner.TelegramReadyCheck()
        base_env = SimpleNamespace(tg_bot_token="", tg_admin_chat_id=0)

        with patch.dict(os.environ, {"FACTORY_RUNTIME_WITH_BOT": "0"}, clear=False):
            warning_result = check.run(SimpleNamespace(env=base_env, profile="local"))
        self.assertEqual(warning_result.severity, "warning")
        self.assertEqual(warning_result.result, "WARN")
        self.assertFalse(warning_result.details["bot_required_by_profile"])

        with patch.dict(os.environ, {"FACTORY_RUNTIME_WITH_BOT": "1"}, clear=False):
            critical_result = check.run(SimpleNamespace(env=base_env, profile="prod"))
        self.assertEqual(critical_result.severity, "critical")
        self.assertEqual(critical_result.result, "FAIL")
        self.assertTrue(critical_result.details["bot_required_by_profile"])

        fake_aiogram = SimpleNamespace(Bot=lambda **_kw: object())
        fake_enums = SimpleNamespace(ParseMode=SimpleNamespace(HTML="HTML"))
        ok_env = SimpleNamespace(tg_bot_token="123:abc", tg_admin_chat_id=42)
        with patch.dict(__import__("sys").modules, {"aiogram": fake_aiogram, "aiogram.enums": fake_enums}, clear=False):
            with patch.dict(os.environ, {"FACTORY_RUNTIME_WITH_BOT": "1"}, clear=False):
                ok_result = check.run(SimpleNamespace(env=ok_env, profile="prod"))
        self.assertEqual(ok_result.result, "PASS")
        self.assertTrue(ok_result.details["config_present"])
        self.assertTrue(ok_result.details["init_ok"])

    def test_youtube_ready_presence_and_severity(self) -> None:
        check = runner.YouTubeReadyCheck()

        missing_env = SimpleNamespace(yt_client_secret_json="", yt_tokens_dir="", upload_backend="youtube")
        fail_result = check.run(SimpleNamespace(env=missing_env, profile="prod"))
        self.assertEqual(fail_result.severity, "critical")
        self.assertEqual(fail_result.result, "FAIL")
        self.assertFalse(fail_result.details["config_paths_present"])
        self.assertFalse(fail_result.details["channel_context_available"])

        warn_result = check.run(SimpleNamespace(env=missing_env, profile="local"))
        self.assertEqual(warn_result.severity, "warning")
        self.assertEqual(warn_result.result, "WARN")

    def test_youtube_ready_local_token_load_with_fixture(self) -> None:
        check = runner.YouTubeReadyCheck()
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            channel_slug = "ambient-lab"
            token_path = base / channel_slug / "token.json"
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text('{"access_token":"x"}', encoding="utf-8")
            secret_path = base / "client.json"
            secret_path.write_text('{"installed":{"client_id":"abc"}}', encoding="utf-8")

            env = SimpleNamespace(yt_client_secret_json=str(secret_path), yt_tokens_dir=str(base), upload_backend="youtube")

            class FakeCreds:
                token = "tok"

            fake_credentials_module = SimpleNamespace(Credentials=SimpleNamespace(from_authorized_user_file=lambda *_a, **_k: FakeCreds()))
            with patch.dict(__import__("sys").modules, {"google.oauth2.credentials": fake_credentials_module}, clear=False):
                with patch.dict(os.environ, {"FACTORY_YT_CHANNEL_SLUG": channel_slug}, clear=False):
                    result = check.run(SimpleNamespace(env=env, profile="prod"))

        self.assertEqual(result.result, "PASS")
        self.assertTrue(result.details["channel_context_available"])
        self.assertEqual(Path(result.details["token_path"]), token_path.resolve())
        self.assertTrue(result.details["token_load_ok"])
        self.assertTrue(result.details["client_load_ok"])

    def test_youtube_ready_channel_context_comes_from_factory_channel_slug_env(self) -> None:
        check = runner.YouTubeReadyCheck()
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            canonical_slug = "ambient-lab"
            decoy_slug = "not-used"

            (base / canonical_slug).mkdir(parents=True, exist_ok=True)
            (base / canonical_slug / "token.json").write_text('{"access_token":"x"}', encoding="utf-8")
            # Decoy token path exists under a different slug to prove the check resolves
            # channel context specifically from FACTORY_YT_CHANNEL_SLUG.
            (base / decoy_slug).mkdir(parents=True, exist_ok=True)
            (base / decoy_slug / "token.json").write_text('{"access_token":"y"}', encoding="utf-8")

            secret_path = base / "client.json"
            secret_path.write_text('{"installed":{"client_id":"abc"}}', encoding="utf-8")
            env = SimpleNamespace(yt_client_secret_json=str(secret_path), yt_tokens_dir=str(base), upload_backend="youtube")

            class FakeCreds:
                token = "tok"

            fake_credentials_module = SimpleNamespace(Credentials=SimpleNamespace(from_authorized_user_file=lambda *_a, **_k: FakeCreds()))
            with patch.dict(__import__("sys").modules, {"google.oauth2.credentials": fake_credentials_module}, clear=False):
                with patch.dict(
                    os.environ,
                    {
                        "FACTORY_YT_CHANNEL_SLUG": canonical_slug,
                        "FACTORY_ACTIVE_CHANNEL_SLUG": decoy_slug,
                    },
                    clear=False,
                ):
                    result = check.run(SimpleNamespace(env=env, profile="prod"))

        self.assertEqual(result.result, "PASS")
        self.assertEqual(result.details["channel_slug"], canonical_slug)
        self.assertEqual(Path(result.details["token_path"]), (base / canonical_slug / "token.json").resolve())

    def test_youtube_ready_no_global_single_token_fallback(self) -> None:
        check = runner.YouTubeReadyCheck()
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            (base / "token.json").write_text('{"access_token":"global"}', encoding="utf-8")
            secret_path = base / "client.json"
            secret_path.write_text('{"installed":{"client_id":"abc"}}', encoding="utf-8")
            env = SimpleNamespace(yt_client_secret_json=str(secret_path), yt_tokens_dir=str(base), upload_backend="youtube")

            with patch.dict(os.environ, {"FACTORY_YT_CHANNEL_SLUG": ""}, clear=False):
                result = check.run(SimpleNamespace(env=env, profile="prod"))

        self.assertEqual(result.result, "FAIL")
        self.assertFalse(result.details["channel_context_available"])
        self.assertIsNone(result.details["token_path"])
        self.assertFalse(result.details["config_paths_present"])

    def test_youtube_ready_missing_per_channel_token_path_fails(self) -> None:
        check = runner.YouTubeReadyCheck()
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            channel_slug = "ambient-lab"
            (base / "token.json").write_text('{"access_token":"global"}', encoding="utf-8")
            secret_path = base / "client.json"
            secret_path.write_text('{"installed":{"client_id":"abc"}}', encoding="utf-8")
            env = SimpleNamespace(yt_client_secret_json=str(secret_path), yt_tokens_dir=str(base), upload_backend="youtube")

            with patch.dict(os.environ, {"FACTORY_YT_CHANNEL_SLUG": channel_slug}, clear=False):
                result = check.run(SimpleNamespace(env=env, profile="prod"))

        self.assertEqual(result.result, "FAIL")
        self.assertTrue(result.details["channel_context_available"])
        expected = (base / channel_slug / "token.json").resolve()
        self.assertEqual(Path(result.details["token_path"]), expected)
        self.assertFalse(result.details["config_paths_present"])

    def test_gdrive_ready_severity_and_local_parse(self) -> None:
        check = runner.GDriveReadyCheck()
        missing_env = SimpleNamespace(
            origin_backend="gdrive",
            gdrive_sa_json="",
            gdrive_oauth_client_json="",
            gdrive_oauth_token_json="",
        )
        fail_result = check.run(SimpleNamespace(env=missing_env, profile="prod"))
        self.assertEqual(fail_result.severity, "critical")
        self.assertEqual(fail_result.result, "FAIL")

        with patch.dict(os.environ, {"IMPORTER_ENABLED": "0"}, clear=False):
            warn_result = check.run(SimpleNamespace(env=missing_env, profile="prod"))
        self.assertEqual(warn_result.severity, "warning")
        self.assertEqual(warn_result.result, "WARN")

        with tempfile.TemporaryDirectory() as td:
            sa_path = Path(td) / "sa.json"
            sa_path.write_text('{"type":"service_account","client_email":"svc@example.test"}', encoding="utf-8")
            ok_env = SimpleNamespace(
                origin_backend="gdrive",
                gdrive_sa_json=str(sa_path),
                gdrive_oauth_client_json="",
                gdrive_oauth_token_json="",
            )

            class FakeSvcCreds:
                service_account_email = "svc@example.test"

            fake_sa_module = SimpleNamespace(
                Credentials=SimpleNamespace(from_service_account_file=lambda *_a, **_k: FakeSvcCreds())
            )
            fake_google_oauth2 = SimpleNamespace(service_account=fake_sa_module)
            with patch.dict(
                __import__("sys").modules,
                {"google.oauth2": fake_google_oauth2, "google.oauth2.service_account": fake_sa_module},
                clear=False,
            ):
                ok_result = check.run(SimpleNamespace(env=ok_env, profile="prod"))

        self.assertEqual(ok_result.result, "PASS")
        self.assertTrue(ok_result.details["credential_path_present"])
        self.assertTrue(ok_result.details["credential_parse_ok"])
        self.assertTrue(ok_result.details["client_init_ok"])

    def test_overall_status_uses_severity_model(self) -> None:
        warning_only = [
            runner.CheckResult(
                check_id="w",
                title="warning",
                category="x",
                severity="warning",
                result="FAIL",
                message="warn",
                details={},
            )
        ]
        self.assertEqual(runner._compute_overall(warning_only), ("WARNING", 1))


if __name__ == "__main__":
    unittest.main()
