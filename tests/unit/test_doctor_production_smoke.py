from __future__ import annotations

import unittest
from collections import namedtuple
from unittest.mock import patch

from services.common.env import Env
from scripts.doctor import _compute_overall, _run_production_smoke
from scripts.run_stack import _worker_roles


DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


class TestDoctorProductionSmoke(unittest.TestCase):
    def _env(self, **overrides: object) -> Env:
        base = Env(
            db_path="data/factory.sqlite3",
            storage_root="storage",
            bind="0.0.0.0",
            port=8080,
            basic_user="admin",
            basic_pass="change_me",
            origin_backend="local",
            origin_local_root="local_origin",
            upload_backend="mock",
            telegram_enabled=0,
            gdrive_root_id="",
            gdrive_library_root_id="",
            gdrive_sa_json="",
            gdrive_oauth_client_json="",
            gdrive_oauth_token_json="",
            oauth_redirect_base_url="",
            oauth_state_secret="",
            gdrive_client_secret_json="",
            gdrive_tokens_dir="",
            yt_client_secret_json="",
            yt_tokens_dir="",
            tg_bot_token="",
            tg_admin_chat_id=0,
            qa_volumedetect_seconds=60,
            job_lock_ttl_sec=3600,
            retry_backoff_sec=300,
            max_render_attempts=3,
            max_upload_attempts=3,
            worker_sleep_sec=5,
        )
        return Env(**{**base.__dict__, **overrides})

    def test_compute_overall_rules(self) -> None:
        status, code = _compute_overall([
            {"severity": "critical", "result": "PASS"},
            {"severity": "warning", "result": "WARN"},
        ])
        self.assertEqual((status, code), ("WARNING", 1))

        status, code = _compute_overall([
            {"severity": "critical", "result": "FAIL"},
            {"severity": "warning", "result": "PASS"},
        ])
        self.assertEqual((status, code), ("FAIL", 2))

        status, code = _compute_overall([
            {"severity": "critical", "result": "PASS"},
            {"severity": "warning", "result": "PASS"},
            {"severity": "info", "result": "SKIP"},
        ])
        self.assertEqual((status, code), ("OK", 0))

    def test_smoke_ok_with_required_workers(self) -> None:
        env = self._env()
        expected_roles = _worker_roles(no_importer_flag=False)

        def fake_http(url: str, *, timeout_sec: float, headers=None):
            self.assertEqual(timeout_sec, 1.0)
            if url.endswith("/health"):
                return {"ok": True, "db": "ok"}
            if url.endswith("/v1/workers"):
                return {"workers": [{"role": role} for role in expected_roles]}
            raise AssertionError(url)

        with patch("scripts.doctor.shutil.disk_usage", return_value=DiskUsage(total=100 * 1024**3, used=60 * 1024**3, free=40 * 1024**3)):
            with patch("scripts.doctor._http_json", side_effect=fake_http):
                report = _run_production_smoke(env, timeout_sec=1.0)

        self.assertEqual(report["status"], "OK")
        self.assertEqual(report["exit_code"], 0)

    def test_smoke_warning_when_optional_readiness_fails(self) -> None:
        env = self._env(
            origin_backend="gdrive",
            upload_backend="youtube",
            telegram_enabled=1,
            tg_bot_token="bad",
            tg_admin_chat_id=123,
        )
        expected_roles = _worker_roles(no_importer_flag=False)

        def fake_http(url: str, *, timeout_sec: float, headers=None):
            if url.endswith("/health"):
                return {"ok": True, "db": "ok"}
            if url.endswith("/v1/workers"):
                return {"workers": [{"role": role} for role in expected_roles]}
            raise AssertionError(url)

        with patch("scripts.doctor.shutil.disk_usage", return_value=DiskUsage(total=100 * 1024**3, used=60 * 1024**3, free=40 * 1024**3)):
            with patch("scripts.doctor._http_json", side_effect=fake_http):
                report = _run_production_smoke(env, timeout_sec=1.0)

        self.assertEqual(report["status"], "WARNING")
        self.assertEqual(report["exit_code"], 1)

    def test_smoke_fail_when_worker_missing(self) -> None:
        env = self._env()

        def fake_http(url: str, *, timeout_sec: float, headers=None):
            if url.endswith("/health"):
                return {"ok": True, "db": "ok"}
            if url.endswith("/v1/workers"):
                return {"workers": [{"role": "qa"}]}
            raise AssertionError(url)

        with patch("scripts.doctor.shutil.disk_usage", return_value=DiskUsage(total=100 * 1024**3, used=60 * 1024**3, free=40 * 1024**3)):
            with patch("scripts.doctor._http_json", side_effect=fake_http):
                report = _run_production_smoke(env, timeout_sec=1.0)

        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["exit_code"], 2)


if __name__ == "__main__":
    unittest.main()
