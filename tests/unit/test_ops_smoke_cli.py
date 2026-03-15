from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import ops_smoke


class TestOpsSmokeCli(unittest.TestCase):
    def test_warning_scenario_keeps_exit_code_and_operational_hint(self) -> None:
        fake_report = {
            "schema_version": "factory_production_smoke/1",
            "generated_at": "2025-01-01T00:00:00+00:00",
            "hostname": "localhost",
            "profile": "prod",
            "overall_status": "WARNING",
            "exit_code": 1,
            "duration_ms": 4,
            "summary": {"total_checks": 1, "pass_count": 0, "warn_count": 1, "fail_count": 0, "skip_count": 0},
            "checks": [
                {
                    "check_id": "stub",
                    "title": "Stub",
                    "category": "framework",
                    "severity": "warning",
                    "result": "WARN",
                    "message": "stub warning",
                    "details": {},
                }
            ],
        }
        with patch("scripts.ops_smoke.run_checks_with_error_capture", return_value=fake_report):
            with patch("sys.argv", ["ops_smoke.py", "--scenario", "post-deploy", "--profile", "prod"]):
                with io.StringIO() as buf, redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        ops_smoke.main()
                    output = buf.getvalue()

        self.assertEqual(cm.exception.code, 1)
        self.assertIn("OPERATIONAL WARNING", output)
        self.assertIn("sop/when_smoke_fails.md", output)

    def test_json_out_written_for_scenario_wrapper(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json") as handle:
            argv = [
                "ops_smoke.py",
                "--scenario",
                "pre-batch-run",
                "--profile",
                "local",
                "--json",
                "--json-out",
                handle.name,
                "--checks",
                "runner_bootstrap",
            ]
            with patch("sys.argv", argv):
                with self.assertRaises(SystemExit) as cm:
                    ops_smoke.main()
            self.assertEqual(cm.exception.code, 0)
            with open(handle.name, "r", encoding="utf-8") as fh:
                payload = json.loads(fh.read())
        self.assertEqual(payload["schema_version"], "factory_production_smoke/1")
        self.assertEqual(payload["summary"]["total_checks"], 1)

    def test_json_mode_stdout_is_pure_json_and_preserves_exit_code(self) -> None:
        fake_report = {
            "schema_version": "factory_production_smoke/1",
            "generated_at": "2025-01-01T00:00:00+00:00",
            "hostname": "localhost",
            "profile": "prod",
            "overall_status": "WARNING",
            "exit_code": 1,
            "duration_ms": 5,
            "summary": {"total_checks": 1, "pass_count": 0, "warn_count": 1, "fail_count": 0, "skip_count": 0},
            "checks": [
                {
                    "check_id": "stub",
                    "title": "Stub",
                    "category": "framework",
                    "severity": "warning",
                    "result": "WARN",
                    "message": "stub warning",
                    "details": {},
                }
            ],
        }
        argv = ["ops_smoke.py", "--scenario", "post-reboot", "--profile", "prod", "--json"]
        with patch("scripts.ops_smoke.run_checks_with_error_capture", return_value=fake_report):
            with patch("sys.argv", argv):
                with io.StringIO() as buf, redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        ops_smoke.main()
                    stdout_text = buf.getvalue().strip()

        self.assertEqual(cm.exception.code, 1)
        parsed = json.loads(stdout_text)
        self.assertEqual(parsed["schema_version"], "factory_production_smoke/1")
        self.assertNotIn("OPERATIONAL", stdout_text)


if __name__ == "__main__":
    unittest.main()
