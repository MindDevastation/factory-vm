from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from scripts import doctor
from services.ops_health_smoke.formatters import render_human_report
from services.ops_health_smoke.runner import run_checks_with_error_capture, run_production_smoke


class TestDoctorProductionSmoke(unittest.TestCase):
    def test_smoke_json_schema(self) -> None:
        report = run_production_smoke(profile="prod")
        self.assertEqual(report["schema_version"], "factory_production_smoke/1")
        self.assertEqual(report["profile"], "prod")
        self.assertIn(report["overall_status"], {"OK", "WARNING", "FAIL"})
        self.assertIn("checks", report)
        self.assertEqual(report["summary"]["total_checks"], len(report["checks"]))
        first = report["checks"][0]
        self.assertEqual(first["check_id"], "runner_bootstrap")
        self.assertEqual(first["result"], "PASS")

    def test_checks_filter_and_runner_error(self) -> None:
        report = run_production_smoke(profile="local", selected_check_ids={"runner_bootstrap"})
        self.assertEqual(report["summary"]["total_checks"], 1)

        error_report = run_checks_with_error_capture(profile="local", selected_check_ids={"missing"})
        self.assertEqual(error_report["overall_status"], "RUNNER_ERROR")
        self.assertEqual(error_report["exit_code"], 3)

    def test_human_output_shape(self) -> None:
        report = run_production_smoke(profile="local")
        human = render_human_report(report)
        self.assertIn("Overall status:", human)
        self.assertIn("Profile: local", human)
        self.assertIn("[PASS] runner_bootstrap", human)
        self.assertIn("Operator hint: System ready", human)

    def test_doctor_cli_json_out_and_exit_code(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".json") as handle:
            argv = ["doctor.py", "production-smoke", "--json", "--json-out", handle.name, "--checks", "runner_bootstrap"]
            with patch("sys.argv", argv):
                with self.assertRaises(SystemExit) as cm:
                    doctor.main()
            self.assertEqual(cm.exception.code, 0)
            with open(handle.name, "r", encoding="utf-8") as fh:
                payload = json.loads(fh.read())
            self.assertEqual(payload["overall_status"], "OK")
            self.assertEqual(payload["summary"]["total_checks"], 1)

    def test_doctor_cli_human_warning_exit_code(self) -> None:
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
        with patch("scripts.doctor.run_checks_with_error_capture", return_value=fake_report):
            with patch("sys.argv", ["doctor.py", "production-smoke"]):
                with io.StringIO() as buf, redirect_stdout(buf):
                    with self.assertRaises(SystemExit) as cm:
                        doctor.main()
                    output = buf.getvalue()
        self.assertEqual(cm.exception.code, 1)
        self.assertIn("Warnings require attention", output)


if __name__ == "__main__":
    unittest.main()
