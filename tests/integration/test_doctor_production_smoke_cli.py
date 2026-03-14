from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class ProductionSmokeCliIntegrationTest(unittest.TestCase):
    def test_production_smoke_human_output(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                "scripts/doctor.py",
                "production-smoke",
                "--profile",
                "local",
                "--checks",
                "runner_bootstrap",
            ],
            cwd=Path(__file__).resolve().parents[2],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("Overall status: OK", proc.stdout)
        self.assertIn("[PASS] runner_bootstrap", proc.stdout)

    def test_production_smoke_json_stdout_and_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "smoke.json"
            proc = subprocess.run(
                [
                    sys.executable,
                    "scripts/doctor.py",
                    "production-smoke",
                    "--profile",
                    "local",
                    "--checks",
                    "runner_bootstrap",
                    "--json",
                    "--json-out",
                    str(out_path),
                ],
                cwd=Path(__file__).resolve().parents[2],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            stdout_payload = json.loads(proc.stdout)
            file_payload = json.loads(out_path.read_text(encoding="utf-8"))

        self.assertEqual(stdout_payload["schema_version"], "factory_production_smoke/1")
        self.assertEqual(stdout_payload["overall_status"], "OK")
        self.assertEqual(stdout_payload["exit_code"], 0)
        self.assertEqual(stdout_payload["summary"]["total_checks"], 1)
        self.assertEqual(stdout_payload["checks"][0]["check_id"], "runner_bootstrap")
        self.assertEqual(file_payload, stdout_payload)


if __name__ == "__main__":
    unittest.main()
