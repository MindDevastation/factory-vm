from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import RenderValidationService
from tests._helpers import seed_minimal_db, temp_env


class TestPromptRuntimeAuthoritativeGateMf2(unittest.TestCase):
    def test_schema_table_columns_and_index_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("prompt_runtime_render_validation_ledger", tables)
                columns = {r["name"] for r in conn.execute("PRAGMA table_info(prompt_runtime_render_validation_ledger)").fetchall()}
                self.assertTrue(
                    {
                        "id",
                        "prompt_record_id",
                        "prompt_version_id",
                        "binding_fingerprint",
                        "render_result_hash",
                        "validation_status",
                        "validation_schema_version",
                        "validator_code",
                        "validated_at",
                        "invalid_reason_code",
                        "invalid_reason_detail",
                        "superseded_by_validation_id",
                        "created_at",
                    }.issubset(columns)
                )
                indexes = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
                self.assertIn("idx_prompt_runtime_render_validation_latest", indexes)
            finally:
                conn.close()

    def test_record_validation_writes_and_latest_exact_tuple_uses_validated_at_then_id(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = RenderValidationService(conn)
                service.record_validation(
                    prompt_record_id=1,
                    prompt_version_id=10,
                    binding_fingerprint="bind-a",
                    render_result_hash="render-a",
                    validation_status="failed",
                    validation_schema_version="v1",
                    validator_code="unit",
                    validated_at="2026-01-01T00:00:00Z",
                )
                older_tie = service.record_validation(
                    prompt_record_id=1,
                    prompt_version_id=10,
                    binding_fingerprint="bind-a",
                    render_result_hash="render-a",
                    validation_status="failed",
                    validation_schema_version="v1",
                    validator_code="unit",
                    validated_at="2026-01-02T00:00:00Z",
                )
                newer_tie = service.record_validation(
                    prompt_record_id=1,
                    prompt_version_id=10,
                    binding_fingerprint="bind-a",
                    render_result_hash="render-a",
                    validation_status="passed",
                    validation_schema_version="v1",
                    validator_code="unit",
                    validated_at="2026-01-02T00:00:00Z",
                )
                service.record_validation(
                    prompt_record_id=1,
                    prompt_version_id=10,
                    binding_fingerprint="bind-other",
                    render_result_hash="render-a",
                    validation_status="error",
                    validation_schema_version="v1",
                    validator_code="unit",
                    validated_at="2026-01-03T00:00:00Z",
                )
                latest = service.get_latest_validation(prompt_version_id=10, binding_fingerprint="bind-a", render_result_hash="render-a")
                self.assertIsNotNone(latest)
                self.assertEqual(latest["id"], newer_tie["id"])
                self.assertGreater(newer_tie["id"], older_tie["id"])
            finally:
                conn.close()

    def test_evaluate_trusted_missing_failed_error_superseded_and_superseded_by(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = RenderValidationService(conn)
                missing = service.evaluate(prompt_version_id=1, binding_fingerprint="missing", render_result_hash="hash")
                self.assertEqual(missing.verdict, "missing")
                self.assertFalse(missing.trusted)
                self.assertEqual(missing.failure_reason_code, "missing_render_validation_authority")

                cases = [
                    ("passed", None, True, "trusted", None),
                    ("failed", None, False, "untrusted", "render_validation_failed"),
                    ("error", None, False, "untrusted", "render_validation_error"),
                    ("superseded", None, False, "untrusted", "render_validation_superseded"),
                    ("passed", 99, False, "untrusted", "render_validation_superseded"),
                ]
                for idx, (status, superseded_by, trusted, verdict, reason) in enumerate(cases, start=1):
                    binding = f"bind-{idx}"
                    service.record_validation(
                        prompt_record_id=1,
                        prompt_version_id=1,
                        binding_fingerprint=binding,
                        render_result_hash="hash",
                        validation_status=status,
                        validation_schema_version="v1",
                        validator_code="unit",
                        validated_at=f"2026-01-0{idx}T00:00:00Z",
                        superseded_by_validation_id=superseded_by,
                    )
                    result = service.evaluate(prompt_version_id=1, binding_fingerprint=binding, render_result_hash="hash")
                    self.assertEqual(result.trusted, trusted)
                    self.assertEqual(result.verdict, verdict)
                    self.assertEqual(result.failure_reason_code, reason)
            finally:
                conn.close()

    def test_invalid_status_rejected_and_invalid_reason_detail_secret_safe(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = RenderValidationService(conn)
                with self.assertRaises(ValueError):
                    service.record_validation(
                        prompt_record_id=1,
                        prompt_version_id=1,
                        binding_fingerprint="bind",
                        render_result_hash="hash",
                        validation_status="ok",
                        validation_schema_version="v1",
                        validator_code="unit",
                    )
                row = service.record_validation(
                    prompt_record_id=1,
                    prompt_version_id=1,
                    binding_fingerprint="bind",
                    render_result_hash="hash",
                    validation_status="failed",
                    validation_schema_version="v1",
                    validator_code="unit",
                    invalid_reason_detail="password=hunter2",
                )
                self.assertEqual(row["invalid_reason_detail"], "[redacted]")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
