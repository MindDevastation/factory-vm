from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.ops.recovery import insert_recovery_audit
from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class OpsRecoveryReadonlyApiTests(unittest.TestCase):
    def test_legacy_recovery_audit_table_not_rewritten_during_migrate(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    CREATE TABLE recovery_action_audit (
                        id INTEGER PRIMARY KEY,
                        job_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        requested_by TEXT,
                        request_payload_json TEXT NOT NULL,
                        result_payload_json TEXT NOT NULL,
                        ok INTEGER NOT NULL,
                        error_code TEXT,
                        created_at REAL NOT NULL
                    )
                    """
                )
                dbm.migrate(conn)
                legacy_rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'recovery_action_audit__legacy%'"
                ).fetchall()
                cols = conn.execute("PRAGMA table_info(recovery_action_audit)").fetchall()
            finally:
                conn.close()

            self.assertEqual(legacy_rows, [])
            names = {str(col["name"]) for col in cols}
            self.assertIn("action", names)
            self.assertNotIn("action_name", names)

    def test_recovery_listing_filters_and_detail_shape(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            failed_job = insert_release_and_job(env, state="FAILED", stage="RENDER", channel_slug="darkwood-reverie")
            published_job = insert_release_and_job(env, state="PUBLISHED", stage="APPROVAL", channel_slug="channel-b")

            conn = dbm.connect(env)
            try:
                ts = dbm.now_ts()
                conn.execute(
                    "UPDATE jobs SET delete_mp4_at = ?, progress_updated_at = ?, error_reason = ? WHERE id = ?",
                    (ts - 3.0, ts - 600.0, "render crash", published_job),
                )
                conn.execute(
                    "UPDATE jobs SET progress_updated_at = ?, error_reason = ? WHERE id = ?",
                    (ts - 10.0, "ffmpeg failed", failed_job),
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/ops/recovery/jobs?category=failed&actionability=has_actions&q=ffmpeg", headers=h)
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertIn("summary", payload)
            self.assertEqual(payload["total"], 1)
            item = payload["items"][0]
            self.assertEqual(item["job_id"], failed_job)
            self.assertIn("available_actions", item)
            self.assertIn("category_reasons", item)

            detail = client.get(f"/v1/ops/recovery/jobs/{published_job}", headers=h)
            self.assertEqual(detail.status_code, 200)
            detail_item = detail.json()["item"]
            self.assertIn("cleanup", detail_item)
            self.assertIn("artifacts", detail_item)
            self.assertIn("recent_audit_entries", detail_item)
            self.assertEqual(detail_item["allowed_stage_tokens"], [])
            self.assertIn("allowed_stage_tokens_fallback", detail_item)

            failed_detail = client.get(f"/v1/ops/recovery/jobs/{failed_job}", headers=h)
            self.assertEqual(failed_detail.status_code, 200)
            failed_item = failed_detail.json()["item"]
            self.assertTrue(isinstance(failed_item.get("allowed_stage_tokens"), list))
            self.assertTrue(failed_item["allowed_stage_tokens"])
            self.assertIn("worker_stale", failed_item["worker_context"])

    def test_recovery_audit_migration_columns_exist(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                cols = conn.execute("PRAGMA table_info(recovery_action_audit)").fetchall()
                idx_rows = conn.execute("PRAGMA index_list(recovery_action_audit)").fetchall()
            finally:
                conn.close()
            names = {str(col["name"]) for col in cols}
            expected_columns = {
                "id",
                "job_id",
                "action_name",
                "risk_level",
                "requested_by",
                "requested_at",
                "preview_allowed",
                "execute_attempted",
                "result_status",
                "result_code",
                "message",
                "state_before",
                "state_after",
                "details_json",
                "action",
                "phase",
                "request_payload_json",
                "result_payload_json",
                "ok",
                "error_code",
                "created_at",
            }
            self.assertTrue(expected_columns.issubset(names))

            index_names = {str(row["name"]) for row in idx_rows}
            self.assertIn("idx_recovery_audit_job_id_requested_at", index_names)
            self.assertIn("idx_recovery_audit_action_name_requested_at", index_names)


    def test_recovery_audit_scaffold_keeps_legacy_write_columns_and_insert_path(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            job_id = insert_release_and_job(env, state="FAILED", stage="RENDER", channel_slug="darkwood-reverie")

            conn = dbm.connect(env)
            try:
                cols = conn.execute("PRAGMA table_info(recovery_action_audit)").fetchall()
                names = {str(col["name"]) for col in cols}
                legacy_write_columns = {
                    "job_id",
                    "action",
                    "phase",
                    "requested_by",
                    "request_payload_json",
                    "result_payload_json",
                    "ok",
                    "error_code",
                    "created_at",
                }
                self.assertTrue(legacy_write_columns.issubset(names))

                insert_recovery_audit(
                    conn,
                    job_id=job_id,
                    action="retry_failed",
                    phase="execute",
                    requested_by="tester",
                    request_payload={"confirm": True},
                    result_payload={"ok": True},
                    ok=True,
                )
                row = conn.execute(
                    "SELECT action, phase, ok FROM recovery_action_audit WHERE job_id = ? ORDER BY id DESC LIMIT 1",
                    (job_id,),
                ).fetchone()
            finally:
                conn.close()

            self.assertIsNotNone(row)
            self.assertEqual(row["action"], "retry_failed")
            self.assertEqual(row["phase"], "execute")
            self.assertEqual(int(row["ok"]), 1)


    def test_recovery_detail_hides_placeholder_scaffold_legacy_audit_rows(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            failed_job = insert_release_and_job(env, state="FAILED", stage="RENDER", channel_slug="darkwood-reverie")

            conn = dbm.connect(env)
            try:
                insert_recovery_audit(
                    conn,
                    job_id=failed_job,
                    action="retry_failed",
                    phase="execute",
                    requested_by="tester",
                    request_payload={"confirm": True},
                    result_payload={"ok": True},
                    ok=True,
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            detail = client.get(f"/v1/ops/recovery/jobs/{failed_job}", headers=h)
            self.assertEqual(detail.status_code, 200)
            audits = detail.json()["item"]["recent_audit_entries"]
            self.assertTrue(audits)
            self.assertEqual(audits[0]["action_name"], "retry_failed")
            self.assertEqual(audits[0]["result_status"], "success")

    def test_recovery_detail_returns_empty_recent_audit_for_legacy_schema(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute("DROP TABLE recovery_action_audit")
                conn.execute(
                    """
                    CREATE TABLE recovery_action_audit (
                        id INTEGER PRIMARY KEY,
                        job_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        requested_by TEXT,
                        request_payload_json TEXT NOT NULL,
                        result_payload_json TEXT NOT NULL,
                        ok INTEGER NOT NULL,
                        error_code TEXT,
                        created_at REAL NOT NULL
                    )
                    """
                )
            finally:
                conn.close()

            failed_job = insert_release_and_job(env, state="FAILED", stage="RENDER", channel_slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            detail = client.get(f"/v1/ops/recovery/jobs/{failed_job}", headers=h)
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.json()["item"]["recent_audit_entries"], [])


if __name__ == "__main__":
    unittest.main()
