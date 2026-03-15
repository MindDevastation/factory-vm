from __future__ import annotations

import importlib
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestRecoveryConsoleApi(unittest.TestCase):
    def _create_ui_job(self, conn, *, channel_slug: str = "darkwood-reverie", title: str, state: str, stage: str) -> int:
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch
        job_id = dbm.create_ui_job_draft(
            conn,
            channel_id=int(ch["id"]),
            title=title,
            description="",
            tags_csv="",
            cover_name="cover",
            cover_ext="png",
            background_name="bg",
            background_ext="png",
            audio_ids_text="a1",
        )
        dbm.update_job_state(conn, job_id, state=state, stage=stage, error_reason="")
        return job_id

    def test_recovery_jobs_summary_and_filters(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                failed_id = self._create_ui_job(conn, title="Failed", state="FAILED", stage="RENDER")
                stale_id = self._create_ui_job(conn, title="Stale", state="RENDERING", stage="RENDER")
                conn.execute(
                    "UPDATE jobs SET locked_by = ?, locked_at = ?, error_reason = ? WHERE id = ?",
                    ("worker-x", dbm.now_ts() - (env.job_lock_ttl_sec + 5), "", stale_id),
                )
                published_id = self._create_ui_job(conn, title="Published", state="PUBLISHED", stage="APPROVAL")
                conn.execute(
                    "UPDATE jobs SET delete_mp4_at = ?, error_reason = ? WHERE id = ?",
                    (dbm.now_ts() + 10, "artifact missing", published_id),
                )
                draft_id = self._create_ui_job(conn, title="Draft", state="DRAFT", stage="DRAFT")
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/v1/ops/recovery/jobs", headers=h)
            self.assertEqual(r.status_code, 200)
            body = r.json()
            self.assertGreaterEqual(body["summary"]["failed"], 1)
            self.assertGreaterEqual(body["summary"]["stale_or_stuck"], 1)
            self.assertGreaterEqual(body["summary"]["cleanup_pending"], 1)
            self.assertGreaterEqual(body["summary"]["restartable"], 1)

            by_id = {int(item["id"]): item for item in body["jobs"]}
            self.assertTrue(by_id[failed_id]["actions"]["retryable"])
            self.assertTrue(by_id[stale_id]["actions"]["reclaimable"])
            self.assertTrue(by_id[published_id]["actions"]["cleanupable"])
            self.assertTrue(by_id[draft_id]["actions"]["restartable"])

            r = client.get("/v1/ops/recovery/jobs?actionability=reclaimable", headers=h)
            self.assertEqual(r.status_code, 200)
            filtered_ids = {int(item["id"]) for item in r.json()["jobs"]}
            self.assertIn(stale_id, filtered_ids)
            self.assertNotIn(failed_id, filtered_ids)

    def test_recovery_actions_confirm_and_audit(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                unsafe_job_id = self._create_ui_job(conn, title="Stale", state="RENDERING", stage="RENDER")
                cleanupable_job_id = self._create_ui_job(conn, title="Failed", state="FAILED", stage="RENDER")
                conn.execute(
                    "UPDATE jobs SET locked_by = ?, locked_at = ? WHERE id = ?",
                    ("worker-y", dbm.now_ts() - (env.job_lock_ttl_sec + 7), unsafe_job_id),
                )
            finally:
                conn.close()

            unsafe_workspace = Path(env.storage_root) / "workspace" / f"job_{unsafe_job_id}"
            unsafe_workspace.mkdir(parents=True, exist_ok=True)
            (unsafe_workspace / "tmp.txt").write_text("x", encoding="utf-8")

            cleanupable_workspace = Path(env.storage_root) / "workspace" / f"job_{cleanupable_job_id}"
            cleanupable_workspace.mkdir(parents=True, exist_ok=True)
            (cleanupable_workspace / "tmp.txt").write_text("x", encoding="utf-8")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.post(f"/v1/ops/recovery/jobs/{unsafe_job_id}/cleanup", headers=h, json={"confirm": False, "reason": "nope"})
            self.assertEqual(r.status_code, 409)

            r = client.post(
                f"/v1/ops/recovery/jobs/{unsafe_job_id}/cleanup",
                headers=h,
                json={"confirm": True, "reason": "cleanup files"},
            )
            self.assertEqual(r.status_code, 409)
            self.assertTrue(unsafe_workspace.exists())

            r = client.post(
                f"/v1/ops/recovery/jobs/{cleanupable_job_id}/cleanup",
                headers=h,
                json={"confirm": True, "reason": "cleanup files"},
            )
            self.assertEqual(r.status_code, 200)
            self.assertFalse(cleanupable_workspace.exists())

            r = client.post(
                f"/v1/ops/recovery/jobs/{unsafe_job_id}/reclaim",
                headers=h,
                json={"confirm": True, "reason": "manual stale reclaim"},
            )
            self.assertEqual(r.status_code, 200)

            r = client.get("/v1/ops/recovery/audit?limit=10", headers=h)
            self.assertEqual(r.status_code, 200)
            items = r.json()["items"]
            self.assertTrue(any(item.get("action") == "reclaim" for item in items))
            cleanup_items = [item for item in items if item.get("action") == "cleanup"]
            self.assertTrue(any(item.get("result") == "ok" for item in cleanup_items))
            self.assertTrue(any(item.get("result") == "rejected" for item in cleanup_items))


if __name__ == "__main__":
    unittest.main()
