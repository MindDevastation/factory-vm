from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.disk_thresholds import DiskPressureLevel
from services.factory_api.ui_jobs_enqueue import UiRenderEnqueueResult, enqueue_ui_render_job

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiJobRetryEndpoint(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _create_ui_job(self, env) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title="Retry Endpoint",
                description="desc",
                tags_csv="a,b",
                cover_name="cover",
                cover_ext="png",
                background_name="bg",
                background_ext="png",
                audio_ids_text="1,2",
                job_type="UI",
            )
        finally:
            conn.close()

    def _prepare_failed_source_with_inputs(self, env) -> int:
        job_id = self._create_ui_job(env)
        conn = dbm.connect(env)
        try:
            draft = dbm.get_ui_job_draft(conn, job_id)
            assert draft is not None
            enqueue = enqueue_ui_render_job(
                conn,
                job_id=job_id,
                channel_id=int(draft["channel_id"]),
                tracks=[
                    {"file_id": "track-retry-1", "filename": "track-retry-1.wav"},
                    {"file_id": "track-retry-2", "filename": "track-retry-2.wav"},
                ],
                background_file_id="bg-retry-1",
                background_filename="bg-retry-1.png",
                cover_file_id="cover-retry-1",
                cover_filename="cover-retry-1.png",
            )
            self.assertTrue(enqueue.enqueued)
            dbm.update_job_state(conn, job_id, state="FAILED", stage="RENDER", error_reason="boom")
        finally:
            conn.close()
        return job_id

    def test_auth_required(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_ui_job(env)

            _, client = self._new_client()
            resp = client.post(f"/v1/ui/jobs/{job_id}/retry")

            self.assertEqual(resp.status_code, 401)

    def test_404_when_job_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post("/v1/ui/jobs/999999/retry", headers=h)

            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_JOB_NOT_FOUND")

    def test_409_when_source_not_failed(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            job_id = self._create_ui_job(env)

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{job_id}/retry", headers=h)

            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_RETRY_NOT_ALLOWED")

    def test_200_new_retry_created_with_expected_payload(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            source_job_id = self._prepare_failed_source_with_inputs(env)

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{source_job_id}/retry", headers=h)

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["source_job_id"], str(source_job_id))
            self.assertEqual(body["attempt_no"], 2)
            self.assertTrue(body["enqueued"])
            self.assertEqual(body["message"], "Retry enqueued")

            conn = dbm.connect(env)
            try:
                child = conn.execute(
                    "SELECT id, state, retry_of_job_id FROM jobs WHERE retry_of_job_id = ?",
                    (source_job_id,),
                ).fetchone()
            finally:
                conn.close()
            assert child is not None
            self.assertEqual(body["retry_job_id"], str(int(child["id"])))
            self.assertEqual(int(child["retry_of_job_id"]), source_job_id)
            self.assertEqual(str(child["state"]), "READY_FOR_RENDER")

    def test_200_noop_when_retry_child_already_exists(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            source_job_id = self._prepare_failed_source_with_inputs(env)

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            first = client.post(f"/v1/ui/jobs/{source_job_id}/retry", headers=h)
            second = client.post(f"/v1/ui/jobs/{source_job_id}/retry", headers=h)

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertTrue(first.json()["enqueued"])
            self.assertFalse(second.json()["enqueued"])
            self.assertEqual(second.json()["message"], "Retry already created")
            self.assertEqual(second.json()["retry_job_id"], first.json()["retry_job_id"])
            self.assertEqual(second.json()["attempt_no"], 2)

    def test_enqueue_failure_maps_to_500_and_leaves_no_orphan_child(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            source_job_id = self._prepare_failed_source_with_inputs(env)

            _, client = self._new_client()
            import services.ui_jobs.retry_service as retry_service

            orig_enqueue = retry_service._enqueue_ui_render_job_in_tx
            retry_service._enqueue_ui_render_job_in_tx = lambda *args, **kwargs: UiRenderEnqueueResult(
                enqueued=False,
                reason="forced_failure",
            )
            self.addCleanup(setattr, retry_service, "_enqueue_ui_render_job_in_tx", orig_enqueue)

            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{source_job_id}/retry", headers=h)

            self.assertEqual(resp.status_code, 500)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_RETRY_ENQUEUE_FAILED")

            conn = dbm.connect(env)
            try:
                child = conn.execute("SELECT id FROM jobs WHERE retry_of_job_id = ?", (source_job_id,)).fetchone()
                orphan_draft = conn.execute(
                    "SELECT job_id FROM ui_job_drafts WHERE job_id IN (SELECT id FROM jobs WHERE retry_of_job_id = ?)",
                    (source_job_id,),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNone(child)
            self.assertIsNone(orphan_draft)

    def test_503_when_disk_pressure_is_critical(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            source_job_id = self._prepare_failed_source_with_inputs(env)

            mod, client = self._new_client()
            mod.evaluate_disk_pressure_for_env = lambda **_kwargs: type("Snap", (), {
                "pressure": DiskPressureLevel.CRITICAL,
                "free_percent": 2.0,
                "free_gib": 5.0,
                "total_bytes": 100 * 1024**3,
                "used_bytes": 95 * 1024**3,
                "free_bytes": 5 * 1024**3,
                "checked_path": "/tmp",
                "resolved_mount_or_anchor": "/",
                "thresholds": type("Thresholds", (), {"fail_percent": 8.0, "fail_gib": 10.0})(),
            })()
            mod.emit_disk_pressure_event = lambda **_kwargs: None
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(f"/v1/ui/jobs/{source_job_id}/retry", headers=h)
            self.assertEqual(resp.status_code, 503)
            self.assertEqual(resp.json()["error"]["code"], "DISK_CRITICAL_WRITE_BLOCKED")



if __name__ == "__main__":
    unittest.main()
