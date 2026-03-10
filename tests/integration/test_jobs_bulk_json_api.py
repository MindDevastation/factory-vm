from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.oauth_tokens import oauth_token_path

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class _PreflightOk:
    def __init__(self):
        self.ok = True
        self.resolved = {
            "tracks": [{"file_id": "track1", "filename": "track1.wav"}],
            "background_file_id": "bg1",
            "background_filename": "bg1.png",
            "cover_file_id": "cover1",
            "cover_filename": "cover1.png",
        }


class TestJobsBulkJsonApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _create_ui_draft(self, env: Env, title: str) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title=title,
                description="",
                tags_csv="a,b",
                cover_name="cover",
                cover_ext="png",
                background_name="bg",
                background_ext="jpg",
                audio_ids_text="1",
                job_type="UI",
            )
        finally:
            conn.close()

    def _create_item(self, channel_id: int, title: str = "Bulk") -> dict[str, object]:
        return {
            "channel_id": channel_id,
            "title": title,
            "description": "desc",
            "tags_csv": "one,two",
            "cover_name": "cover",
            "cover_ext": "png",
            "background_name": "bg",
            "background_ext": "jpg",
            "audio_ids_text": "1",
        }

    def test_preview_all_modes_and_examples(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            job_id = self._create_ui_draft(env, "Existing")

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            a_preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(a_preview.status_code, 200)
            self.assertEqual(a_preview.json(), {"mode": "create_draft_jobs", "summary": {"requested": 1, "valid": 1, "failed": 0}, "results": [{"index": 0, "valid": True}]})

            b_preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(b_preview.status_code, 200)
            self.assertEqual(b_preview.json()["mode"], "create_and_enqueue")

            c_preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "enqueue_existing_jobs", "items": [{"job_id": job_id}]},
            )
            self.assertEqual(c_preview.status_code, 200)
            self.assertEqual(c_preview.json(), {"mode": "enqueue_existing_jobs", "summary": {"requested": 1, "valid": 1, "failed": 0}, "results": [{"job_id": str(job_id), "enqueued": True}]})

    def test_execute_modes_atomicity_and_non_atomic_behavior(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            existing_ok = self._create_ui_draft(env, "enqueue ok")
            existing_bad = self._create_ui_draft(env, "enqueue bad")
            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, existing_bad, state="READY_FOR_RENDER", stage="FETCH")
            finally:
                conn.close()

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            conn = dbm.connect(env)
            try:
                before_a = dbm.list_jobs(conn, limit=500)
            finally:
                conn.close()
            a_execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id), {"channel_id": channel_id}]},
            )
            self.assertEqual(a_execute.status_code, 200)
            self.assertEqual(a_execute.json()["summary"]["created"], 0)
            conn = dbm.connect(env)
            try:
                after_a = dbm.list_jobs(conn, limit=500)
            finally:
                conn.close()
            self.assertEqual(len(before_a), len(after_a))

            b_execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id, title="B1")]},
            )
            self.assertEqual(b_execute.status_code, 200)
            b_payload = b_execute.json()
            self.assertEqual(b_payload["mode"], "create_and_enqueue")
            self.assertEqual(b_payload["summary"], {"requested": 1, "created": 1, "enqueued": 1, "noop": 0, "failed": 0})
            created_job_id = int(b_payload["results"][0]["job_id"])
            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, created_job_id)
                self.assertEqual(job["state"], "READY_FOR_RENDER")
            finally:
                conn.close()

            b_fail = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id), {"channel_id": channel_id}]},
            )
            self.assertEqual(b_fail.status_code, 200)
            self.assertEqual(b_fail.json()["summary"]["created"], 0)

            c_execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "enqueue_existing_jobs", "items": [{"job_id": existing_ok}, {"job_id": existing_bad}]},
            )
            self.assertEqual(c_execute.status_code, 200)
            self.assertEqual(c_execute.json(), {
                "mode": "enqueue_existing_jobs",
                "summary": {"requested": 2, "enqueued": 1, "noop": 0, "failed": 1},
                "results": [
                    {"job_id": str(existing_ok), "enqueued": True},
                    {"job_id": str(existing_bad), "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"}},
                ],
            })

    def test_execute_mode_c_runtime_exception_converted_to_item_error(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            ok_job = self._create_ui_draft(env, "mode c ok")
            boom_job = self._create_ui_draft(env, "mode c boom")

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()

            original_render_selected = mod._render_selected_item

            def _raise_for_specific_job(job_id_text: str):
                if int(job_id_text) == boom_job:
                    raise RuntimeError("unexpected runtime crash")
                return original_render_selected(job_id_text)

            mod._render_selected_item = _raise_for_specific_job

            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "enqueue_existing_jobs", "items": [{"job_id": ok_job}, {"job_id": boom_job}]},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {
                "mode": "enqueue_existing_jobs",
                "summary": {"requested": 2, "enqueued": 1, "noop": 0, "failed": 1},
                "results": [
                    {"job_id": str(ok_job), "enqueued": True},
                    {"job_id": str(boom_job), "error": {"code": "UIJ_INTERNAL", "message": "Internal error"}},
                ],
            })

    def test_execute_mode_b_runtime_exception_converted_to_enqueue_item_error(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            mod._render_selected_item = lambda _job_id_text: (_ for _ in ()).throw(RuntimeError("enqueue exploded"))

            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id, title="B crash")]},
            )
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["mode"], "create_and_enqueue")
            self.assertEqual(payload["summary"], {"requested": 1, "created": 1, "enqueued": 0, "noop": 0, "failed": 1})
            self.assertEqual(payload["results"][0]["enqueue"], {"job_id": payload["results"][0]["job_id"], "error": {"code": "UIJ_INTERNAL", "message": "Internal error"}})

            created_job_id = int(payload["results"][0]["job_id"])
            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, created_job_id)
                self.assertIsNotNone(job)
                self.assertEqual(job["state"], "DRAFT")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
