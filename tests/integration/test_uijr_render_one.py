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


class TestUiJobsRenderOne(unittest.TestCase):
    def _create_draft_job(self, env: Env) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title="UI Draft",
                description="",
                tags_csv="one,two",
                cover_name="cover",
                cover_ext="png",
                background_name="bg",
                background_ext="jpg",
                audio_ids_text="001",
                job_type="UI",
            )
        finally:
            conn.close()

    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def test_auth_required(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            job_id = self._create_draft_job(env)

            _, client = self._new_client()
            resp = client.post(f"/v1/ui/jobs/{job_id}/render")
            self.assertEqual(resp.status_code, 401)

    def test_404_when_job_missing(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post("/v1/ui/jobs/999999/render", headers=h)

            self.assertEqual(resp.status_code, 404)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_JOB_NOT_FOUND")

    def test_409_when_job_not_draft(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            job_id = self._create_draft_job(env)

            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, job_id, state="READY_FOR_RENDER", stage="FETCH")
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/ui/jobs/{job_id}/render", headers=h)

            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_RENDER_NOT_ALLOWED")

    def test_200_enqueued_true_for_draft_without_inputs(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            job_id = self._create_draft_job(env)

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(f"/v1/ui/jobs/{job_id}/render", headers=h)

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"job_id": str(job_id), "enqueued": True, "message": "Render enqueued"})

    def test_200_enqueued_false_when_inputs_exist(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            job_id = self._create_draft_job(env)

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                aid = dbm.create_asset(
                    conn,
                    channel_id=int(ch["id"]),
                    kind="AUDIO",
                    origin="LOCAL",
                    origin_id="local_track",
                    name="local.wav",
                    path="/tmp/local.wav",
                )
                dbm.link_job_input(conn, job_id, aid, "TRACK", 0)
            finally:
                conn.close()

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(f"/v1/ui/jobs/{job_id}/render", headers=h)

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"job_id": str(job_id), "enqueued": False, "message": "Already in progress"})


if __name__ == "__main__":
    unittest.main()
