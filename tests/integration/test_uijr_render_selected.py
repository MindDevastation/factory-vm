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


class TestUiJobsRenderSelected(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _create_draft_job(self, env: Env, title: str) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title=title,
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

    def test_auth_required(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            _, client = self._new_client()
            resp = client.post("/v1/ui/jobs/render_selected", json={"job_ids": ["1"]})
            self.assertEqual(resp.status_code, 401)

    def test_400_on_empty_list(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post("/v1/ui/jobs/render_selected", headers=h, json={"job_ids": []})

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "UIJ_INVALID_INPUT")

    def test_mixed_outcomes_and_summary(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            eligible_job_id = self._create_draft_job(env, "Eligible")
            noop_job_id = self._create_draft_job(env, "Noop")
            not_allowed_job_id = self._create_draft_job(env, "Not Allowed")
            missing_job_id = 999999

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, not_allowed_job_id, state="READY_FOR_RENDER", stage="FETCH")

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
                dbm.link_job_input(conn, noop_job_id, aid, "TRACK", 0)
            finally:
                conn.close()

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post(
                "/v1/ui/jobs/render_selected",
                headers=h,
                json={"job_ids": [str(eligible_job_id), str(noop_job_id), str(not_allowed_job_id), str(missing_job_id)]},
            )

            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(
                payload,
                {
                    "results": [
                        {"job_id": str(eligible_job_id), "enqueued": True},
                        {"job_id": str(noop_job_id), "enqueued": False, "message": "Already in progress"},
                        {
                            "job_id": str(not_allowed_job_id),
                            "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"},
                        },
                        {
                            "job_id": str(missing_job_id),
                            "error": {"code": "UIJ_JOB_NOT_FOUND", "message": "UI job not found"},
                        },
                    ],
                    "summary": {"requested": 4, "enqueued": 1, "noop": 1, "failed": 2},
                },
            )


if __name__ == "__main__":
    unittest.main()
