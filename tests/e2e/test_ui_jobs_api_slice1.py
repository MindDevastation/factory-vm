from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiJobsApiSlice1(unittest.TestCase):
    def test_create_get_update_draft_and_release_sync(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                self.assertIsNotNone(ch)
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            payload = {
                "channel_id": channel_id,
                "title": "My First",
                "description": "desc",
                "tags_csv": "one,two",
                "cover_name": "cover",
                "cover_ext": "png",
                "background_name": "bg",
                "background_ext": "jpg",
                "audio_ids_text": "001 015",
            }
            rc = client.post("/v1/ui/jobs", json=payload, headers=h)
            self.assertEqual(rc.status_code, 200)
            job_id = int(rc.json()["job_id"])

            rg = client.get(f"/v1/ui/jobs/{job_id}", headers=h)
            self.assertEqual(rg.status_code, 200)
            self.assertEqual(rg.json()["draft"]["title"], "My First")

            upd = dict(payload)
            upd["title"] = "Updated"
            upd["tags_csv"] = "x,y"
            ru = client.post(f"/v1/ui/jobs/{job_id}", json=upd, headers=h)
            self.assertEqual(ru.status_code, 200)

            conn2 = dbm.connect(env)
            try:
                job = dbm.get_job(conn2, job_id)
                self.assertIsNotNone(job)
                self.assertEqual(job["state"], "DRAFT")
                self.assertEqual(job["job_type"], "UI")
                self.assertEqual(job["release_title"], "Updated")
            finally:
                conn2.close()

    def test_project_immutable_on_update(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                rows = conn.execute("SELECT id FROM channels ORDER BY id ASC LIMIT 2").fetchall()
                self.assertGreaterEqual(len(rows), 2)
                ch1 = int(rows[0]["id"])
                ch2 = int(rows[1]["id"])
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            payload = {
                "channel_id": ch1,
                "title": "Immutable",
                "description": "",
                "tags_csv": "",
                "cover_name": "",
                "cover_ext": "",
                "background_name": "bg",
                "background_ext": "jpg",
                "audio_ids_text": "001",
            }
            rc = client.post("/v1/ui/jobs", json=payload, headers=h)
            self.assertEqual(rc.status_code, 200)
            job_id = int(rc.json()["job_id"])

            payload["channel_id"] = ch2
            ru = client.post(f"/v1/ui/jobs/{job_id}", json=payload, headers=h)
            self.assertEqual(ru.status_code, 409)


if __name__ == "__main__":
    unittest.main()
