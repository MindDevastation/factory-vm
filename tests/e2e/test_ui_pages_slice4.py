from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPagesSlice4(unittest.TestCase):
    def test_pages_and_validation(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(ch["id"]),
                    title="T",
                    description="",
                    tags_csv="",
                    cover_name="",
                    cover_ext="",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/ui/jobs/create", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Create Job", r.text)
            self.assertIn("<form", r.text)
            self.assertIn('name="channel_id"', r.text)
            self.assertIn('name="title"', r.text)
            self.assertIn('name="audio_ids_text"', r.text)
            self.assertIn('name="background_name"', r.text)
            self.assertIn('name="background_ext"', r.text)

            r = client.get("/", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn('action="/ui/jobs/render_all"', r.text)
            self.assertIn('method="post"', r.text)
            self.assertIn('id="channel-add-btn"', r.text)
            self.assertIn('id="channels-table"', r.text)

            r = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Edit Job", r.text)

            r = client.post(
                "/ui/jobs/create",
                headers=h,
                data={
                    "channel_id": int(ch["id"]),
                    "title": "",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "",
                    "background_ext": "",
                    "audio_ids_text": "",
                },
            )
            self.assertEqual(r.status_code, 422)
            self.assertIn("title is required", r.text)
            self.assertIn("audio ids are required", r.text)

            r = client.post(
                "/ui/jobs/create",
                headers=h,
                data={
                    "channel_id": "not-a-number",
                    "title": "Valid title",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "bg",
                    "background_ext": "jpg",
                    "audio_ids_text": "001",
                },
            )
            self.assertNotEqual(r.status_code, 500)
            self.assertEqual(r.status_code, 422)
            self.assertIn("project is required", r.text)

            r = client.post(
                "/ui/jobs/create",
                headers=h,
                data={
                    "channel_id": 999999,
                    "title": "Valid title",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "bg",
                    "background_ext": "jpg",
                    "audio_ids_text": "001",
                },
            )
            self.assertEqual(r.status_code, 422)
            self.assertIn("project is invalid", r.text)

            conn2 = dbm.connect(env)
            try:
                dbm.update_job_state(conn2, job_id, state="READY_FOR_RENDER", stage="FETCH")
            finally:
                conn2.close()

            r = client.post(
                f"/ui/jobs/{job_id}/edit",
                headers=h,
                data={
                    "channel_id": int(ch["id"]),
                    "title": "Z",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "bg",
                    "background_ext": "jpg",
                    "audio_ids_text": "001",
                },
            )
            self.assertEqual(r.status_code, 409)


if __name__ == "__main__":
    unittest.main()
