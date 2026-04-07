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


    def test_playlist_builder_draft_create_relaxes_audio_and_background_only_for_builder_flow(self) -> None:
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

            manual_resp = client.post(
                "/v1/ui/jobs",
                json={
                    "channel_id": channel_id,
                    "title": "Manual strict",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "",
                    "background_ext": "",
                    "audio_ids_text": "",
                },
                headers=h,
            )
            self.assertEqual(manual_resp.status_code, 422)
            manual_errors = manual_resp.json()["detail"]["field_errors"]
            self.assertIn("audio", manual_errors)
            self.assertIn("background", manual_errors)

            builder_resp = client.post(
                "/v1/ui/jobs/playlist-builder-draft",
                json={
                    "channel_id": channel_id,
                    "title": "Builder draft",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "",
                    "background_ext": "",
                },
                headers=h,
            )
            self.assertEqual(builder_resp.status_code, 200)
            job_id = int(builder_resp.json()["job_id"])
            self.assertTrue(int(builder_resp.json()["release_id"]) > 0)

            conn2 = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn2, job_id)
                self.assertIsNotNone(draft)
                self.assertEqual(str(draft["audio_ids_text"]), "")
                self.assertEqual(str(draft["background_name"]), "")
                self.assertEqual(str(draft["background_ext"]), "")
            finally:
                conn2.close()

    def test_create_form_rolls_back_when_preflight_fails(self) -> None:
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

            class _PreflightFail:
                ok = False
                field_errors = {"audio": ["audio id 999 matches=0"]}
                resolved = {}

            mod.run_preflight_for_job = lambda _conn, _env, _job_id: _PreflightFail()

            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            form = {
                "channel_id": str(channel_id),
                "title": "Draft should not persist",
                "description": "",
                "tags_csv": "",
                "cover_name": "",
                "cover_ext": "",
                "background_name": "missing-bg",
                "background_ext": "jpg",
                "audio_ids_text": "999",
            }
            resp = client.post(
                "/ui/jobs/create",
                headers={**h, "Content-Type": "application/x-www-form-urlencoded"},
                data=form,
            )
            self.assertEqual(resp.status_code, 422)
            self.assertIn("audio id 999 matches=0", resp.text)

            conn2 = dbm.connect(env)
            try:
                total = int(conn2.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(total, 0)
            finally:
                conn2.close()

    def test_create_api_blocks_persist_when_background_preflight_fails(self) -> None:
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

            class _BackgroundFail:
                ok = False
                field_errors = {"background": ["background 'missing-bg.jpg' matches=0"]}
                resolved = {}

            mod.run_preflight_for_job = lambda _conn, _env, _job_id, _drive=None: _BackgroundFail()

            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)
            payload = {
                "channel_id": channel_id,
                "title": "Background gate",
                "description": "",
                "tags_csv": "",
                "cover_name": "",
                "cover_ext": "",
                "background_name": "missing-bg",
                "background_ext": "jpg",
                "audio_ids_text": "001",
            }
            resp = client.post("/v1/ui/jobs", json=payload, headers=h)
            self.assertEqual(resp.status_code, 422)
            self.assertIn("background", resp.json()["detail"]["field_errors"])

            conn2 = dbm.connect(env)
            try:
                total = int(conn2.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(total, 0)
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

    def test_ext_validation_rejects_unsupported_values(self) -> None:
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
                "title": "Bad ext",
                "description": "",
                "tags_csv": "",
                "cover_name": "cover",
                "cover_ext": "gif",
                "background_name": "bg",
                "background_ext": "bmp",
                "audio_ids_text": "001",
            }
            resp = client.post("/v1/ui/jobs", json=payload, headers=h)
            self.assertEqual(resp.status_code, 422)
            errors = resp.json()["detail"]["field_errors"]
            self.assertIn("cover", errors)
            self.assertIn("background", errors)


if __name__ == "__main__":
    unittest.main()
