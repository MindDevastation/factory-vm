from __future__ import annotations

import importlib
import json
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import logs_path, qa_path

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job, basic_auth_header


class TestApiMoreEndpoints(unittest.TestCase):
    def test_health_workers_logs_qa(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            # seed a worker heartbeat
            conn = dbm.connect(env)
            try:
                dbm.touch_worker(conn, worker_id="orchestrator:1", role="orchestrator", pid=1, hostname="h", details={"x": 1})
            finally:
                conn.close()

            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")

            # Create job log + qa file
            logs_path(env, job_id).parent.mkdir(parents=True, exist_ok=True)
            logs_path(env, job_id).write_text("line1\nline2\n", encoding="utf-8")

            qa_path(env, job_id).parent.mkdir(parents=True, exist_ok=True)
            qa_path(env, job_id).write_text(json.dumps({"hard_ok": True}), encoding="utf-8")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/health")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.json().get("ok"))

            rw = client.get("/v1/workers", headers=h)
            self.assertEqual(rw.status_code, 200)
            self.assertTrue(rw.json()["workers"])

            rl = client.get(f"/v1/jobs/{job_id}/logs?tail=1", headers=h)
            self.assertEqual(rl.status_code, 200)
            self.assertIn("line2", rl.text)

            rq = client.get(f"/v1/jobs/{job_id}/qa", headers=h)
            self.assertEqual(rq.status_code, 200)
            self.assertEqual(rq.json()["qa"]["hard_ok"], True)

            rj = client.get(f"/v1/jobs/{job_id}", headers=h)
            self.assertEqual(rj.status_code, 200)
            self.assertEqual(int(rj.json()["job"]["id"]), job_id)

    def test_channels_requires_auth_and_returns_schema(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            insert_release_and_job(env, channel_slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.get("/v1/channels")
            self.assertIn(unauthorized.status_code, (401, 403))

            authorized = client.get("/v1/channels", headers=h)
            self.assertEqual(authorized.status_code, 200)
            channels = authorized.json()
            self.assertIsInstance(channels, list)
            self.assertGreater(len(channels), 0)
            for item in channels:
                self.assertIsInstance(item, dict)
                self.assertIn("id", item)
                self.assertIn("slug", item)
                self.assertIn("display_name", item)

            display_names = [str(item["display_name"]) for item in channels]
            self.assertEqual(display_names, sorted(display_names))


    def test_channels_export_yaml_requires_auth_and_contains_slug_display_name(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.get("/v1/channels/export/yaml")
            self.assertIn(unauthorized.status_code, (401, 403))

            authorized = client.get("/v1/channels/export/yaml", headers=h)
            self.assertEqual(authorized.status_code, 200)
            self.assertIn("text/plain", authorized.headers.get("content-type", ""))
            body = authorized.text

            self.assertIn("channels:", body)
            self.assertIn('slug: darkwood-reverie', body)
            self.assertIn('display_name: Darkwood Reverie', body)
            self.assertNotIn('yt_token_json_path', body)
            self.assertNotIn('yt_client_secret_json_path', body)


    def test_create_channel_endpoint(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.post(
                "/v1/channels",
                json={"slug": "new-channel", "display_name": "New Channel"},
            )
            self.assertIn(unauthorized.status_code, (401, 403))

            invalid_slug = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "bad_slug", "display_name": "Valid Name"},
            )
            self.assertEqual(invalid_slug.status_code, 422)

            invalid_display_name = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "good-slug", "display_name": "   "},
            )
            self.assertEqual(invalid_display_name.status_code, 422)

            created = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "new-channel", "display_name": "New Channel"},
            )
            self.assertEqual(created.status_code, 200)
            body = created.json()
            self.assertIsInstance(body.get("id"), int)
            self.assertEqual(body.get("slug"), "new-channel")
            self.assertEqual(body.get("display_name"), "New Channel")

            duplicate = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "new-channel", "display_name": "Another Name"},
            )
            self.assertEqual(duplicate.status_code, 409)

    def test_update_channel_endpoint(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.patch(
                "/v1/channels/darkwood-reverie",
                json={"display_name": "Darkwood Updated"},
            )
            self.assertIn(unauthorized.status_code, (401, 403))

            invalid_display_name = client.patch(
                "/v1/channels/darkwood-reverie",
                headers=h,
                json={"display_name": "   "},
            )
            self.assertEqual(invalid_display_name.status_code, 422)

            not_found = client.patch(
                "/v1/channels/missing-channel",
                headers=h,
                json={"display_name": "Darkwood Updated"},
            )
            self.assertEqual(not_found.status_code, 404)

            updated = client.patch(
                "/v1/channels/darkwood-reverie",
                headers=h,
                json={"display_name": "Darkwood Updated"},
            )
            self.assertEqual(updated.status_code, 200)
            body = updated.json()
            self.assertEqual(body.get("slug"), "darkwood-reverie")
            self.assertEqual(body.get("display_name"), "Darkwood Updated")


    def test_delete_channel_endpoint(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            insert_release_and_job(env, channel_slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.delete("/v1/channels/channel-c")
            self.assertIn(unauthorized.status_code, (401, 403))

            not_found = client.delete("/v1/channels/missing-channel", headers=h)
            self.assertEqual(not_found.status_code, 404)

            with_job = client.delete("/v1/channels/darkwood-reverie", headers=h)
            self.assertEqual(with_job.status_code, 409)
            self.assertIn("jobs exist", with_job.json().get("detail", ""))

            no_jobs = client.delete("/v1/channels/channel-c", headers=h)
            self.assertEqual(no_jobs.status_code, 200)
            body = no_jobs.json()
            self.assertEqual(body.get("ok"), True)
            self.assertEqual(body.get("slug"), "channel-c")

            conn = dbm.connect(env)
            try:
                self.assertIsNone(dbm.get_channel_by_slug(conn, "channel-c"))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
