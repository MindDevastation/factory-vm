from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.track_analyzer import track_jobs_db
from services.workers.track_jobs import track_jobs_cycle
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestTrackJobsApiSlice1(unittest.TestCase):
    def _seed_canon(self, env: Env, *, slug: str, include_channel: bool = True, include_threshold: bool = True) -> None:
        conn = dbm.connect(env)
        try:
            if include_channel:
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", (slug,))
            if include_threshold:
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", (slug,))
        finally:
            conn.close()

    def test_auth_required(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            r = client.post("/v1/track_jobs/discover", json={"channel_slug": "darkwood-reverie"})
            self.assertIn(r.status_code, (401, 403))

    def test_404_channel_not_found(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="missing-channel")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "missing-channel"})
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json().get("detail"), "channel not found")

    def test_404_channel_not_in_canon_when_canon_channels_missing(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie", include_channel=False, include_threshold=True)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "darkwood-reverie"})
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json().get("detail"), "CHANNEL_NOT_IN_CANON")

    def test_404_channel_not_in_canon_when_canon_thresholds_missing(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie", include_channel=True, include_threshold=False)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.post(
                "/v1/track_jobs/analyze",
                headers=h,
                json={"channel_slug": "darkwood-reverie", "scope": "pending", "max_tracks": 0, "force": False},
            )
            self.assertEqual(r.status_code, 404)
            self.assertEqual(r.json().get("detail"), "CHANNEL_NOT_IN_CANON")

    def test_202_happy_path_and_read_endpoints(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            discover = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "darkwood-reverie"})
            self.assertEqual(discover.status_code, 202)
            discover_body = discover.json()
            self.assertEqual(discover_body.get("status"), "QUEUED")
            discover_id = int(discover_body["job_id"])

            get_job = client.get(f"/v1/track_jobs/{discover_id}", headers=h)
            self.assertEqual(get_job.status_code, 200)
            self.assertEqual(get_job.json()["job"]["status"], "QUEUED")
            self.assertEqual(get_job.json()["job"]["job_type"], "SCAN_TRACKS")

            conn = dbm.connect(env)
            try:
                track_jobs_db.append_log(conn, job_id=discover_id, message="queued", level="INFO")
            finally:
                conn.close()

            get_logs = client.get(f"/v1/track_jobs/{discover_id}/logs?tail=200", headers=h)
            self.assertEqual(get_logs.status_code, 200)
            logs = get_logs.json().get("logs", [])
            self.assertEqual(len(logs), 1)
            self.assertEqual(logs[0]["message"], "queued")

    def test_409_already_running(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie")

            conn = dbm.connect(env)
            try:
                track_jobs_db.enqueue_job(conn, job_type="ANALYZE_TRACKS", channel_slug="darkwood-reverie", payload={})
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.post(
                "/v1/track_jobs/analyze",
                headers=h,
                json={"channel_slug": "darkwood-reverie", "scope": "pending", "max_tracks": 0, "force": False},
            )
            self.assertEqual(r.status_code, 409)
            self.assertEqual(r.json().get("detail"), "TRACK_JOB_ALREADY_RUNNING")

    def test_enable_then_discover_then_disable_flow(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            before = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "darkwood-reverie"})
            self.assertEqual(before.status_code, 404)
            self.assertEqual(before.json().get("detail"), "CHANNEL_NOT_IN_CANON")

            enable = client.post("/v1/track_catalog/darkwood-reverie/enable", headers=h)
            self.assertEqual(enable.status_code, 200)
            self.assertEqual(
                enable.json(),
                {"ok": True, "channel_slug": "darkwood-reverie", "track_catalog_enabled": True},
            )

            discover = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "darkwood-reverie"})
            self.assertEqual(discover.status_code, 202)
            discover_body = discover.json()
            self.assertEqual(discover_body.get("status"), "QUEUED")
            self.assertTrue(str(discover_body.get("job_id") or "").strip())

            disable = client.delete("/v1/track_catalog/darkwood-reverie/enable", headers=h)
            self.assertEqual(disable.status_code, 200)
            self.assertEqual(
                disable.json(),
                {"ok": True, "channel_slug": "darkwood-reverie", "track_catalog_enabled": False},
            )

            after = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "darkwood-reverie"})
            self.assertEqual(after.status_code, 404)
            self.assertEqual(after.json().get("detail"), "CHANNEL_NOT_IN_CANON")

    def test_discover_job_fails_when_per_channel_token_missing(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = "/secure/gdrive/client_secret.json"
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            env = Env.load()
            seed_minimal_db(env)
            self._seed_canon(env, slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            discover = client.post("/v1/track_jobs/discover", headers=h, json={"channel_slug": "darkwood-reverie"})
            self.assertEqual(discover.status_code, 202)
            job_id = int(discover.json()["job_id"])

            track_jobs_cycle(env=env, worker_id="t-track-jobs-discover-missing-token")

            get_job = client.get(f"/v1/track_jobs/{job_id}", headers=h)
            self.assertEqual(get_job.status_code, 200)
            job = get_job.json()["job"]
            self.assertEqual(job["status"], "FAILED")
            payload = job.get("payload") or {}
            self.assertIn("GDrive token missing or unreadable for channel 'darkwood-reverie'", str(payload.get("last_message") or ""))
            self.assertIn("Generate/Regenerate Drive Token in dashboard.", str(payload.get("last_message") or ""))


if __name__ == "__main__":
    unittest.main()
