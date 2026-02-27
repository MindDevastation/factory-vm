from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.track_analyzer import track_jobs_db
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


if __name__ == "__main__":
    unittest.main()
