from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestApiHtmlAndErrors(unittest.TestCase):
    def test_health_html_and_error_branches(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")

            conn = dbm.connect(env)
            try:
                # worker with invalid JSON to cover details_json except branch
                conn.execute(
                    "INSERT INTO worker_heartbeats(worker_id, role, pid, hostname, details_json, last_seen) VALUES(?,?,?,?,?,?)",
                    ("w1", "importer", 1, "h", "{invalid-json", dbm.now_ts()),
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/health")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.json().get("ok"))

            # HTML pages
            r = client.get("/", headers=auth)
            self.assertEqual(r.status_code, 200)

            r = client.get(f"/jobs/{job_id}", headers=auth)
            self.assertEqual(r.status_code, 200)

            r = client.get("/jobs/99999", headers=auth)
            self.assertEqual(r.status_code, 404)

            # /v1/workers bad details_json -> details={}
            r = client.get("/v1/workers", headers=auth)
            self.assertEqual(r.status_code, 200)
            workers = r.json()["workers"]
            self.assertTrue(any(w.get("worker_id") == "w1" and w.get("details") == {} for w in workers))

            # /v1/jobs/{id} not found
            r = client.get("/v1/jobs/99999", headers=auth)
            self.assertEqual(r.status_code, 404)

            # logs/qa endpoints when files missing
            r = client.get(f"/v1/jobs/{job_id}/logs", headers=auth)
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.text, "")

            r = client.get(f"/v1/jobs/{job_id}/qa", headers=auth)
            self.assertEqual(r.status_code, 200)
            self.assertIsNone(r.json()["qa"])

            # approve/reject invalid state -> 409
            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, job_id, state="QA_RUNNING", stage="QA")
            finally:
                conn.close()

            r = client.post(f"/v1/jobs/{job_id}/approve", headers=auth, json={"comment": "ok"})
            self.assertEqual(r.status_code, 409)
            r = client.post(f"/v1/jobs/{job_id}/reject", headers=auth, json={"comment": "no"})
            self.assertEqual(r.status_code, 409)

            # mark_published invalid state -> 409
            r = client.post(f"/v1/jobs/{job_id}/mark_published", headers=auth, json={})
            self.assertEqual(r.status_code, 409)

            # cancel 404
            r = client.post("/v1/jobs/99999/cancel", headers=auth, json={"reason": "x"})
            self.assertEqual(r.status_code, 404)

            # cancel works from WAIT_APPROVAL and then becomes terminal
            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL")
            finally:
                conn.close()

            r = client.post(f"/v1/jobs/{job_id}/cancel", headers=auth, json={"reason": "stop"})
            self.assertEqual(r.status_code, 200)
            r = client.post(f"/v1/jobs/{job_id}/cancel", headers=auth, json={"reason": "stop2"})
            self.assertEqual(r.status_code, 409)
