from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job, basic_auth_header


class TestApiE2E(unittest.TestCase):
    def test_basic_auth_and_job_actions(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            # Create two jobs: one WAIT_APPROVAL, one READY_FOR_RENDER
            job_wait = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")
            job_ready = insert_release_and_job(env, title="To cancel", state="READY_FOR_RENDER", stage="FETCH")

            # Import app after env set
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            # no auth
            r = client.get("/v1/jobs")
            self.assertEqual(r.status_code, 401)

            h = basic_auth_header(env.basic_user, env.basic_pass)

            r2 = client.get("/v1/jobs", headers=h)
            self.assertEqual(r2.status_code, 200)
            jobs = r2.json()["jobs"]
            ids = {int(j["id"]) for j in jobs}
            self.assertIn(job_wait, ids)
            self.assertIn(job_ready, ids)

            # approve
            ra = client.post(f"/v1/jobs/{job_wait}/approve", json={"comment": "ok"}, headers=h)
            self.assertEqual(ra.status_code, 200)

            # mark published
            rp = client.post(f"/v1/jobs/{job_wait}/mark_published", json={}, headers=h)
            self.assertEqual(rp.status_code, 200)

            # cancel READY job
            rc = client.post(f"/v1/jobs/{job_ready}/cancel", json={"reason": "stop"}, headers=h)
            self.assertEqual(rc.status_code, 200)

            # cancel terminal should 409
            rc2 = client.post(f"/v1/jobs/{job_wait}/cancel", json={"reason": "stop"}, headers=h)
            self.assertEqual(rc2.status_code, 409)

            # verify DB states
            conn = dbm.connect(env)
            try:
                j1 = dbm.get_job(conn, job_wait)
                j2 = dbm.get_job(conn, job_ready)
            finally:
                conn.close()

            assert j1 is not None and j2 is not None
            self.assertEqual(j1["state"], "PUBLISHED")
            self.assertEqual(j2["state"], "CANCELLED")


if __name__ == "__main__":
    unittest.main()
