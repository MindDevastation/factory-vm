from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestUiJobsStatusesEndpoint(unittest.TestCase):
    def test_requires_basic_auth(self) -> None:
        with temp_env() as (_, _):
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get("/v1/ui/jobs/statuses")

            self.assertEqual(response.status_code, 401)

    def test_returns_source_of_truth_statuses(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
            finally:
                conn.close()
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get(
                "/v1/ui/jobs/statuses",
                headers=basic_auth_header(env.basic_user, env.basic_pass),
            )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertIn("statuses", body)
            self.assertTrue(body["statuses"])
            expected = list(dbm.UI_JOB_STATES)
            self.assertEqual(body["statuses"], expected)

    def test_appends_unknown_states_currently_present_in_jobs(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                channel = dbm.create_channel(conn, slug="status-check", display_name="Status Check", kind="YOUTUBE")
                release_cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, created_at)
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (int(channel["id"]), "State domain", "state domain", "[]", dbm.now_ts()),
                )
                release_id = int(release_cur.lastrowid)
                dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=release_id,
                    job_type="UI",
                    state="LEGACY_STATE",
                    stage="RENDER",
                    priority=0,
                    attempt=0,
                    created_at=dbm.now_ts(),
                    updated_at=dbm.now_ts(),
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)

            response = client.get(
                "/v1/ui/jobs/statuses",
                headers=basic_auth_header(env.basic_user, env.basic_pass),
            )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["statuses"][-1], "LEGACY_STATE")
            self.assertEqual(body["statuses"][:-1], list(dbm.UI_JOB_STATES))


if __name__ == "__main__":
    unittest.main()
