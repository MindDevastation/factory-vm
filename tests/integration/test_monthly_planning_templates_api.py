from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMonthlyPlanningTemplatesApi(unittest.TestCase):
    def _snapshot_counts(self, env: Env) -> dict[str, int]:
        conn = dbm.connect(env)
        try:
            return {
                "planned_releases": int(conn.execute("SELECT COUNT(*) AS c FROM planned_releases").fetchone()["c"]),
                "jobs": int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"]),
                "releases": int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"]),
            }
        finally:
            conn.close()

    def test_crud_and_archived_visibility(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            before = self._snapshot_counts(env)

            create_resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "April core batch",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "day-01-main",
                            "slot_code": "day_01_main",
                            "position": 1,
                            "title": "Release 01",
                            "day_of_month": 1,
                            "notes": "optional",
                        }
                    ],
                },
            )
            self.assertEqual(create_resp.status_code, 201)
            created = create_resp.json()
            self.assertEqual(created["status"], "ACTIVE")
            self.assertEqual(created["apply_run_count"], 0)
            tid = int(created["id"])

            list_resp = client.get("/v1/planner/monthly-planning-templates?channel_id=1&status=ACTIVE&q=april", headers=auth)
            self.assertEqual(list_resp.status_code, 200)
            list_body = list_resp.json()
            self.assertEqual(list_body["total"], 1)
            self.assertEqual(list_body["items"][0]["item_count"], 1)

            detail_resp = client.get(f"/v1/planner/monthly-planning-templates/{tid}", headers=auth)
            self.assertEqual(detail_resp.status_code, 200)
            self.assertEqual(detail_resp.json()["template_name"], "April core batch")

            patch_resp = client.patch(
                f"/v1/planner/monthly-planning-templates/{tid}",
                headers=auth,
                json={
                    "template_name": "April core batch v2",
                    "items": [
                        {
                            "item_key": "day-02-main",
                            "slot_code": "day_02_main",
                            "position": 1,
                            "title": "Release 02",
                            "day_of_month": 2,
                            "notes": None,
                        }
                    ],
                },
            )
            self.assertEqual(patch_resp.status_code, 200)
            patched = patch_resp.json()
            self.assertEqual(patched["template_name"], "April core batch v2")
            self.assertEqual(patched["items"][0]["item_key"], "day-02-main")

            archive_resp = client.post(f"/v1/planner/monthly-planning-templates/{tid}/archive", headers=auth)
            self.assertEqual(archive_resp.status_code, 200)
            self.assertEqual(archive_resp.json()["status"], "ARCHIVED")

            archived_list = client.get("/v1/planner/monthly-planning-templates?status=ARCHIVED", headers=auth)
            self.assertEqual(archived_list.status_code, 200)
            self.assertEqual(archived_list.json()["total"], 1)

            archived_detail = client.get(f"/v1/planner/monthly-planning-templates/{tid}", headers=auth)
            self.assertEqual(archived_detail.status_code, 200)
            self.assertEqual(archived_detail.json()["status"], "ARCHIVED")

            after = self._snapshot_counts(env)
            self.assertEqual(before, after)

    def test_validation_errors_use_error_envelope(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "bad",
                    "content_type": "",
                    "items": [
                        {
                            "item_key": "bad-key",
                            "slot_code": "slot_1",
                            "position": 1,
                            "title": "x",
                            "day_of_month": 1,
                            "notes": None,
                        }
                    ],
                },
            )
            self.assertEqual(resp.status_code, 400)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "MPT_INVALID_CONTENT_TYPE")


if __name__ == "__main__":
    unittest.main()
