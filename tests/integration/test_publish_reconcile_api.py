from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPublishReconcileApi(unittest.TestCase):
    def _seed_publish_job(self, env, *, expected_visibility: str, observed_visibility: str | None) -> int:
        conn = dbm.connect(env)
        try:
            channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert channel is not None
            ts = dbm.now_ts()
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?,?,?,?,?)",
                (int(channel["id"]), "reconcile-release", "desc", "[]", ts),
            )
            release_id = int(cur.lastrowid)
            job_id = dbm.insert_job_with_lineage_defaults(
                conn,
                release_id=release_id,
                job_type="UI",
                state="UPLOADED",
                stage="PUBLISH",
                priority=1,
                attempt=0,
                created_at=ts,
                updated_at=ts,
            )
            conn.execute(
                "UPDATE jobs SET publish_state = 'published_public', publish_target_visibility = ?, publish_last_transition_at = ?, updated_at = ? WHERE id = ?",
                (expected_visibility, ts, ts, job_id),
            )
            if observed_visibility is not None:
                dbm.set_youtube_upload(
                    conn,
                    job_id,
                    video_id=f"vid-{job_id}",
                    url=f"https://example.test/watch/{job_id}",
                    studio_url=f"https://studio.example.test/{job_id}",
                    privacy=observed_visibility,
                )
            conn.commit()
            return job_id
        finally:
            conn.close()

    def test_run_persists_drift_without_mutating_publish_state_and_detail_contract(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            drift_job_id = self._seed_publish_job(env, expected_visibility="public", observed_visibility="unlisted")
            no_drift_job_id = self._seed_publish_job(env, expected_visibility="public", observed_visibility="public")

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            before_conn = dbm.connect(env)
            try:
                before_states = {
                    int(row["id"]): str(row["publish_state"])
                    for row in before_conn.execute(
                        "SELECT id, publish_state FROM jobs WHERE id IN (?, ?) ORDER BY id",
                        (drift_job_id, no_drift_job_id),
                    ).fetchall()
                }
            finally:
                before_conn.close()

            run_resp = client.post("/v1/publish/reconcile/run", headers=headers)
            self.assertEqual(run_resp.status_code, 200)
            run_body = run_resp.json()
            self.assertIn("run_id", run_body)
            self.assertEqual(run_body["trigger_mode"], "manual")
            self.assertEqual(run_body["status"], "completed")
            self.assertEqual(run_body["summary"]["total_jobs"], 2)
            self.assertEqual(run_body["summary"]["compared_jobs"], 2)
            self.assertEqual(run_body["summary"]["drift_count"], 1)
            self.assertEqual(run_body["summary"]["no_drift_count"], 1)

            run_id = int(run_body["run_id"])
            detail_resp = client.get(f"/v1/publish/reconcile/runs/{run_id}", headers=headers)
            self.assertEqual(detail_resp.status_code, 200)
            detail = detail_resp.json()
            self.assertEqual(detail["run_id"], run_id)
            self.assertEqual(detail["summary"]["drift_count"], 1)
            self.assertEqual(len(detail["items"]), 2)
            self.assertEqual({item["drift_classification"] for item in detail["items"]}, {"drift_detected", "no_drift"})

            after_conn = dbm.connect(env)
            try:
                after_states = {
                    int(row["id"]): str(row["publish_state"])
                    for row in after_conn.execute(
                        "SELECT id, publish_state FROM jobs WHERE id IN (?, ?) ORDER BY id",
                        (drift_job_id, no_drift_job_id),
                    ).fetchall()
                }
            finally:
                after_conn.close()
            self.assertEqual(before_states, after_states)


    def test_pre_publication_states_are_excluded_from_candidates_and_do_not_trigger_source_unavailable(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            excluded_job_id = self._seed_publish_job(env, expected_visibility="public", observed_visibility=None)

            conn = dbm.connect(env)
            try:
                ts = dbm.now_ts()
                conn.execute(
                    "UPDATE jobs SET publish_state = 'waiting_for_schedule', publish_target_visibility = 'public', publish_last_transition_at = ?, updated_at = ? WHERE id = ?",
                    (ts, ts, excluded_job_id),
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post("/v1/publish/reconcile/run", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["status"], "completed")
            self.assertEqual(body["summary"]["total_jobs"], 0)
            self.assertEqual(body["summary"]["compared_jobs"], 0)
            self.assertEqual(body["summary"]["drift_count"], 0)
            self.assertEqual(body["summary"]["no_drift_count"], 0)

            run_id = int(body["run_id"])
            detail = client.get(f"/v1/publish/reconcile/runs/{run_id}", headers=headers)
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.json()["items"], [])

    def test_run_source_unavailable_returns_503_and_persists_run_without_items(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            self._seed_publish_job(env, expected_visibility="public", observed_visibility=None)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.post("/v1/publish/reconcile/run", headers=headers)
            self.assertEqual(resp.status_code, 503)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "PRC_SOURCE_UNAVAILABLE")
            run_id = int(body["error"]["run_id"])

            conn = dbm.connect(env)
            try:
                run_row = conn.execute("SELECT * FROM publish_reconcile_runs WHERE id = ?", (run_id,)).fetchone()
                self.assertIsNotNone(run_row)
                assert run_row is not None
                self.assertEqual(str(run_row["status"]), "source_unavailable")
                self.assertEqual(int(run_row["compared_jobs"]), 0)
                self.assertEqual(int(run_row["drift_count"]), 0)
                self.assertEqual(int(run_row["no_drift_count"]), 0)
                item_count = int(
                    conn.execute("SELECT COUNT(*) AS c FROM publish_reconcile_items WHERE run_id = ?", (run_id,)).fetchone()["c"]
                )
                self.assertEqual(item_count, 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
