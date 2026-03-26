from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerMassActionsPreviewApi(unittest.TestCase):
    def _insert_planned_release(self, conn, *, publish_at: str, title: str) -> int:
        return int(
            conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES('darkwood-reverie', 'LONG', ?, ?, 'n', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (title, publish_at),
            ).lastrowid
        )

    def _insert_release(self, conn, *, title: str, meta_id: str) -> int:
        channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
        return int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, ?, 'd', '[]', '2026-01-01T00:00:00Z', NULL, ?, 0)
                """,
                (channel_id, title, meta_id),
            ).lastrowid
        )

    def test_preview_and_get_session_roundtrip(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planned_id = self._insert_planned_release(conn, publish_at="2026-01-10T00:00:00Z", title="materialize")
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            preview = client.post(
                "/v1/planner/mass-actions/preview",
                headers=auth,
                json={"action_type": "BATCH_MATERIALIZE_SELECTED", "selected_item_ids": [planned_id]},
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertEqual(body["action_type"], "BATCH_MATERIALIZE_SELECTED")
            self.assertEqual(body["selected_count"], 1)
            self.assertIn("session_id", body)
            self.assertIn("expires_at", body)

            session_id = str(body["session_id"])
            get_resp = client.get(f"/v1/planner/mass-actions/{session_id}", headers=auth)
            self.assertEqual(get_resp.status_code, 200)
            get_body = get_resp.json()
            self.assertEqual(get_body["session_id"], session_id)
            self.assertEqual(get_body["action_type"], "BATCH_MATERIALIZE_SELECTED")
            self.assertEqual(get_body["selected_item_ids"], [planned_id])

    def test_preview_validation_and_not_found_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            invalid_action = client.post(
                "/v1/planner/mass-actions/preview",
                headers=auth,
                json={"action_type": "NOPE", "selected_item_ids": [1]},
            )
            self.assertEqual(invalid_action.status_code, 422)
            self.assertEqual(invalid_action.json()["error"]["code"], "PMA_INVALID_ACTION_TYPE")

            empty = client.post(
                "/v1/planner/mass-actions/preview",
                headers=auth,
                json={"action_type": "BATCH_MATERIALIZE_SELECTED", "selected_item_ids": []},
            )
            self.assertEqual(empty.status_code, 422)
            self.assertEqual(empty.json()["error"]["code"], "PMA_SELECTION_EMPTY")

            oversized = client.post(
                "/v1/planner/mass-actions/preview",
                headers=auth,
                json={"action_type": "BATCH_MATERIALIZE_SELECTED", "selected_item_ids": list(range(1, 202))},
            )
            self.assertEqual(oversized.status_code, 422)
            self.assertEqual(oversized.json()["error"]["code"], "PMA_SELECTION_TOO_LARGE")

            bool_item = client.post(
                "/v1/planner/mass-actions/preview",
                headers=auth,
                json={"action_type": "BATCH_MATERIALIZE_SELECTED", "selected_item_ids": [True]},
            )
            self.assertEqual(bool_item.status_code, 400)
            self.assertEqual(bool_item.json()["error"]["code"], "PLR_INVALID_INPUT")

            not_found = client.get("/v1/planner/mass-actions/missing", headers=auth)
            self.assertEqual(not_found.status_code, 404)
            self.assertEqual(not_found.json()["error"]["code"], "PMA_SESSION_NOT_FOUND")

    def test_job_creation_preview_mixed_and_no_side_effect_writes(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                create_new_id = self._insert_planned_release(conn, publish_at="2026-01-11T00:00:00Z", title="create-job")
                existing_id = self._insert_planned_release(conn, publish_at="2026-01-11T01:00:00Z", title="existing-job")
                skipped_id = self._insert_planned_release(conn, publish_at="2026-01-11T02:00:00Z", title="skipped-job")
                failed_id = self._insert_planned_release(conn, publish_at="2026-01-11T03:00:00Z", title="failed-job")

                rel_create = self._insert_release(conn, title="rel-create", meta_id="meta-r-create")
                rel_existing = self._insert_release(conn, title="rel-existing", meta_id="meta-r-existing")
                rel_failed = self._insert_release(conn, title="rel-failed", meta_id="meta-r-failed")
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_create, create_new_id))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_existing, existing_id))
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (rel_failed, failed_id))

                existing_job = dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=rel_existing,
                    job_type="RELEASE",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=0,
                    updated_at=0,
                )
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (existing_job, rel_existing))
                dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=rel_failed,
                    job_type="RELEASE",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=0,
                    updated_at=0,
                )
                dbm.insert_job_with_lineage_defaults(
                    conn,
                    release_id=rel_failed,
                    job_type="RELEASE",
                    state="DRAFT",
                    stage="DRAFT",
                    priority=0,
                    attempt=0,
                    created_at=0,
                    updated_at=0,
                )
                conn.commit()

                before_release_count = int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"])
                before_job_count = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            preview = client.post(
                "/v1/planner/mass-actions/preview",
                headers=auth,
                json={
                    "action_type": "BATCH_CREATE_JOBS_FOR_SELECTED",
                    "selected_item_ids": [create_new_id, existing_id, skipped_id, failed_id],
                },
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            by_id = {int(item["planned_release_id"]): item for item in body["items"]}
            self.assertEqual(by_id[create_new_id]["result_kind"], "SUCCESS_CREATED_NEW")
            self.assertEqual(by_id[existing_id]["result_kind"], "SUCCESS_RETURNED_EXISTING")
            self.assertEqual(by_id[skipped_id]["result_kind"], "SKIPPED_NON_EXECUTABLE")
            self.assertEqual(by_id[failed_id]["result_kind"], "FAILED_INVALID_OR_INCONSISTENT")

            conn = dbm.connect(env)
            try:
                after_release_count = int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"])
                after_job_count = int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(before_release_count, after_release_count)
                self.assertEqual(before_job_count, after_job_count)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
