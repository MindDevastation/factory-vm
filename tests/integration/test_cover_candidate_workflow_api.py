from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestCoverCandidateWorkflowApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_release(self, conn) -> tuple[int, int]:
        channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        assert channel is not None
        release_id = int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                VALUES(?, 'cover flow', 'desc', '[]', NULL, NULL, 'origin-cover-flow', NULL, 1.0)
                """,
                (int(channel["id"]),),
            ).lastrowid
        )
        return release_id, int(channel["id"])

    def test_cover_input_candidate_preview_select_flow_with_provenance(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, channel_id = self._seed_release(conn)
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://cover-preview",
                    name="cover-preview.png",
                    path="/tmp/cover-preview.png",
                )
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            input_payload = client.post(
                f"/v1/visual/releases/{release_id}/cover/input-payload",
                headers=headers,
                json={
                    "provider_family": "gen_provider_x",
                    "input_payload": {"prompt": "mist forest", "seed": 42},
                    "template_ref": {"template_id": 12, "template_name": "Cover Base"},
                },
            )
            self.assertEqual(input_payload.status_code, 200)
            input_payload_id = int(input_payload.json()["input_payload_id"])

            candidate_create = client.post(
                f"/v1/visual/releases/{release_id}/cover/candidates",
                headers=headers,
                json={
                    "cover_asset_id": cover_asset_id,
                    "source_provider_family": "gen_provider_x",
                    "source_reference": "gen://job-123#1",
                    "input_payload_id": input_payload_id,
                    "selection_mode": "auto_assisted",
                    "template_ref": {"template_id": 12},
                },
            )
            self.assertEqual(candidate_create.status_code, 200)
            created = candidate_create.json()
            self.assertEqual(created["source_provider_family"], "gen_provider_x")
            self.assertEqual(created["source_reference"], "gen://job-123#1")
            self.assertEqual(created["input_payload_id"], input_payload_id)
            self.assertFalse(created["is_manual_selection"])
            self.assertNotIn("thumbnail_asset_id", created)
            candidate_id = created["candidate_id"]

            listed = client.get(f"/v1/visual/releases/{release_id}/cover/candidates", headers=headers)
            self.assertEqual(listed.status_code, 200)
            self.assertEqual(listed.json()["selected_candidate_id"], None)
            self.assertEqual(len(listed.json()["candidates"]), 1)

            preview = client.get(f"/v1/visual/releases/{release_id}/cover/candidates/{candidate_id}/preview", headers=headers)
            self.assertEqual(preview.status_code, 200)
            preview_body = preview.json()
            self.assertEqual(int(preview_body["preview"]["cover_asset"]["asset_id"]), cover_asset_id)
            self.assertEqual(preview_body["preview"]["selection_mode"], "auto_assisted")
            self.assertNotIn("thumbnail_asset_id", preview_body["preview"])

            selected = client.post(
                f"/v1/visual/releases/{release_id}/cover/select",
                headers=headers,
                json={"candidate_id": candidate_id},
            )
            self.assertEqual(selected.status_code, 200)
            self.assertTrue(selected.json()["approval_path_ready"])
            self.assertEqual(selected.json()["selected_candidate_id"], candidate_id)

            conn2 = dbm.connect(env)
            try:
                selected_row = conn2.execute(
                    "SELECT candidate_id FROM release_visual_cover_selected_candidates WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert selected_row is not None
                self.assertEqual(str(selected_row["candidate_id"]), candidate_id)
            finally:
                conn2.close()

    def test_cover_candidate_requires_existing_release_and_asset(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            missing = client.post(
                "/v1/visual/releases/99999/cover/candidates",
                headers=headers,
                json={
                    "cover_asset_id": 1,
                    "source_provider_family": "manual_provider",
                    "selection_mode": "manual",
                },
            )
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json()["error"]["code"], "VCOVER_RELEASE_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
