from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.planner.runtime_visual_resolver import apply_release_visual_package
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

    def _seed_release_with_runtime_job(self, conn) -> tuple[int, int, int, int]:
        channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        assert channel is not None
        job_id = dbm.create_ui_job_draft(
            conn,
            channel_id=int(channel["id"]),
            title="cover apply flow",
            description="desc",
            tags_csv="a",
            cover_name="cover-old.png",
            cover_ext="png",
            background_name="bg-old.png",
            background_ext="png",
            audio_ids_text="1",
            job_type="UI",
        )
        job = dbm.get_job(conn, job_id)
        assert job is not None
        release_id = int(job["release_id"])
        conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))
        bg_asset_id = dbm.create_asset(
            conn,
            channel_id=int(channel["id"]),
            kind="IMAGE",
            origin="LOCAL",
            origin_id="local://bg-runtime",
            name="bg-runtime.png",
            path="/tmp/bg-runtime.png",
        )
        old_cover_asset_id = dbm.create_asset(
            conn,
            channel_id=int(channel["id"]),
            kind="IMAGE",
            origin="LOCAL",
            origin_id="local://cover-old",
            name="cover-old.png",
            path="/tmp/cover-old.png",
        )
        dbm.link_job_input(conn, job_id, bg_asset_id, "BACKGROUND", 0)
        dbm.link_job_input(conn, job_id, old_cover_asset_id, "COVER", 0)
        return release_id, int(channel["id"]), int(bg_asset_id), int(job_id)

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

    def test_cover_approve_apply_uses_selected_cover_for_thumbnail_and_runtime(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, channel_id, background_asset_id, job_id = self._seed_release_with_runtime_job(conn)
                new_cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://cover-new",
                    name="cover-new.png",
                    path="/tmp/cover-new.png",
                )
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                f"/v1/visual/releases/{release_id}/cover/candidates",
                headers=headers,
                json={
                    "cover_asset_id": new_cover_asset_id,
                    "source_provider_family": "manual_provider",
                    "source_reference": "manual://cover-new",
                    "selection_mode": "manual",
                },
            )
            self.assertEqual(created.status_code, 200)
            candidate_id = str(created.json()["candidate_id"])

            selected = client.post(
                f"/v1/visual/releases/{release_id}/cover/select",
                headers=headers,
                json={"candidate_id": candidate_id},
            )
            self.assertEqual(selected.status_code, 200)

            approved = client.post(
                f"/v1/visual/releases/{release_id}/cover/approve",
                headers=headers,
                json={},
            )
            self.assertEqual(approved.status_code, 200)
            self.assertEqual(str(approved.json()["candidate_id"]), candidate_id)

            applied = client.post(f"/v1/visual/releases/{release_id}/cover/apply", headers=headers)
            self.assertEqual(applied.status_code, 200)
            body = applied.json()
            self.assertEqual(int(body["background_asset_id"]), background_asset_id)
            self.assertEqual(int(body["cover_asset_id"]), new_cover_asset_id)
            self.assertEqual(body["summary"]["thumbnail_source"]["source_kind"], "cover_asset")
            self.assertEqual(int(body["summary"]["thumbnail_source"]["asset_id"]), new_cover_asset_id)

            conn2 = dbm.connect(env)
            try:
                package = conn2.execute(
                    "SELECT background_asset_id, cover_asset_id FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert package is not None
                self.assertEqual(int(package["background_asset_id"]), background_asset_id)
                self.assertEqual(int(package["cover_asset_id"]), new_cover_asset_id)
                role_rows = conn2.execute(
                    "SELECT role, asset_id FROM job_inputs WHERE job_id = ? AND role IN ('BACKGROUND','COVER') ORDER BY role ASC",
                    (job_id,),
                ).fetchall()
                self.assertEqual([(str(r["role"]), int(r["asset_id"])) for r in role_rows], [("BACKGROUND", background_asset_id), ("COVER", new_cover_asset_id)])
            finally:
                conn2.close()

    def test_cover_apply_surfaces_reuse_warning_context_and_writes_history(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, channel_id, background_asset_id, _job_id = self._seed_release_with_runtime_job(conn)
                prior_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at) VALUES(?, 'prior', 'd', '[]', NULL, NULL, 'origin-prior-cover-reuse', NULL, 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                reused_cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://cover-reused",
                    name="cover-reused.png",
                    path="/tmp/cover-reused.png",
                )
                apply_release_visual_package(
                    conn,
                    release_id=prior_release,
                    background_asset_id=background_asset_id,
                    cover_asset_id=reused_cover_asset_id,
                    source_preview_id=None,
                    applied_by="seed",
                )
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                f"/v1/visual/releases/{release_id}/cover/candidates",
                headers=headers,
                json={
                    "cover_asset_id": reused_cover_asset_id,
                    "source_provider_family": "manual_provider",
                    "source_reference": "manual://cover-reused",
                    "selection_mode": "manual",
                },
            )
            self.assertEqual(created.status_code, 200)
            candidate_id = str(created.json()["candidate_id"])
            self.assertEqual(
                client.post(
                    f"/v1/visual/releases/{release_id}/cover/select",
                    headers=headers,
                    json={"candidate_id": candidate_id},
                ).status_code,
                200,
            )
            self.assertEqual(
                client.post(
                    f"/v1/visual/releases/{release_id}/cover/approve",
                    headers=headers,
                    json={},
                ).status_code,
                200,
            )

            blocked = client.post(f"/v1/visual/releases/{release_id}/cover/apply", headers=headers, json={})
            self.assertEqual(blocked.status_code, 422)
            self.assertEqual(blocked.json()["error"]["code"], "VCOVER_REUSE_OVERRIDE_REQUIRED")
            self.assertIn("prior_release_id", blocked.json()["error"]["message"])

            applied = client.post(
                f"/v1/visual/releases/{release_id}/cover/apply",
                headers=headers,
                json={"reuse_override_confirmed": True},
            )
            self.assertEqual(applied.status_code, 200)
            self.assertTrue(applied.json()["reuse"]["requires_override"])
            self.assertGreaterEqual(len(applied.json()["reuse"]["prior_usage"]), 1)

            conn2 = dbm.connect(env)
            try:
                events = conn2.execute(
                    """
                    SELECT history_stage
                    FROM release_visual_history_events
                    WHERE release_id = ? AND preview_scope = 'cover'
                    ORDER BY id ASC
                    """,
                    (release_id,),
                ).fetchall()
                self.assertEqual([str(row["history_stage"]) for row in events], ["PREVIEWED", "APPROVED", "APPLIED"])
            finally:
                conn2.close()


if __name__ == "__main__":
    unittest.main()
