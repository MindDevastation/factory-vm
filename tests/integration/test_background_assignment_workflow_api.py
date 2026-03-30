from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestBackgroundAssignmentWorkflowApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _seed_release_with_job(self, conn) -> tuple[int, int, int]:
        channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        assert channel is not None
        job_id = dbm.create_ui_job_draft(
            conn,
            channel_id=int(channel["id"]),
            title="bg flow",
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

        cover_asset_id = dbm.create_asset(
            conn,
            channel_id=int(channel["id"]),
            kind="IMAGE",
            origin="CHANNEL",
            origin_id="channel://cover-1",
            name="cover-1.png",
            path="/tmp/cover-1.png",
        )
        dbm.link_job_input(conn, job_id, cover_asset_id, "COVER", 0)
        return release_id, int(channel["id"]), cover_asset_id

    def _template_payload_with_default_background(self, background_asset_id: int) -> dict[str, object]:
        return {
            "palette_guidance": "palette",
            "typography_rules": "type",
            "text_layout_rules": "layout",
            "composition_framing_rules": "framing",
            "branding_rules": "branding",
            "output_profile_guidance": "output",
            "background_compatibility_guidance": "bg",
            "cover_composition_guidance": "cover",
            "allowed_motifs": ["motif"],
            "banned_motifs": ["ban"],
            "default_background_asset_id": background_asset_id,
        }

    def test_candidate_preview_approve_apply_flow_and_provenance(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, channel_id, _cover_asset_id = self._seed_release_with_job(conn)
                managed_bg = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="MANAGED",
                    origin_id="managed://bg-1",
                    name="bg-managed.png",
                    path="/tmp/bg-managed.png",
                )
                manual_bg = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://bg-manual",
                    name="bg-manual.png",
                    path="/tmp/bg-manual.png",
                )
                dbm.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Default Visual",
                    template_payload_json=dbm.json_dumps(self._template_payload_with_default_background(managed_bg)),
                    status="ACTIVE",
                    is_default=True,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            candidates = client.get(f"/v1/visual/releases/{release_id}/background/candidates", headers=headers)
            self.assertEqual(candidates.status_code, 200)
            body = candidates.json()
            self.assertIn("managed_library", body["source_families"])
            self.assertTrue(any(int(item["asset_id"]) == managed_bg for item in body["candidates"]))
            self.assertEqual(int(body["prefill"]["background_asset_id"]), managed_bg)
            self.assertEqual(body["prefill"]["selection_mode"], "auto_assisted")
            self.assertTrue(body["prefill"]["template_assisted"])
            assisted_candidate = next(item for item in body["candidates"] if int(item["asset_id"]) == managed_bg)
            self.assertEqual(assisted_candidate["selection_mode_prefill"], "auto_assisted")
            self.assertTrue(assisted_candidate["template_assisted"])

            preview = client.post(
                f"/v1/visual/releases/{release_id}/background/preview",
                headers=headers,
                json={
                    "background_asset_id": manual_bg,
                    "source_family": "operator_imported",
                    "source_reference": "local://bg-manual",
                    "template_assisted": True,
                },
            )
            self.assertEqual(preview.status_code, 200)
            preview_id = preview.json()["preview_id"]
            self.assertEqual(preview.json()["selection"]["selection_mode"], "manual")
            self.assertFalse(preview.json()["selection"]["template_assisted"])

            approve = client.post(
                f"/v1/visual/releases/{release_id}/background/approve",
                headers=headers,
                json={"preview_id": preview_id},
            )
            self.assertEqual(approve.status_code, 200)

            apply_r = client.post(f"/v1/visual/releases/{release_id}/background/apply", headers=headers)
            self.assertEqual(apply_r.status_code, 200)
            applied = apply_r.json()
            self.assertEqual(int(applied["background_asset_id"]), manual_bg)
            self.assertIn("background_asset", applied["summary"])
            self.assertEqual(applied["summary"]["background_asset"]["source_family"], "operator_imported")
            self.assertEqual(applied["summary"]["background_asset"]["selection_mode"], "manual")
            self.assertFalse(applied["summary"]["background_asset"]["template_assisted"])

            conn2 = dbm.connect(env)
            try:
                provenance = conn2.execute(
                    "SELECT source_family, source_reference, selection_mode, template_assisted FROM release_visual_background_decisions WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert provenance is not None
                self.assertEqual(str(provenance["source_family"]), "operator_imported")
                self.assertEqual(str(provenance["selection_mode"]), "manual")
                self.assertEqual(int(provenance["template_assisted"]), 0)
            finally:
                conn2.close()

    def test_apply_forbidden_when_canonical_cover_missing(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r', 'd', '[]', NULL, NULL, 'origin-cover-miss', NULL, 1.0)
                        """,
                        (int(channel["id"]),),
                    ).lastrowid
                )
                bg_id = dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://bg-no-cover",
                    name="bg-no-cover.png",
                    path="/tmp/bg-no-cover.png",
                )
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/visual/releases/{release_id}/background/preview",
                headers=headers,
                json={"background_asset_id": bg_id, "source_family": "operator_imported", "source_reference": "local://bg-no-cover"},
            )
            self.assertEqual(preview.status_code, 200)
            preview_id = preview.json()["preview_id"]
            approve = client.post(
                f"/v1/visual/releases/{release_id}/background/approve",
                headers=headers,
                json={"preview_id": preview_id},
            )
            self.assertEqual(approve.status_code, 200)

            apply_r = client.post(f"/v1/visual/releases/{release_id}/background/apply", headers=headers)
            self.assertEqual(apply_r.status_code, 422)
            self.assertEqual(apply_r.json()["error"]["code"], "VBG_CANONICAL_COVER_REQUIRED")

            conn2 = dbm.connect(env)
            try:
                applied_count = conn2.execute(
                    "SELECT COUNT(*) AS c FROM release_visual_applied_packages WHERE release_id = ?",
                    (release_id,),
                ).fetchone()
                assert applied_count is not None
                self.assertEqual(int(applied_count["c"]), 0)
            finally:
                conn2.close()

    def test_generation_source_family_is_rejected(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, channel_id, _cover_asset_id = self._seed_release_with_job(conn)
                bg_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://bg-gen-reject",
                    name="bg-gen-reject.png",
                    path="/tmp/bg-gen-reject.png",
                )
            finally:
                conn.close()

            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                f"/v1/visual/releases/{release_id}/background/preview",
                headers=headers,
                json={"background_asset_id": bg_id, "source_family": "generation"},
            )
            self.assertEqual(preview.status_code, 422)
            self.assertEqual(preview.json()["error"]["code"], "VBG_UNSUPPORTED_SOURCE_FAMILY")


if __name__ == "__main__":
    unittest.main()
