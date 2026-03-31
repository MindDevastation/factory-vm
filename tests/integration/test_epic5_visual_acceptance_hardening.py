from __future__ import annotations

import importlib
import json
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.planner import background_assignment_service
from services.planner.runtime_visual_resolver import apply_release_visual_package
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestEpic5VisualAcceptanceHardening(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_runtime_binding_and_thumbnail_cover_compatibility(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="runtime compatibility",
                    description="d",
                    tags_csv="a",
                    cover_name="cover-old.png",
                    cover_ext="png",
                    background_name="bg-old.png",
                    background_ext="png",
                    audio_ids_text="1",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))

                bg_asset_id = dbm.create_asset(
                    conn, channel_id=int(channel["id"]), kind="IMAGE", origin="LOCAL", origin_id="local://bg-new", name="bg-new.png", path="/tmp/bg-new.png"
                )
                cover_asset_id = dbm.create_asset(
                    conn, channel_id=int(channel["id"]), kind="IMAGE", origin="LOCAL", origin_id="local://cover-new", name="cover-new.png", path="/tmp/cover-new.png"
                )
                out = apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="tester",
                )
                self.assertTrue(out.runtime_bound)
                roles = conn.execute(
                    "SELECT role, asset_id FROM job_inputs WHERE job_id = ? AND role IN ('BACKGROUND','COVER') ORDER BY role ASC",
                    (job_id,),
                ).fetchall()
                self.assertEqual([(str(r["role"]), int(r["asset_id"])) for r in roles], [("BACKGROUND", bg_asset_id), ("COVER", cover_asset_id)])
            finally:
                conn.close()

            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            created = client.post(
                f"/v1/visual/releases/{release_id}/cover/candidates",
                headers=h,
                json={"cover_asset_id": cover_asset_id, "source_provider_family": "manual_provider", "selection_mode": "manual"},
            )
            self.assertEqual(created.status_code, 200)
            candidate_id = str(created.json()["candidate_id"])
            self.assertEqual(client.post(f"/v1/visual/releases/{release_id}/cover/select", headers=h, json={"candidate_id": candidate_id}).status_code, 200)
            self.assertEqual(client.post(f"/v1/visual/releases/{release_id}/cover/approve", headers=h, json={}).status_code, 200)
            applied = client.post(f"/v1/visual/releases/{release_id}/cover/apply", headers=h, json={"reuse_override_confirmed": True})
            self.assertEqual(applied.status_code, 200)
            self.assertEqual(applied.json()["summary"]["thumbnail_source"]["source_kind"], "cover_asset")

    def test_no_open_job_deferred_runtime_resolution(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                release_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?, 'deferred', 'd', '[]', 1.0)",
                        (int(channel["id"]),),
                    ).lastrowid
                )
                bg_asset_id = dbm.create_asset(conn, channel_id=int(channel["id"]), kind="IMAGE", origin="LOCAL", origin_id="local://defer-bg", name="defer-bg.png", path="/tmp/defer-bg.png")
                cover_asset_id = dbm.create_asset(conn, channel_id=int(channel["id"]), kind="IMAGE", origin="LOCAL", origin_id="local://defer-cover", name="defer-cover.png", path="/tmp/defer-cover.png")
                out = apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="tester",
                )
                self.assertTrue(out.deferred)
                self.assertFalse(out.runtime_bound)
            finally:
                conn.close()

    def test_reuse_override_path_and_audit_explainability(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])
                prior_release = int(conn.execute("INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?, 'prior', 'd', '[]', 1.0)", (channel_id,)).lastrowid)
                target_release = int(conn.execute("INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?, 'target', 'd', '[]', 1.0)", (channel_id,)).lastrowid)
                reused_bg = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="local://reuse-bg", name="reuse-bg.png", path="/tmp/reuse-bg.png")
                reused_cover = dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="local://reuse-cover", name="reuse-cover.png", path="/tmp/reuse-cover.png")
                dbm.create_asset(conn, channel_id=channel_id, kind="IMAGE", origin="LOCAL", origin_id="local://alt-bg", name="alt-bg.png", path="/tmp/alt-bg.png")
                conn.execute(
                    "INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at) VALUES(?, ?, ?, NULL, 'seed', '2026-01-01T00:00:00+00:00')",
                    (prior_release, reused_bg, reused_cover),
                )
                conn.execute(
                    "INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at) VALUES(?, ?, ?, NULL, 'seed', '2026-01-02T00:00:00+00:00')",
                    (target_release, reused_bg, reused_cover),
                )
                preview = background_assignment_service.preview_background_assignment(
                    conn,
                    release_id=target_release,
                    background_asset_id=reused_bg,
                    source_family=None,
                    source_reference=None,
                    template_assisted=False,
                    selected_by="operator",
                )
                background_assignment_service.approve_background_assignment(
                    conn,
                    release_id=target_release,
                    preview_id=str(preview["preview_id"]),
                    approved_by="operator",
                )
                with self.assertRaises(background_assignment_service.BackgroundAssignmentError):
                    background_assignment_service.apply_background_assignment(
                        conn, release_id=target_release, applied_by="operator", reuse_override_confirmed=False
                    )
                background_assignment_service.apply_background_assignment(
                    conn, release_id=target_release, applied_by="operator", reuse_override_confirmed=True
                )
                history = conn.execute(
                    "SELECT reuse_warning_json, decision_mode, preview_id FROM release_visual_history_events WHERE release_id = ? AND history_stage = 'APPLIED' ORDER BY id DESC LIMIT 1",
                    (target_release,),
                ).fetchone()
                self.assertIsNotNone(history)
                reuse_json = json.loads(str(history["reuse_warning_json"]) or "{}")
                self.assertTrue(reuse_json.get("requires_override"))
                self.assertTrue(reuse_json.get("override_confirmed"))
                self.assertTrue(reuse_json.get("override_applied"))
                self.assertGreaterEqual(len(reuse_json.get("prior_usage") or []), 1)
                self.assertTrue(str(history["decision_mode"]) in {"manual", "auto_assisted"})
                self.assertTrue(str(history["preview_id"]).startswith("vbg-"))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
