from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.planner.runtime_visual_resolver import apply_release_visual_package
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestEpic5VisualSmoke(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def test_visual_workflow_ui_surface_smoke(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="epic5 smoke ui",
                    description="d",
                    tags_csv="a",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="png",
                    audio_ids_text="1",
                )
            finally:
                conn.close()

            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            page = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(page.status_code, 200)
            self.assertIn('id="visual-workflow-section"', page.text)
            self.assertIn("Visual Workflow (Epic 5 primary)", page.text)
            self.assertIn("Cover Apply", page.text)

    def test_visual_background_candidates_no_telegram_dependency_smoke(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="epic5 smoke tg-independence",
                    description="d",
                    tags_csv="a",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="png",
                    audio_ids_text="1",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://epic5-smoke-bg",
                    name="epic5-smoke-bg.png",
                    path="/tmp/epic5-smoke-bg.png",
                )
                # Keep at least one known-resolved candidate path available for endpoint payload.
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://epic5-smoke-cover",
                    name="epic5-smoke-cover.png",
                    path="/tmp/epic5-smoke-cover.png",
                )
                apply_release_visual_package(
                    conn,
                    release_id=release_id,
                    background_asset_id=bg_asset_id,
                    cover_asset_id=cover_asset_id,
                    source_preview_id=None,
                    applied_by="seed",
                )
            finally:
                conn.close()

            os.environ.pop("TELEGRAM_BOT_TOKEN", None)
            os.environ.pop("TELEGRAM_CHAT_ID", None)
            os.environ.pop("TELEGRAM_NOTIFICATIONS_ENABLED", None)

            client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            r = client.get(f"/v1/visual/releases/{release_id}/background/candidates", headers=h)
            self.assertEqual(r.status_code, 200)
            payload = r.json()
            self.assertEqual(int(payload["release_id"]), release_id)
            self.assertIn("candidates", payload)

    def test_runtime_background_cover_bindings_contract_smoke(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="epic5 smoke runtime",
                    description="d",
                    tags_csv="a",
                    cover_name="cover-old",
                    cover_ext="png",
                    background_name="bg-old",
                    background_ext="png",
                    audio_ids_text="1",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))

                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://epic5-smoke-runtime-bg",
                    name="epic5-smoke-runtime-bg.png",
                    path="/tmp/epic5-smoke-runtime-bg.png",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://epic5-smoke-runtime-cover",
                    name="epic5-smoke-runtime-cover.png",
                    path="/tmp/epic5-smoke-runtime-cover.png",
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

                rows = conn.execute(
                    "SELECT role, asset_id FROM job_inputs WHERE job_id = ? AND role IN ('BACKGROUND','COVER') ORDER BY role ASC",
                    (job_id,),
                ).fetchall()
                self.assertEqual(
                    [(str(r["role"]), int(r["asset_id"])) for r in rows],
                    [("BACKGROUND", bg_asset_id), ("COVER", cover_asset_id)],
                )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
