from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.integrations.gdrive import DriveItem

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class FakeDrive:
    def __init__(self, tree: dict[str, list[DriveItem]]):
        self.tree = tree

    def list_children(self, parent_id: str):
        return list(self.tree.get(parent_id, []))


class TestUiJobsRenderAllSlice3(unittest.TestCase):
    def test_render_all_enqueues_ui_drafts_and_creates_inputs(self) -> None:
        with temp_env() as (_, _):
            os.environ["GDRIVE_ROOT_ID"] = "root"
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch
                ok_job = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(ch["id"]),
                    title="OK Job",
                    description="",
                    tags_csv="one,two",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001 015",
                )
                bad_job = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(ch["id"]),
                    title="Bad Job",
                    description="",
                    tags_csv="",
                    cover_name="",
                    cover_ext="",
                    background_name="missing",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
            finally:
                conn.close()

            tree = {
                "root": [DriveItem(id="project", name="Darkwood Reverie", mime_type="application/vnd.google-apps.folder")],
                "project": [
                    DriveItem(id="image", name="Image", mime_type="application/vnd.google-apps.folder"),
                    DriveItem(id="covers", name="Covers", mime_type="application/vnd.google-apps.folder"),
                    DriveItem(id="audio", name="Audio", mime_type="application/vnd.google-apps.folder"),
                ],
                "image": [DriveItem(id="bgf", name="bg.jpg", mime_type="image/jpeg")],
                "covers": [DriveItem(id="covf", name="cover.png", mime_type="image/png")],
                "audio": [DriveItem(id="d1", name="Feb26", mime_type="application/vnd.google-apps.folder")],
                "d1": [
                    DriveItem(id="a1", name="001_Title.wav", mime_type="audio/wav"),
                    DriveItem(id="a2", name="015_Title.wav", mime_type="audio/wav"),
                ],
            }

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            mod._create_drive_client = lambda _env: FakeDrive(tree)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            rr = client.post("/v1/ui/jobs/render_all", headers=h)
            self.assertEqual(rr.status_code, 200)
            self.assertEqual(rr.json()["enqueued_count"], 1)
            self.assertEqual(rr.json()["failed_count"], 1)

            conn2 = dbm.connect(env)
            try:
                ok = dbm.get_job(conn2, ok_job)
                bad = dbm.get_job(conn2, bad_job)
                self.assertEqual(ok["state"], "READY_FOR_RENDER")
                self.assertEqual(ok["stage"], "FETCH")
                self.assertTrue(conn2.execute("SELECT 1 FROM job_inputs WHERE job_id=?", (ok_job,)).fetchone())

                roles = [r["role"] for r in conn2.execute("SELECT role FROM job_inputs WHERE job_id=? ORDER BY role, order_index", (ok_job,)).fetchall()]
                self.assertIn("BACKGROUND", roles)
                self.assertIn("TRACK", roles)
                self.assertIn("COVER", roles)

                self.assertEqual(bad["state"], "DRAFT")
                self.assertTrue(str(bad.get("error_reason") or ""))
            finally:
                conn2.close()


if __name__ == "__main__":
    unittest.main()
