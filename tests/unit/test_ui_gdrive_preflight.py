from __future__ import annotations

import os
import unittest

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.ui_gdrive import run_preflight_for_job
from services.integrations.gdrive import DriveItem

from tests._helpers import seed_minimal_db, temp_env


EXPECTED_ERROR_KEYS = {"project", "title", "audio", "background", "cover", "tags"}


class FakeDrive:
    def __init__(self, tree: dict[str, list[DriveItem]]):
        self.tree = tree

    def list_children(self, parent_id: str):
        return list(self.tree.get(parent_id, []))


class TestUiGdrivePreflight(unittest.TestCase):
    def _seed_job(self, env: Env) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            job_id = dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title="Title",
                description="",
                tags_csv="",
                cover_name="cover",
                cover_ext="png",
                background_name="bg",
                background_ext="jpg",
                audio_ids_text="001 015",
                job_type="UI",
            )
            return job_id
        finally:
            conn.close()

    def test_preflight_success(self) -> None:
        with temp_env() as (_, _):
            os.environ["GDRIVE_ROOT_ID"] = "root"
            env = Env.load()
            seed_minimal_db(env)
            job_id = self._seed_job(env)

            tree = {
                "root": [DriveItem(id="project", name="Darkwood Reverie", mime_type="application/vnd.google-apps.folder")],
                "project": [
                    DriveItem(id="image", name="Image", mime_type="application/vnd.google-apps.folder"),
                    DriveItem(id="covers", name="Covers", mime_type="application/vnd.google-apps.folder"),
                    DriveItem(id="audio", name="Audio", mime_type="application/vnd.google-apps.folder"),
                ],
                "image": [DriveItem(id="bgf", name="BG.jpg", mime_type="image/jpeg")],
                "covers": [DriveItem(id="covf", name="cover.png", mime_type="image/png")],
                "audio": [DriveItem(id="d1", name="Feb26", mime_type="application/vnd.google-apps.folder")],
                "d1": [
                    DriveItem(id="a1", name="001_Title.wav", mime_type="audio/wav"),
                    DriveItem(id="a2", name="015_Title.wav", mime_type="audio/wav"),
                ],
            }

            conn = dbm.connect(env)
            try:
                res = run_preflight_for_job(conn, env, job_id, drive=FakeDrive(tree))
            finally:
                conn.close()

            self.assertTrue(res.ok)
            self.assertEqual(set(res.field_errors.keys()), EXPECTED_ERROR_KEYS)
            self.assertTrue(all(res.field_errors[k] == [] for k in EXPECTED_ERROR_KEYS))
            self.assertEqual(res.resolved["background_file_id"], "bgf")
            self.assertEqual(res.resolved["background_filename"], "BG.jpg")
            self.assertEqual(res.resolved["cover_file_id"], "covf")
            self.assertEqual(res.resolved["cover_filename"], "cover.png")
            self.assertEqual(res.resolved["track_file_ids"], ["a1", "a2"])
            self.assertEqual(
                res.resolved["tracks"],
                [
                    {"file_id": "a1", "filename": "001_Title.wav"},
                    {"file_id": "a2", "filename": "015_Title.wav"},
                ],
            )

    def test_preflight_missing_root_sets_error_reason(self) -> None:
        with temp_env() as (_, _):
            os.environ.pop("GDRIVE_ROOT_ID", None)
            env = Env.load()
            seed_minimal_db(env)
            job_id = self._seed_job(env)

            conn = dbm.connect(env)
            try:
                res = run_preflight_for_job(conn, env, job_id, drive=FakeDrive({}))
                job = dbm.get_job(conn, job_id)
            finally:
                conn.close()

            self.assertFalse(res.ok)
            self.assertIn("GDRIVE_ROOT_ID is not configured", res.field_errors["project"])
            self.assertIsNotNone(job)
            self.assertEqual(str(job.get("error_reason") or ""), "GDRIVE_ROOT_ID is not configured")

    def test_preflight_audio_match_errors(self) -> None:
        with temp_env() as (_, _):
            os.environ["GDRIVE_ROOT_ID"] = "root"
            env = Env.load()
            seed_minimal_db(env)
            job_id = self._seed_job(env)

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
                    DriveItem(id="a1b", name="001_Alt.wav", mime_type="audio/wav"),
                ],
            }

            conn = dbm.connect(env)
            try:
                res = run_preflight_for_job(conn, env, job_id, drive=FakeDrive(tree))
            finally:
                conn.close()

            self.assertFalse(res.ok)
            self.assertEqual(set(res.field_errors.keys()), EXPECTED_ERROR_KEYS)
            self.assertIn("matches=2", res.field_errors["audio"][0])
            self.assertTrue(all(res.field_errors[k] == [] for k in EXPECTED_ERROR_KEYS if k != "audio"))

    def test_preflight_audio_missing_match_error(self) -> None:
        with temp_env() as (_, _):
            os.environ["GDRIVE_ROOT_ID"] = "root"
            env = Env.load()
            seed_minimal_db(env)
            job_id = self._seed_job(env)

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
                "d1": [DriveItem(id="a1", name="001_Title.wav", mime_type="audio/wav")],
            }

            conn = dbm.connect(env)
            try:
                res = run_preflight_for_job(conn, env, job_id, drive=FakeDrive(tree))
            finally:
                conn.close()

            self.assertFalse(res.ok)
            self.assertEqual(set(res.field_errors.keys()), EXPECTED_ERROR_KEYS)
            self.assertIn("matches=0", res.field_errors["audio"][0])
            self.assertTrue(all(res.field_errors[k] == [] for k in EXPECTED_ERROR_KEYS if k != "audio"))


if __name__ == "__main__":
    unittest.main()
