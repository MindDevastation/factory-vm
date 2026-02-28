from __future__ import annotations

import os
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from services.common import db as dbm
from services.common.env import Env
from services.track_analyzer import track_jobs_db as tjdb
from services.workers.track_jobs import track_jobs_cycle
from tests._helpers import seed_minimal_db, temp_env

_FOLDER = "application/vnd.google-apps.folder"
_FILE = "audio/wav"


@dataclass
class _FakeItem:
    id: str
    name: str
    mime_type: str


class _FakeDrive:
    def __init__(self) -> None:
        self._children: dict[str, list[_FakeItem]] = {}

    def add_child(self, parent_id: str, item: _FakeItem) -> None:
        self._children.setdefault(parent_id, []).append(item)

    def list_children(self, parent_id: str):
        return list(self._children.get(parent_id, []))

    def update_name(self, file_id: str, new_name: str) -> None:
        for items in self._children.values():
            for item in items:
                if item.id == file_id:
                    item.name = new_name
                    return
        raise AssertionError(f"file not found: {file_id}")


class TestTrackDiscoverDisplayName(unittest.TestCase):
    def test_scan_tracks_resolves_channel_folder_by_display_name(self) -> None:
        with temp_env() as (_, _env0):
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = "/secure/gdrive/client_secret.json"
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                job_id = tjdb.enqueue_job(conn, job_type="SCAN_TRACKS", channel_slug="darkwood-reverie", payload={})
            finally:
                conn.close()

            drive = _FakeDrive()
            drive.add_child(env.gdrive_library_root_id, _FakeItem("ch", "Darkwood Reverie", _FOLDER))
            drive.add_child("ch", _FakeItem("audio", "Audio", _FOLDER))
            drive.add_child("audio", _FakeItem("m202501", "202501", _FOLDER))
            drive.add_child("m202501", _FakeItem("fid-1", "001_Title.wav", _FILE))

            with patch("services.workers.track_jobs._build_track_catalog_drive_client", lambda **_kw: drive):
                track_jobs_cycle(env=env, worker_id="t-track-jobs-discover-display-name")

            conn2 = dbm.connect(env)
            try:
                job = tjdb.get_job(conn2, job_id)
                assert job is not None
                self.assertEqual(job["status"], "DONE")

                row = conn2.execute(
                    "SELECT track_id, filename FROM tracks WHERE channel_slug = ? ORDER BY id ASC LIMIT 1",
                    ("darkwood-reverie",),
                ).fetchone()
                assert row is not None
                self.assertEqual(row["track_id"], "001")
                self.assertEqual(row["filename"], "001_Title.wav")
            finally:
                conn2.close()


if __name__ == "__main__":
    unittest.main()
