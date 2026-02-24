from __future__ import annotations

import json
import os
import unittest
from dataclasses import dataclass
from typing import Dict, List, Optional

from services.common.env import Env
from services.common import db as dbm
from services.workers import importer
from services.integrations.gdrive import DriveItem

from tests._helpers import seed_minimal_db, temp_env


@dataclass
class _FakeDrive:
    children: Dict[str, List[DriveItem]]
    texts: Dict[str, str]

    def list_children(self, parent_id: str) -> List[DriveItem]:
        return list(self.children.get(parent_id, []))

    def find_child_folder(self, parent_id: str, name: str) -> Optional[DriveItem]:
        for it in self.list_children(parent_id):
            if it.mime_type == "application/vnd.google-apps.folder" and it.name == name:
                return it
        return None

    def find_child_file(self, parent_id: str, name: str) -> Optional[DriveItem]:
        for it in self.list_children(parent_id):
            if it.mime_type != "application/vnd.google-apps.folder" and it.name == name:
                return it
        return None

    def download_text(self, file_id: str) -> str:
        return self.texts[file_id]


class TestImporterGDriveMocked(unittest.TestCase):
    def _env_gdrive(self):
        ctx = temp_env()
        td, _env = ctx.__enter__()
        self.addCleanup(lambda: ctx.__exit__(None, None, None))

        os.environ["ORIGIN_BACKEND"] = "gdrive"
        os.environ["GDRIVE_ROOT_ID"] = "root"
        env = Env.load()
        seed_minimal_db(env)
        return td, env

    def test_gdrive_import_new_release_ready_and_assets_attached(self):
        _td, env = self._env_gdrive()

        channels = DriveItem(id="channels", name="channels", mime_type="application/vnd.google-apps.folder")
        ch = DriveItem(id="ch1", name="darkwood-reverie", mime_type="application/vnd.google-apps.folder")
        incoming = DriveItem(id="incoming", name="incoming", mime_type="application/vnd.google-apps.folder")
        rel = DriveItem(id="rel1", name="rel", mime_type="application/vnd.google-apps.folder")
        meta = DriveItem(id="meta1", name="meta.json", mime_type="application/json")
        audio = DriveItem(id="audio1", name="audio", mime_type="application/vnd.google-apps.folder")
        images = DriveItem(id="img1", name="images", mime_type="application/vnd.google-apps.folder")
        t1 = DriveItem(id="a1", name="track_1.wav", mime_type="audio/wav")
        cover = DriveItem(id="c1", name="cover.png", mime_type="image/png")

        meta_obj = {
            "title": "Smoke",
            "description": "d",
            "tags": ["#a"],
            "assets": {"audio": ["track_1.wav"], "cover": "cover.png"},
        }

        drive = _FakeDrive(
            children={
                "root": [channels],
                "channels": [ch],
                "ch1": [incoming],
                "incoming": [rel],
                "rel1": [meta, audio, images],
                "audio1": [t1],
                "img1": [cover],
            },
            texts={"meta1": json.dumps(meta_obj)},
        )

        with unittest.mock.patch.object(importer, "DriveClient", lambda **_kw: drive):
            importer.importer_cycle(env=env, worker_id="importer:1")

        conn = dbm.connect(env)
        try:
            job = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(job["state"], "READY_FOR_RENDER")
            job_id = int(job["id"])
            n_inputs = conn.execute("SELECT COUNT(1) AS n FROM job_inputs WHERE job_id=?", (job_id,)).fetchone()
            self.assertEqual(int(n_inputs["n"]), 2)
        finally:
            conn.close()

    def test_gdrive_meta_parse_fail_skips_release(self):
        _td, env = self._env_gdrive()

        channels = DriveItem(id="channels", name="channels", mime_type="application/vnd.google-apps.folder")
        ch = DriveItem(id="ch1", name="darkwood-reverie", mime_type="application/vnd.google-apps.folder")
        incoming = DriveItem(id="incoming", name="incoming", mime_type="application/vnd.google-apps.folder")
        rel = DriveItem(id="rel1", name="rel", mime_type="application/vnd.google-apps.folder")
        meta = DriveItem(id="meta1", name="meta.json", mime_type="application/json")

        drive = _FakeDrive(
            children={"root": [channels], "channels": [ch], "ch1": [incoming], "incoming": [rel], "rel1": [meta]},
            texts={"meta1": "{bad"},
        )

        with unittest.mock.patch.object(importer, "DriveClient", lambda **_kw: drive):
            importer.importer_cycle(env=env, worker_id="importer:1")

        conn = dbm.connect(env)
        try:
            n = conn.execute("SELECT COUNT(1) AS n FROM releases").fetchone()
            self.assertEqual(int(n["n"]), 0)
        finally:
            conn.close()

    def test_gdrive_existing_waiting_inputs_is_promoted(self):
        _td, env = self._env_gdrive()
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            ts = dbm.now_ts()
            cur = conn.execute(
                "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
                (int(ch["id"]), "Smoke", "d", "[]", None, "rel1", "meta1", ts),
            )
            release_id = int(cur.lastrowid)
            cur2 = conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, priority, attempt, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?)",
                (release_id, "RENDER_LONG", "WAITING_INPUTS", "FETCH", 1, 0, ts, ts),
            )
            job_id = int(cur2.lastrowid)
        finally:
            conn.close()

        channels = DriveItem(id="channels", name="channels", mime_type="application/vnd.google-apps.folder")
        chf = DriveItem(id="ch1", name="darkwood-reverie", mime_type="application/vnd.google-apps.folder")
        incoming = DriveItem(id="incoming", name="incoming", mime_type="application/vnd.google-apps.folder")
        rel = DriveItem(id="rel1", name="rel", mime_type="application/vnd.google-apps.folder")
        meta = DriveItem(id="meta1", name="meta.json", mime_type="application/json")
        audio = DriveItem(id="audio1", name="audio", mime_type="application/vnd.google-apps.folder")
        images = DriveItem(id="img1", name="images", mime_type="application/vnd.google-apps.folder")
        t1 = DriveItem(id="a1", name="track_1.wav", mime_type="audio/wav")
        cover = DriveItem(id="c1", name="cover.png", mime_type="image/png")

        meta_obj = {"title": "Smoke", "description": "d", "tags": [], "assets": {"audio": ["track_1.wav"], "cover": "cover.png"}}
        drive = _FakeDrive(
            children={
                "root": [channels],
                "channels": [chf],
                "ch1": [incoming],
                "incoming": [rel],
                "rel1": [meta, audio, images],
                "audio1": [t1],
                "img1": [cover],
            },
            texts={"meta1": json.dumps(meta_obj)},
        )

        with unittest.mock.patch.object(importer, "DriveClient", lambda **_kw: drive):
            importer.importer_cycle(env=env, worker_id="importer:1")

        conn = dbm.connect(env)
        try:
            job = dbm.get_job(conn, job_id)
            self.assertEqual(job["state"], "READY_FOR_RENDER")
            n_inputs = conn.execute("SELECT COUNT(1) AS n FROM job_inputs WHERE job_id=?", (job_id,)).fetchone()
            self.assertGreater(int(n_inputs["n"]), 0)
        finally:
            conn.close()

    def test_gdrive_missing_audio_or_images_creates_waiting_inputs(self):
        _td, env = self._env_gdrive()

        channels = DriveItem(id="channels", name="channels", mime_type="application/vnd.google-apps.folder")
        ch = DriveItem(id="ch1", name="darkwood-reverie", mime_type="application/vnd.google-apps.folder")
        incoming = DriveItem(id="incoming", name="incoming", mime_type="application/vnd.google-apps.folder")
        rel = DriveItem(id="rel1", name="rel", mime_type="application/vnd.google-apps.folder")
        meta = DriveItem(id="meta1", name="meta.json", mime_type="application/json")

        meta_obj = {"title": "Smoke", "description": "d", "tags": []}
        drive = _FakeDrive(
            children={"root": [channels], "channels": [ch], "ch1": [incoming], "incoming": [rel], "rel1": [meta]},
            texts={"meta1": json.dumps(meta_obj)},
        )

        with unittest.mock.patch.object(importer, "DriveClient", lambda **_kw: drive):
            importer.importer_cycle(env=env, worker_id="importer:1")

        conn = dbm.connect(env)
        try:
            job = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(job["state"], "WAITING_INPUTS")
        finally:
            conn.close()
