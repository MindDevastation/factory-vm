from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch
from pathlib import Path

from services.common import db as dbm
from services.common.env import Env
from services.workers.importer import importer_cycle

from tests._helpers import temp_env, seed_minimal_db


class TestImporterLocal(unittest.TestCase):
    def test_importer_creates_release_job_and_inputs(self) -> None:
        with temp_env() as (td, _env0):
            origin_root = Path(td.name) / "origin"
            os.environ["ORIGIN_LOCAL_ROOT"] = str(origin_root)
            os.environ["ORIGIN_BACKEND"] = "local"
            env = Env.load()

            seed_minimal_db(env)

            # Create local release structure
            rel_dir = origin_root / "channels" / "darkwood-reverie" / "incoming" / "rel1"
            (rel_dir / "audio").mkdir(parents=True, exist_ok=True)
            (rel_dir / "images").mkdir(parents=True, exist_ok=True)

            (rel_dir / "audio" / "track1.wav").write_bytes(b"RIFF0000WAVEfmt ")
            (rel_dir / "images" / "cover.png").write_bytes(b"\x89PNG\r\n\x1a\n")

            meta = {
                "title": "DEV Smoke Test",
                "description": "d",
                "tags": ["a"],
                "assets": {"audio": ["audio/track1.wav"], "cover": "images/cover.png"},
            }
            (rel_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

            with patch("services.common.config.load_channels", side_effect=RuntimeError("yaml runtime read forbidden")):
                importer_cycle(env=env, worker_id="t-imp")

            conn = dbm.connect(env)
            try:
                releases = conn.execute("SELECT * FROM releases").fetchall()
                self.assertEqual(len(releases), 1)
                jobs = conn.execute("SELECT * FROM jobs").fetchall()
                self.assertEqual(len(jobs), 1)
                job_id = int(jobs[0]["id"])
                inputs = conn.execute("SELECT * FROM job_inputs WHERE job_id=?", (job_id,)).fetchall()
                # 1 track + 1 cover
                self.assertEqual(len(inputs), 2)
            finally:
                conn.close()

            # Re-run importer: should not create duplicates
            importer_cycle(env=env, worker_id="t-imp")
            conn2 = dbm.connect(env)
            try:
                self.assertEqual(int(conn2.execute("SELECT COUNT(1) AS n FROM releases").fetchone()["n"]), 1)
                self.assertEqual(int(conn2.execute("SELECT COUNT(1) AS n FROM jobs").fetchone()["n"]), 1)
            finally:
                conn2.close()

    def test_importer_skips_out_of_folder_asset_paths(self) -> None:
        with temp_env() as (td, _env0):
            origin_root = Path(td.name) / "origin"
            os.environ["ORIGIN_LOCAL_ROOT"] = str(origin_root)
            os.environ["ORIGIN_BACKEND"] = "local"
            env = Env.load()

            seed_minimal_db(env)

            rel_dir = origin_root / "channels" / "darkwood-reverie" / "incoming" / "rel-unsafe"
            (rel_dir / "audio").mkdir(parents=True, exist_ok=True)
            (rel_dir / "images").mkdir(parents=True, exist_ok=True)
            (rel_dir / "audio" / "track-safe.wav").write_bytes(b"RIFF0000WAVEfmt ")
            (rel_dir / "images" / "cover-safe.png").write_bytes(b"\x89PNG\r\n\x1a\n")

            outside_audio = rel_dir.parent / "outside.wav"
            outside_cover = rel_dir.parent / "outside.png"
            outside_audio.write_bytes(b"RIFF0000WAVEfmt ")
            outside_cover.write_bytes(b"\x89PNG\r\n\x1a\n")

            meta = {
                "title": "Unsafe Path Test",
                "description": "d",
                "tags": ["a"],
                "assets": {
                    "audio": ["audio/track-safe.wav", "../outside.wav"],
                    "cover": "../outside.png",
                },
            }
            (rel_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

            with patch("services.common.config.load_channels", side_effect=RuntimeError("yaml runtime read forbidden")):
                importer_cycle(env=env, worker_id="t-imp-unsafe")

            conn = dbm.connect(env)
            try:
                job = conn.execute("SELECT id FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
                assert job is not None
                job_id = int(job["id"])
                assets = conn.execute("SELECT origin_id FROM assets ORDER BY id ASC").fetchall()
                self.assertTrue(all(str(rel_dir.resolve()) in str(a["origin_id"]) for a in assets))
                inputs = conn.execute("SELECT role, order_index FROM job_inputs WHERE job_id=? ORDER BY role, order_index", (job_id,)).fetchall()
                self.assertEqual(len(inputs), 1)
                self.assertEqual(str(inputs[0]["role"]), "TRACK")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
