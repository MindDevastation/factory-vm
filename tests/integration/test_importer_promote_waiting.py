from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from services.common import db as dbm
from services.common.env import Env
from services.workers.importer import importer_cycle

from tests._helpers import temp_env, seed_minimal_db


class TestImporterPromoteWaiting(unittest.TestCase):
    def test_waiting_inputs_promoted_when_assets_appear(self) -> None:
        with temp_env() as (td, _env0):
            origin_root = Path(td.name) / "origin"
            os.environ["ORIGIN_LOCAL_ROOT"] = str(origin_root)
            os.environ["ORIGIN_BACKEND"] = "local"
            env = Env.load()
            seed_minimal_db(env)

            rel_dir = origin_root / "channels" / "darkwood-reverie" / "incoming" / "rel_wait"
            rel_dir.mkdir(parents=True, exist_ok=True)

            meta = {
                "title": "Needs inputs",
                "description": "d",
                "tags": [],
                "assets": {"audio": ["audio/track1.wav"], "cover": "images/cover.png"},
            }
            (rel_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

            # first run: no audio/images dirs => WAITING_INPUTS
            importer_cycle(env=env, worker_id="t-imp")

            conn = dbm.connect(env)
            try:
                job = conn.execute("SELECT id, state FROM jobs").fetchone()
                assert job is not None
                job_id = int(job["id"])
                self.assertEqual(job["state"], "WAITING_INPUTS")
            finally:
                conn.close()

            # create assets
            (rel_dir / "audio").mkdir(parents=True, exist_ok=True)
            (rel_dir / "images").mkdir(parents=True, exist_ok=True)
            (rel_dir / "audio" / "track1.wav").write_bytes(b"RIFF0000WAVEfmt ")
            (rel_dir / "images" / "cover.png").write_bytes(b"\x89PNG\r\n\x1a\n")

            # second run: should attach inputs + promote
            importer_cycle(env=env, worker_id="t-imp")

            conn2 = dbm.connect(env)
            try:
                job2 = conn2.execute("SELECT id, state FROM jobs WHERE id=?", (job_id,)).fetchone()
                assert job2 is not None
                self.assertEqual(job2["state"], "READY_FOR_RENDER")
                n = conn2.execute("SELECT COUNT(1) AS n FROM job_inputs WHERE job_id=?", (job_id,)).fetchone()
                self.assertEqual(int(n["n"]), 2)
            finally:
                conn2.close()


if __name__ == "__main__":
    unittest.main()
