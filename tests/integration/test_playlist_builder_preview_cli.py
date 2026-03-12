from __future__ import annotations

import json
import unittest

from services.common import db as dbm
from services.playlist_builder.core import PlaylistBuilder, resolve_effective_brief_for_job
from scripts.playlist_builder_preview import run_preview
from tests._helpers import seed_minimal_db, temp_env


class PlaylistBuilderPreviewCliIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self._ctx = temp_env()
        _, self.env = self._ctx.__enter__()
        seed_minimal_db(self.env)
        self.conn = dbm.connect(self.env)
        dbm.migrate(self.conn)
        ch = dbm.get_channel_by_slug(self.conn, "darkwood-reverie")
        assert ch
        self.job_id = dbm.create_ui_job_draft(
            self.conn,
            channel_id=int(ch["id"]),
            title="PB",
            description="PB",
            tags_csv="",
            cover_name=None,
            cover_ext=None,
            background_name="bg",
            background_ext="png",
            audio_ids_text="",
        )
        ts = dbm.now_ts()
        for pk, duration in [(101, 260.0), (102, 270.0), (103, 280.0)]:
            self.conn.execute(
                "INSERT INTO tracks(id, channel_slug, track_id, gdrive_file_id, duration_sec, month_batch, discovered_at, analyzed_at) VALUES(?,?,?,?,?,?,?,?)",
                (pk, "darkwood-reverie", f"t{pk}", f"g{pk}", duration, "2024-01", ts, ts),
            )
            self.conn.execute(
                "INSERT INTO track_analysis_flat(track_pk, channel_slug, track_id, analysis_computed_at, analysis_status, duration_sec, yamnet_top_tags_text, voice_flag, speech_flag, dominant_texture, dsp_score, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))",
                (pk, "darkwood-reverie", f"t{pk}", ts, "ok", duration, "calm,ambient", 0, 0, "smooth", 0.5),
            )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()
        self._ctx.__exit__(None, None, None)

    def test_core_and_cli_adapter_are_consistent(self) -> None:
        brief = resolve_effective_brief_for_job(self.conn, job_id=self.job_id, request_override={"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 20})
        core = PlaylistBuilder().generate_preview(self.conn, brief).model_dump()

        payload = run_preview(job_id=self.job_id, override_json=json.dumps({"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 20}))
        self.assertEqual(core["ordered_track_pks"], payload["preview"]["ordered_track_pks"])
        self.assertEqual(payload["brief"]["generation_mode"], "safe")


if __name__ == "__main__":
    unittest.main()
