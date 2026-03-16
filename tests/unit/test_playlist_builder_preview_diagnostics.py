from __future__ import annotations

import unittest

from services.common import db as dbm
from services.playlist_builder.models import PlaylistBrief
from services.playlist_builder.workflow import PlaylistBuilderApiError, build_preview_response, create_preview, create_preview_for_brief
from tests._helpers import temp_env, seed_minimal_db


class PlaylistBuilderPreviewDiagnosticsTest(unittest.TestCase):
    def setUp(self) -> None:
        self._ctx = temp_env()
        _, self.env = self._ctx.__enter__()
        seed_minimal_db(self.env)
        self.conn = dbm.connect(self.env)
        dbm.migrate(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self._ctx.__exit__(None, None, None)

    def _insert_analyzed_track(self, *, pk: int, channel_slug: str, analyzed_at: float | None, batch: str = "2024-01") -> None:
        ts = dbm.now_ts()
        self.conn.execute(
            "INSERT INTO tracks(id, channel_slug, track_id, gdrive_file_id, duration_sec, month_batch, discovered_at, analyzed_at) VALUES(?,?,?,?,?,?,?,?)",
            (pk, channel_slug, f"t{pk}", f"g{pk}", 240.0, batch, ts, analyzed_at),
        )
        self.conn.execute(
            """
            INSERT INTO track_analysis_flat(
                track_pk, channel_slug, track_id, analysis_computed_at, analysis_status,
                duration_sec, yamnet_top_tags_text, voice_flag, speech_flag, dominant_texture,
                dsp_score, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (pk, channel_slug, f"t{pk}", ts, "ok", 240.0, "ambient,calm", 0, 0, "smooth", 0.4, "2024-01-01T00:00:00"),
        )

    def test_zero_candidate_preview_returns_diagnostics_and_reason(self) -> None:
        brief = PlaylistBrief(channel_slug="darkwood-reverie", required_tags=[], excluded_tags=[])
        with self.assertRaises(PlaylistBuilderApiError) as ctx:
            create_preview_for_brief(self.conn, brief=brief)

        self.assertEqual(ctx.exception.message, "No analyzed eligible tracks found for this channel")
        self.assertEqual(ctx.exception.diagnostics.get("after_analyzed_eligible"), 0)
        self.assertEqual(ctx.exception.diagnostics.get("after_required_tags"), 0)

    def test_analyzed_eligible_uses_flat_analysis_even_if_tracks_analyzed_at_missing(self) -> None:
        self._insert_analyzed_track(pk=1, channel_slug="darkwood-reverie", analyzed_at=None)
        brief = PlaylistBrief(channel_slug="darkwood-reverie", required_tags=[], excluded_tags=[], min_duration_min=3, max_duration_min=5)

        envelope = create_preview_for_brief(self.conn, brief=brief)
        response = build_preview_response(envelope)

        self.assertEqual(response["summary"]["diagnostics"]["after_analyzed_eligible"], 1)
        self.assertEqual(response["summary"]["diagnostics"]["final_candidates"], 1)
        self.assertEqual(len(response["tracks"]), 1)

    def test_month_batch_is_soft_preference_not_hard_filter(self) -> None:
        self._insert_analyzed_track(pk=1, channel_slug="darkwood-reverie", analyzed_at=dbm.now_ts(), batch="2024-03")
        brief = PlaylistBrief(
            channel_slug="darkwood-reverie",
            required_tags=[],
            excluded_tags=[],
            preferred_month_batch="2024-01",
            min_duration_min=3,
            max_duration_min=5,
        )

        envelope = create_preview_for_brief(self.conn, brief=brief)
        diagnostics = build_preview_response(envelope)["summary"]["diagnostics"]

        self.assertEqual(diagnostics["after_month_batch_preference_or_filter"], 1)
        self.assertEqual(diagnostics["final_candidates"], 1)

    def test_create_flow_resolves_channel_from_job_context(self) -> None:
        channel = dbm.get_channel_by_slug(self.conn, "channel-b")
        assert channel is not None
        job_id = dbm.create_ui_job_draft(
            self.conn,
            channel_id=int(channel["id"]),
            title="x",
            description="x",
            tags_csv="",
            cover_name=None,
            cover_ext=None,
            background_name="bg",
            background_ext="png",
            audio_ids_text="1",
        )
        self._insert_analyzed_track(pk=1, channel_slug="channel-b", analyzed_at=dbm.now_ts())
        self.conn.commit()

        envelope = create_preview(self.conn, job_id=job_id, override={}, created_by="admin")
        diagnostics = build_preview_response(envelope)["summary"]["diagnostics"]

        self.assertEqual(diagnostics["resolved_channel_slug"], "channel-b")


if __name__ == "__main__":
    unittest.main()
