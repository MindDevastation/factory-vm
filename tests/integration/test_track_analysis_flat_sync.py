from __future__ import annotations

import tempfile
import unittest
import wave
from pathlib import Path
from unittest import mock

import numpy as np

from services.common import db as dbm
from services.track_analyzer.analyze import analyze_tracks
from services.track_analyzer.track_analysis_flat import sync_track_analysis_flat


class TestTrackAnalysisFlatSync(unittest.TestCase):
    class _FakeDrive:
        def download_to_path(self, file_id: str, dest: Path) -> None:
            del file_id
            dest.parent.mkdir(parents=True, exist_ok=True)
            sample_rate = 16000
            t = np.linspace(0.0, 1.0, sample_rate, endpoint=False, dtype=np.float32)
            waveform = 0.2 * np.sin(2.0 * np.pi * 220.0 * t)
            pcm = np.clip(waveform * 32767.0, -32768.0, 32767.0).astype(np.int16)
            with wave.open(str(dest), "wb") as wav_file:
                wav_file.setnchannels(1)
                wav_file.setsampwidth(2)
                wav_file.setframerate(sample_rate)
                wav_file.writeframes(pcm.tobytes())

    def test_sync_upserts_flat_row(self) -> None:
        conn = dbm.connect(type("E", (), {"db_path": ":memory:"})())
        try:
            dbm.migrate(conn)
            conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001.wav", "A", None, None, dbm.now_ts(), None),
            )
            track_row = conn.execute("SELECT * FROM tracks WHERE track_id = ?", ("001",)).fetchone()
            assert track_row is not None

            sync_track_analysis_flat(
                conn,
                track_row=track_row,
                features_payload={
                    "analysis_status": "COMPLETE",
                    "duration_sec": 12.5,
                    "true_peak_dbfs": -2.1,
                    "spikes_found": True,
                    "yamnet_top_classes": [{"label": "Music", "score": 0.95}],
                    "voice_flag": True,
                    "voice_flag_reason": "voice_prob=0.9",
                    "speech_flag": False,
                    "speech_flag_reason": "speech_prob=0.0",
                    "dominant_texture": "smooth",
                    "texture_confidence": 0.9,
                    "texture_reason": "ok",
                    "advanced_v1": {"meta": {"analyzer_version": "adv", "schema_version": "v1"}},
                },
                tags_payload={
                    "yamnet_tags": ["Music"],
                    "prohibited_cues_notes": "No prohibited cues detected by fallback analyzer.",
                    "prohibited_cues": {"flags": {"clipping_detected": False}},
                },
                scores_payload={"dsp_score": 0.8, "dsp_score_version": "v1", "dsp_notes": "weighted components"},
                analysis_computed_at=1234.5,
            )

            flat = conn.execute("SELECT * FROM track_analysis_flat WHERE track_pk = ?", (int(track_row["id"]),)).fetchone()
            self.assertIsNotNone(flat)
            assert flat is not None
            self.assertEqual(flat["analysis_status"], "COMPLETE")
            self.assertEqual(flat["channel_slug"], "darkwood-reverie")
            self.assertEqual(flat["yamnet_top_tags_text"], "Music")
            self.assertEqual(flat["dsp_score_version"], "v1")
            self.assertEqual(flat["voice_flag"], 1)
            self.assertEqual(flat["speech_flag"], 0)
        finally:
            conn.close()

    def test_analyze_writes_flat_row_after_payload_upserts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
                )
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                cur = conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001.wav", "A", None, None, dbm.now_ts(), None),
                )
                track_pk = int(cur.lastrowid)

                with mock.patch("services.track_analyzer.analyze.ffmpeg.ffprobe_json", return_value={"format": {"duration": "12.5"}}), mock.patch(
                    "services.track_analyzer.analyze.ffmpeg.run",
                    return_value=(0, "", "[Parsed_ebur128_0] Peak: -2.1 dB"),
                ), mock.patch(
                    "services.track_analyzer.analyze.yamnet.analyze_with_yamnet",
                    return_value={"top_classes": [{"label": "Music", "score": 0.95}], "probabilities": {"speech": 0.0, "voice": 0.0}},
                ):
                    stats = analyze_tracks(
                        conn,
                        self._FakeDrive(),
                        channel_slug="darkwood-reverie",
                        storage_root=td,
                        job_id=42,
                        scope="pending",
                        force=False,
                        max_tracks=10,
                    )
                self.assertEqual(stats.processed, 1)

                flat = conn.execute("SELECT * FROM track_analysis_flat WHERE track_pk = ?", (track_pk,)).fetchone()
                self.assertIsNotNone(flat)
                assert flat is not None
                self.assertEqual(flat["analysis_status"], "COMPLETE")
                self.assertEqual(flat["track_id"], "001")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
