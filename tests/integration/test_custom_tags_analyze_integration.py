from __future__ import annotations

import json
import tempfile
import unittest
import wave
from pathlib import Path
from unittest import mock

import numpy as np

from services.common import db as dbm
from services.track_analyzer.analyze import analyze_tracks


class FakeDrive:
    def download_to_path(self, file_id: str, dest: Path) -> None:
        del file_id
        dest.parent.mkdir(parents=True, exist_ok=True)
        sample_rate = 16000
        seconds = 1.0
        t = np.linspace(0.0, seconds, int(sample_rate * seconds), endpoint=False, dtype=np.float32)
        waveform = 0.25 * np.sin(2.0 * np.pi * 220.0 * t)
        pcm = np.clip(waveform * 32767.0, -32768.0, 32767.0).astype(np.int16)
        with wave.open(str(dest), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(sample_rate)
            wav_file.writeframes(pcm.tobytes())


class TestCustomTagsAnalyzeIntegration(unittest.TestCase):
    def _seed_track(self, conn) -> int:
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
            ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001_A.wav", "A", None, None, dbm.now_ts(), None),
        )
        return int(cur.lastrowid)

    def _insert_tag(self, conn, *, code: str, category: str) -> int:
        cur = conn.execute(
            """
            INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            """,
            (code, code.title(), category, None, 1, "2025-01-01", "2025-01-01"),
        )
        return int(cur.lastrowid)

    def _insert_rule(self, conn, *, tag_id: int, source_path: str, operator: str, value: object) -> None:
        conn.execute(
            """
            INSERT INTO custom_tag_rules(
                tag_id, source_path, operator, value_json, match_mode,
                priority, weight, required, stop_after_match, is_active, created_at, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (tag_id, source_path, operator, json.dumps(value), "ALL", 100, None, 0, 0, 1, "2025-01-01", "2025-01-01"),
        )

    def _run_analyze(self, conn, *, td: str, yamnet_payload: dict, scope: str = "pending", force: bool = False) -> None:
        with mock.patch("services.track_analyzer.analyze.ffmpeg.ffprobe_json", return_value={"format": {"duration": "12.5"}}), mock.patch(
            "services.track_analyzer.analyze.ffmpeg.run",
            return_value=(0, "", "[Parsed_ebur128_0] Peak: -2.1 dB"),
        ), mock.patch("services.track_analyzer.analyze.yamnet.analyze_with_yamnet", return_value=yamnet_payload):
            stats = analyze_tracks(
                conn,
                FakeDrive(),
                channel_slug="darkwood-reverie",
                storage_root=td,
                job_id=808,
                scope=scope,
                force=force,
                max_tracks=10,
            )
        self.assertEqual(stats.failed, 0)
        self.assertEqual(stats.processed, 1)

    def test_analyze_empty_catalog_is_noop_for_custom_assignments(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                track_pk = self._seed_track(conn)

                self._run_analyze(
                    conn,
                    td=td,
                    yamnet_payload={
                        "top_classes": [{"label": "Music", "score": 0.8}],
                        "probabilities": {"speech": 0.0, "voice": 0.0, "music": 0.8},
                    },
                )

                cnt = conn.execute(
                    "SELECT COUNT(*) AS c FROM track_custom_tag_assignments WHERE track_pk = ?",
                    (track_pk,),
                ).fetchone()
                self.assertEqual(int(cnt["c"]), 0)
            finally:
                conn.close()

    def test_analyze_with_rule_creates_auto_assignment(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                track_pk = self._seed_track(conn)
                tag_id = self._insert_tag(conn, code="music_like", category="MOOD")
                self._insert_rule(
                    conn,
                    tag_id=tag_id,
                    source_path="track_tags.payload_json.yamnet_tags",
                    operator="contains",
                    value="Music",
                )

                self._run_analyze(
                    conn,
                    td=td,
                    yamnet_payload={
                        "top_classes": [{"label": "Music", "score": 0.9}],
                        "probabilities": {"speech": 0.0, "voice": 0.1, "music": 0.9},
                    },
                )

                assignment = conn.execute(
                    "SELECT state FROM track_custom_tag_assignments WHERE track_pk = ? AND tag_id = ?",
                    (track_pk, tag_id),
                ).fetchone()
                self.assertIsNotNone(assignment)
                self.assertEqual(str(assignment["state"]), "AUTO")

                feature_row = conn.execute("SELECT payload_json FROM track_features WHERE track_pk = ?", (track_pk,)).fetchone()
                self.assertIsNotNone(feature_row)
                features = dbm.json_loads(feature_row["payload_json"])
                self.assertIn("voice_flag", features)
            finally:
                conn.close()

    def test_rerun_updates_auto_and_preserves_manual_and_suppressed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                track_pk = self._seed_track(conn)
                auto_tag_id = self._insert_tag(conn, code="voice_auto", category="MOOD")
                manual_tag_id = self._insert_tag(conn, code="always_manual", category="MOOD")
                suppressed_tag_id = self._insert_tag(conn, code="always_suppressed", category="THEME")

                self._insert_rule(
                    conn,
                    tag_id=auto_tag_id,
                    source_path="track_features.payload_json.voice_flag",
                    operator="equals",
                    value=True,
                )
                self._insert_rule(
                    conn,
                    tag_id=manual_tag_id,
                    source_path="track_tags.payload_json.yamnet_tags",
                    operator="contains",
                    value="Music",
                )
                self._insert_rule(
                    conn,
                    tag_id=suppressed_tag_id,
                    source_path="track_tags.payload_json.yamnet_tags",
                    operator="contains",
                    value="Music",
                )

                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, manual_tag_id, "MANUAL", "2025-01-01", "2025-01-01"),
                )
                conn.execute(
                    "INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at) VALUES(?,?,?,?,?)",
                    (track_pk, suppressed_tag_id, "SUPPRESSED", "2025-01-01", "2025-01-01"),
                )

                with mock.patch("services.track_analyzer.analyze.ffmpeg.ffprobe_json", return_value={"format": {"duration": "12.5"}}), mock.patch(
                    "services.track_analyzer.analyze.ffmpeg.run",
                    return_value=(0, "", "[Parsed_ebur128_0] Peak: -2.1 dB"),
                ), mock.patch(
                    "services.track_analyzer.analyze.yamnet.analyze_with_yamnet",
                    side_effect=[
                        {
                            "top_classes": [{"label": "Music", "score": 0.9}],
                            "probabilities": {"speech": 0.0, "voice": 0.3, "music": 0.9},
                        },
                        {
                            "top_classes": [{"label": "Music", "score": 0.9}],
                            "probabilities": {"speech": 0.3, "voice": 0.05, "music": 0.9},
                        },
                    ],
                ):
                    first = analyze_tracks(
                        conn,
                        FakeDrive(),
                        channel_slug="darkwood-reverie",
                        storage_root=td,
                        job_id=809,
                        scope="pending",
                        force=False,
                        max_tracks=10,
                    )
                    second = analyze_tracks(
                        conn,
                        FakeDrive(),
                        channel_slug="darkwood-reverie",
                        storage_root=td,
                        job_id=810,
                        scope="all",
                        force=True,
                        max_tracks=10,
                    )

                self.assertEqual(first.processed, 1)
                self.assertEqual(second.processed, 1)

                rows = conn.execute(
                    "SELECT tag_id, state FROM track_custom_tag_assignments WHERE track_pk = ? ORDER BY tag_id ASC",
                    (track_pk,),
                ).fetchall()
                by_tag = {int(row["tag_id"]): str(row["state"]) for row in rows}

                self.assertNotIn(auto_tag_id, by_tag)
                self.assertEqual(by_tag[manual_tag_id], "MANUAL")
                self.assertEqual(by_tag[suppressed_tag_id], "SUPPRESSED")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
