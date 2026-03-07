from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.track_analysis_report.registry import COLUMN_REGISTRY
from tests._helpers import basic_auth_header, temp_env


class TestTrackAnalysisReportApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _seed_channel(self, env) -> None:
        conn = dbm.connect(env)
        try:
            dbm.migrate(conn)
            conn.execute(
                """
                INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled)
                VALUES(?, ?, 'LONG', 1.0, 'long_1080p24', 0)
                """,
                ("darkwood-reverie", "Darkwood Reverie"),
            )
            conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
            conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
        finally:
            conn.close()

    def _seed_track_analysis_rows(self, env) -> None:
        conn = dbm.connect(env)
        try:
            track_pk = int(
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?, ?, ?, 'gdrive', ?, ?, ?, 180.0, 1000.0, 1005.0)
                    """,
                    ("darkwood-reverie", "001", "file-001", "001.wav", "Title 001", "Artist X"),
                ).lastrowid
            )
            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (track_pk, '{"analysis_status":"ok","voice_flag":false}', 1010.0),
            )
            conn.execute(
                "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (track_pk, '{"yamnet_tags":["rain","wind"]}', 1020.0),
            )
            conn.execute(
                "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (track_pk, '{"dsp_score":0.93}', 1030.0),
            )
        finally:
            conn.close()

    def test_channels_requires_auth(self) -> None:
        with temp_env() as (_, env):
            self._seed_channel(env)
            _, client = self._new_client()
            resp = client.get("/v1/track-catalog/analysis-report/channels")
            self.assertEqual(resp.status_code, 401)

    def test_channels_returns_expected_schema(self) -> None:
        with temp_env() as (_, env):
            self._seed_channel(env)
            _, client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/track-catalog/analysis-report/channels", headers=auth)

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertIn("channels", body)
            self.assertEqual(len(body["channels"]), 1)
            self.assertEqual(body["channels"][0]["channel_slug"], "darkwood-reverie")
            self.assertEqual(body["channels"][0]["display_name"], "Darkwood Reverie")

    def test_analysis_report_valid_channel_returns_payload_shape(self) -> None:
        with temp_env() as (_, env):
            self._seed_channel(env)
            self._seed_track_analysis_rows(env)
            _, client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/track-catalog/analysis-report",
                params={"channel_slug": "darkwood-reverie"},
                headers=auth,
            )

            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["channel_slug"], "darkwood-reverie")
            self.assertIn("column_groups", body)
            self.assertIn("columns", body)
            self.assertIn("rows", body)
            self.assertIn("summary", body)
            self.assertEqual(body["summary"]["tracks_count"], 1)
            self.assertEqual(len(body["columns"]), len(COLUMN_REGISTRY))
            self.assertEqual(len(body["rows"]), 1)
            self.assertTrue(all("source_path" in col for col in body["columns"]))

    def test_analysis_report_missing_channel_slug_returns_tar_invalid_channel(self) -> None:
        with temp_env() as (_, env):
            self._seed_channel(env)
            _, client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/track-catalog/analysis-report", headers=auth)

            self.assertEqual(resp.status_code, 400)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "TAR_INVALID_CHANNEL")

    def test_analysis_report_unknown_channel_returns_tar_channel_not_found(self) -> None:
        with temp_env() as (_, env):
            self._seed_channel(env)
            _, client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/track-catalog/analysis-report",
                params={"channel_slug": "unknown-channel"},
                headers=auth,
            )

            self.assertEqual(resp.status_code, 404)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "TAR_CHANNEL_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
