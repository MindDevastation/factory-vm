from __future__ import annotations

import importlib
import io
import unittest

from fastapi.testclient import TestClient
from openpyxl import load_workbook

from services.common import db as dbm
from services.track_analysis_report.xlsx_export import build_group_header_spans, sanitize_sheet_name
from tests._helpers import basic_auth_header, temp_env


class TestTrackAnalysisReportXlsxApi(unittest.TestCase):
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
                ("darkwood-reverie", "Darkwood Reverie / Night"),
            )
            conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
            conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
        finally:
            conn.close()

    def _seed_tracks(self, env) -> None:
        conn = dbm.connect(env)
        try:
            first_pk = int(
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?, ?, ?, 'gdrive', ?, ?, ?, 180.0, 1000.0, 1005.0)
                    """,
                    ("darkwood-reverie", "001", "file-001", "001.wav", "Title 001", "Artist X"),
                ).lastrowid
            )
            second_pk = int(
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?, ?, ?, 'gdrive', ?, ?, ?, 181.0, 2000.0, 2005.0)
                    """,
                    ("darkwood-reverie", "002", "file-002", "002.wav", "Title 002", "Artist Y"),
                ).lastrowid
            )
            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (first_pk, '{"analysis_status":"ok","voice_flag":false,"yamnet_agg":{"voice_labels_used":["speech"],"speech_labels_used":["conversation"]}}', 1010.0),
            )
            conn.execute(
                "INSERT INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (first_pk, '{"yamnet_tags":["rain","wind"]}', 1020.0),
            )
            conn.execute(
                "INSERT INTO track_scores(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (first_pk, '{"dsp_score":0.93}', 1030.0),
            )
            conn.execute(
                "INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
                (second_pk, '{"analysis_status":"ok","voice_flag":true}', 2010.0),
            )
        finally:
            conn.close()

    def test_xlsx_endpoint_matches_api_dataset(self) -> None:
        with temp_env() as (_, env):
            self._seed_channel(env)
            self._seed_tracks(env)
            _, client = self._new_client()
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            api_resp = client.get(
                "/v1/track-catalog/analysis-report",
                params={"channel_slug": "darkwood-reverie"},
                headers=auth,
            )
            self.assertEqual(api_resp.status_code, 200)
            api_payload = api_resp.json()

            xlsx_resp = client.get(
                "/v1/track-catalog/analysis-report.xlsx",
                params={"channel_slug": "darkwood-reverie"},
                headers=auth,
            )

            self.assertEqual(xlsx_resp.status_code, 200)
            self.assertEqual(
                xlsx_resp.headers.get("content-type"),
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            disposition = xlsx_resp.headers.get("content-disposition") or ""
            self.assertRegex(
                disposition,
                r'^attachment; filename="analysis_report__darkwood-reverie__\d{8}_\d{6}\.xlsx"$',
            )

            wb = load_workbook(io.BytesIO(xlsx_resp.content))
            self.assertEqual(len(wb.sheetnames), 1)
            ws = wb.active

            expected_sheet = sanitize_sheet_name("Darkwood Reverie / Night")
            self.assertEqual(ws.title, expected_sheet)
            self.assertEqual(ws.freeze_panes, "A3")

            columns = api_payload["columns"]
            expected_spans = build_group_header_spans(columns)
            actual_merged = sorted(str(item) for item in ws.merged_cells.ranges)
            expected_merged = sorted(
                f"{_col(start)}1:{_col(end)}1" for start, end, _ in expected_spans
            )
            self.assertEqual(actual_merged, expected_merged)

            for start, _end, group in expected_spans:
                self.assertEqual(ws.cell(row=1, column=start).value, group)

            ordered_keys = [col["key"] for col in columns]
            self.assertEqual(
                [ws.cell(row=2, column=idx).value for idx in range(1, len(ordered_keys) + 1)],
                ordered_keys,
            )

            self.assertEqual(len(api_payload["rows"]), ws.max_row - 2)

            for row_offset, api_row in enumerate(api_payload["rows"], start=3):
                for col_idx, key in enumerate(ordered_keys, start=1):
                    self.assertEqual(ws.cell(row=row_offset, column=col_idx).value, api_row.get(key))



def _col(value: int) -> str:
    result = ""
    current = value
    while current > 0:
        current, rem = divmod(current - 1, 26)
        result = chr(65 + rem) + result
    return result


if __name__ == "__main__":
    unittest.main()
