from __future__ import annotations

import unittest
from unittest import mock

from services.analytics_center.analyzer_service_foundation import (
    AnalyzerSnapshotReadRequest,
    AnalyzerSnapshotWriteRequest,
    read_analyzer_snapshots,
    write_analyzer_snapshot,
)
from services.common import db as dbm
from tests._helpers import temp_env


class TestAnalyzerServiceFoundation(unittest.TestCase):
    def test_write_and_read_enforce_foundation_lineage(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                row = conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("analyzer-channel", "Analyzer Channel", "music", 1.0, "default", 0),
                )
                channel_id = int(row.lastrowid)

                snapshot_id = write_analyzer_snapshot(
                    conn,
                    AnalyzerSnapshotWriteRequest(
                        entity_type="CHANNEL",
                        entity_ref=str(channel_id),
                        source_family="INTERNAL_OPERATIONAL",
                        window_type="LAST_KNOWN_CURRENT",
                        snapshot_status="CURRENT",
                        freshness_status="FRESH",
                        payload_json={"k": "v"},
                        captured_at=123.0,
                        channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                        format_profile="LONG_FORM",
                        sync_state="SUCCEEDED",
                        is_current=True,
                    ),
                )
                self.assertGreater(snapshot_id, 0)

                rows = read_analyzer_snapshots(
                    conn,
                    AnalyzerSnapshotReadRequest(entity_type="CHANNEL", entity_ref=str(channel_id), current_only=True),
                )
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["coverage_state"], "REFRESHED")
                self.assertIn("analyzer_foundation", str(rows[0]["lineage_json"]))
            finally:
                conn.close()

    def test_invalid_sync_state_rejected(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                row = conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("analyzer-channel", "Analyzer Channel", "music", 1.0, "default", 0),
                )
                channel_id = int(row.lastrowid)
                with self.assertRaises(ValueError):
                    write_analyzer_snapshot(
                        conn,
                        AnalyzerSnapshotWriteRequest(
                            entity_type="CHANNEL",
                            entity_ref=str(channel_id),
                            source_family="INTERNAL_OPERATIONAL",
                            window_type="LAST_KNOWN_CURRENT",
                            snapshot_status="CURRENT",
                            freshness_status="FRESH",
                            payload_json={"k": "v"},
                            captured_at=123.0,
                            channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
                            format_profile="LONG_FORM",
                            sync_state="BOGUS",
                            is_current=True,
                        ),
                    )
            finally:
                conn.close()

    def test_invariant_guard_for_core_mode(self) -> None:
        with mock.patch("services.analytics_center.analyzer_service_foundation.CORE_ANALYZER_MODE", "BROKEN"):
            with self.assertRaises(ValueError):
                from services.analytics_center.analyzer_service_foundation import _assert_foundation_invariants

                _assert_foundation_invariants()
