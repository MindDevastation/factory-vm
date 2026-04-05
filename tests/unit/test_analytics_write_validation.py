from __future__ import annotations

import unittest

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_INVALID_BASELINE_REFERENCE,
    E5A_INVALID_ENTITY_TYPE,
    E5A_INVALID_FRESHNESS_STATUS,
    E5A_INVALID_PAYLOAD_JSON,
    E5A_INVALID_SCOPE_LINK,
    E5A_INVALID_SNAPSHOT_STATUS,
    E5A_INVALID_SOURCE_FAMILY,
    E5A_INVALID_WINDOW_TYPE,
)
from services.analytics_center.write_service import SnapshotWriteInput, write_snapshot
from services.common import db as dbm
from tests._helpers import seed_minimal_db, temp_env


class TestAnalyticsWriteValidation(unittest.TestCase):
    def _valid_snapshot(self) -> SnapshotWriteInput:
        return SnapshotWriteInput(
            entity_type="CHANNEL",
            entity_ref="1",
            source_family="INTERNAL_OPERATIONAL",
            window_type="LAST_KNOWN_CURRENT",
            snapshot_status="CURRENT",
            freshness_status="FRESH",
            payload_json={"views": 10},
            explainability_json={"primary_reason": "ok"},
            lineage_json={"trace": ["t"]},
            anomaly_markers_json=["a"],
            captured_at=1.0,
            is_current=True,
        )

    def test_entity_type_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                payload = self._valid_snapshot()
                payload = SnapshotWriteInput(**{**payload.__dict__, "entity_type": "BAD"})
                with self.assertRaises(AnalyticsDomainError) as ctx:
                    write_snapshot(conn, payload)
                self.assertEqual(ctx.exception.code, E5A_INVALID_ENTITY_TYPE)
            finally:
                conn.close()

    def test_source_family_window_status_freshness_validation(self) -> None:
        cases = [
            ("source_family", "BAD", E5A_INVALID_SOURCE_FAMILY),
            ("window_type", "BAD", E5A_INVALID_WINDOW_TYPE),
            ("snapshot_status", "BAD", E5A_INVALID_SNAPSHOT_STATUS),
            ("freshness_status", "BAD", E5A_INVALID_FRESHNESS_STATUS),
        ]
        for field, value, code in cases:
            with self.subTest(field=field):
                with temp_env() as (_td, env):
                    seed_minimal_db(env)
                    conn = dbm.connect(env)
                    try:
                        payload = self._valid_snapshot()
                        payload = SnapshotWriteInput(**{**payload.__dict__, field: value})
                        with self.assertRaises(AnalyticsDomainError) as ctx:
                            write_snapshot(conn, payload)
                        self.assertEqual(ctx.exception.code, code)
                    finally:
                        conn.close()

    def test_payload_json_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                payload = self._valid_snapshot()
                payload = SnapshotWriteInput(**{**payload.__dict__, "payload_json": "{bad"})
                with self.assertRaises(AnalyticsDomainError) as ctx:
                    write_snapshot(conn, payload)
                self.assertEqual(ctx.exception.code, E5A_INVALID_PAYLOAD_JSON)
            finally:
                conn.close()

    def test_baseline_reference_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                payload = self._valid_snapshot()
                payload = SnapshotWriteInput(**{**payload.__dict__, "comparison_baseline_snapshot_id": 9999})
                with self.assertRaises(AnalyticsDomainError) as ctx:
                    write_snapshot(conn, payload)
                self.assertEqual(ctx.exception.code, E5A_INVALID_BASELINE_REFERENCE)
            finally:
                conn.close()

    def test_linkage_anchor_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                payload = self._valid_snapshot()
                payload = SnapshotWriteInput(**{**payload.__dict__, "entity_ref": "9999"})
                with self.assertRaises(AnalyticsDomainError) as ctx:
                    write_snapshot(conn, payload)
                self.assertEqual(ctx.exception.code, E5A_INVALID_SCOPE_LINK)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
