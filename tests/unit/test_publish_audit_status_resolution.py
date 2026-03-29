from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api.publish_audit_status import resolve_effective_audit_status
from tests._helpers import seed_minimal_db, temp_env


class TestPublishAuditStatusResolution(unittest.TestCase):
    def test_effective_resolution_project_default_only(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO publish_audit_status_project_defaults(
                        singleton_key, status, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(1, 'approved', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-1')
                    """
                )
                out = resolve_effective_audit_status(conn, channel_slug="darkwood-reverie")
                self.assertEqual(out["project_default_status"], "approved")
                self.assertIsNone(out["channel_override_status"])
                self.assertEqual(out["effective_status"], "approved")
            finally:
                conn.close()

    def test_effective_resolution_channel_override_wins(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO publish_audit_status_project_defaults(
                        singleton_key, status, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(1, 'pending', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-1')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO publish_audit_status_channel_overrides(
                        channel_slug, status, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES('darkwood-reverie', 'manual-only', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-2')
                    """
                )
                out = resolve_effective_audit_status(conn, channel_slug="darkwood-reverie")
                self.assertEqual(out["project_default_status"], "pending")
                self.assertEqual(out["channel_override_status"], "manual-only")
                self.assertEqual(out["effective_status"], "manual-only")
            finally:
                conn.close()

    def test_effective_resolution_falls_back_to_unknown_without_default(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out = resolve_effective_audit_status(conn, channel_slug="darkwood-reverie")
                self.assertEqual(out["project_default_status"], "unknown")
                self.assertIsNone(out["channel_override_status"])
                self.assertEqual(out["effective_status"], "unknown")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
