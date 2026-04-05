from __future__ import annotations

import unittest

from services.common import db as dbm
from services.factory_api.publish_policy import _resolve_effective_policy
from tests._helpers import seed_minimal_db, temp_env


class TestPublishPolicyResolution(unittest.TestCase):
    def test_precedence_item_over_channel_over_project(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO publish_policy_project_defaults(
                        singleton_key, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(1, 'manual_only', 'public', 'policy_requires_manual', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-1')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO publish_policy_channel_overrides(
                        channel_slug, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES('darkwood-reverie', 'hold', 'unlisted', 'channel_policy_block', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-2')
                    """
                )
                rel = conn.execute("SELECT id FROM releases ORDER BY id ASC LIMIT 1").fetchone()
                if not rel:
                    ch = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
                    self.assertIsNotNone(ch)
                    rel_id = int(
                        conn.execute(
                            """
                            INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                            VALUES(?, 'r', 'd', '[]', NULL, NULL, 'meta-resolve-1', 1.0)
                            """,
                            (int(ch["id"]),),
                        ).lastrowid
                    )
                else:
                    rel_id = int(rel["id"])
                conn.execute(
                    """
                    INSERT INTO publish_policy_item_overrides(
                        release_id, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(?, 'auto', 'public', 'item_override_block', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-3')
                    """,
                    (rel_id,),
                )

                out = _resolve_effective_policy(conn, release_id=rel_id, channel_slug="darkwood-reverie")
                self.assertEqual(out["effective_publish_mode"], "auto")
                self.assertEqual(out["effective_target_visibility"], "public")
                self.assertEqual(out["effective_reason_code"], "item_override_block")
                self.assertEqual(out["resolved_scope"], "item")
            finally:
                conn.close()

    def test_null_unset_falls_back_from_item_and_channel(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                rel = conn.execute("SELECT id FROM releases ORDER BY id ASC LIMIT 1").fetchone()
                if not rel:
                    ch = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
                    self.assertIsNotNone(ch)
                    rel_id = int(
                        conn.execute(
                            """
                            INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                            VALUES(?, 'r', 'd', '[]', NULL, NULL, 'meta-resolve-2', 1.0)
                            """,
                            (int(ch["id"]),),
                        ).lastrowid
                    )
                else:
                    rel_id = int(rel["id"])

                conn.execute(
                    """
                    INSERT INTO publish_policy_project_defaults(
                        singleton_key, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(1, 'manual_only', 'unlisted', 'policy_requires_manual', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-10')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO publish_policy_channel_overrides(
                        channel_slug, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES('darkwood-reverie', NULL, NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-11')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO publish_policy_item_overrides(
                        release_id, publish_mode, target_visibility, reason_code, created_at, updated_at, updated_by, last_reason, last_request_id
                    ) VALUES(?, NULL, NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'admin', 'seed', 'req-12')
                    """,
                    (rel_id,),
                )

                out = _resolve_effective_policy(conn, release_id=rel_id, channel_slug="darkwood-reverie")
                self.assertEqual(out["effective_publish_mode"], "manual_only")
                self.assertEqual(out["effective_target_visibility"], "unlisted")
                self.assertEqual(out["effective_reason_code"], "policy_requires_manual")
                self.assertEqual(out["resolved_scope"], "project")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
