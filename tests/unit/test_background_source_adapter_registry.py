from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner.background_source_adapter_registry import (
    ALLOWED_BACKGROUND_SOURCE_FAMILIES,
    BackgroundSourceAdapterError,
    build_default_background_source_adapter_registry,
)
from tests._helpers import seed_minimal_db, temp_env


class TestBackgroundSourceAdapterRegistry(unittest.TestCase):
    def test_default_registry_registers_all_allowed_families(self) -> None:
        registry = build_default_background_source_adapter_registry()
        self.assertEqual(registry.list_families(), list(ALLOWED_BACKGROUND_SOURCE_FAMILIES))

    def test_registry_rejects_unsupported_family(self) -> None:
        registry = build_default_background_source_adapter_registry()
        with self.assertRaises(BackgroundSourceAdapterError) as ctx:
            registry.get("generation")
        self.assertEqual(ctx.exception.code, "VBG_UNSUPPORTED_SOURCE_FAMILY")

    def test_registry_adapter_returns_normalized_candidate_shape(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                        VALUES(?, 'r', 'd', '[]', NULL, NULL, 'origin-registry', NULL, 1.0)
                        """,
                        (int(channel["id"]),),
                    ).lastrowid
                )
                dbm.create_asset(
                    conn,
                    channel_id=int(channel["id"]),
                    kind="IMAGE",
                    origin="MANAGED",
                    origin_id="managed://bg-1",
                    name="bg-managed.png",
                    path="/tmp/bg-managed.png",
                )

                registry = build_default_background_source_adapter_registry()
                rows = registry.get("managed_library")(conn, release_id, int(channel["id"]))
                self.assertEqual(len(rows), 1)
                item = rows[0]
                self.assertEqual(item.source_family, "managed_library")
                self.assertEqual(item.source_reference, "managed://bg-1")
                self.assertEqual(item.selection_mode_prefill, "manual")
                self.assertFalse(item.template_assisted)
                self.assertEqual(item.warnings, [])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
