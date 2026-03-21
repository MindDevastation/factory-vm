from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.metadata import channel_defaults_service


class TestMetadataChannelDefaultsService(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:", isolation_level=None)
        self.conn.row_factory = dbm._dict_factory  # type: ignore[attr-defined]
        self.conn.execute("PRAGMA foreign_keys=ON;")
        dbm.migrate(self.conn)
        for slug, name in [("darkwood-reverie", "Darkwood Reverie"), ("channel-b", "Channel B")]:
            self.conn.execute(
                "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                (slug, name, "LONG", 1.0, "long_1080p24", 0),
            )

    def tearDown(self) -> None:
        self.conn.close()

    def _insert_source(self, table: str, *, channel_slug: str = "darkwood-reverie", status: str = "ACTIVE", validation_status: str = "VALID") -> int:
        common = (channel_slug, status, validation_status)
        if table == "title_templates":
            row = self.conn.execute(
                "INSERT INTO title_templates(channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, last_validated_at, created_at, updated_at, archived_at) VALUES (?, 'Title', '{{channel_slug}}', ?, 0, ?, NULL, 't', 't', 't', NULL)",
                common,
            )
        elif table == "description_templates":
            row = self.conn.execute(
                "INSERT INTO description_templates(channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, last_validated_at, created_at, updated_at, archived_at) VALUES (?, 'Desc', '{{channel_slug}}', ?, 0, ?, NULL, 't', 't', 't', NULL)",
                common,
            )
        else:
            row = self.conn.execute(
                "INSERT INTO video_tag_presets(channel_slug, preset_name, preset_body_json, status, is_default, validation_status, validation_errors_json, last_validated_at, created_at, updated_at, archived_at) VALUES (?, 'Preset', '[\"tag\"]', ?, 0, ?, NULL, 't', 't', 't', NULL)",
                common,
            )
        return int(row.lastrowid)

    def test_update_success_idempotent_and_atomic_failure(self) -> None:
        title_id = self._insert_source("title_templates")
        desc_id = self._insert_source("description_templates")
        preset_id = self._insert_source("video_tag_presets")

        first = channel_defaults_service.update_channel_defaults(
            self.conn,
            channel_slug="darkwood-reverie",
            default_title_template_id=title_id,
            default_description_template_id=desc_id,
            default_video_tag_preset_id=preset_id,
        )
        self.assertTrue(first["defaults_updated"])
        second = channel_defaults_service.update_channel_defaults(
            self.conn,
            channel_slug="darkwood-reverie",
            default_title_template_id=title_id,
            default_description_template_id=desc_id,
            default_video_tag_preset_id=preset_id,
        )
        self.assertFalse(second["defaults_updated"])

        foreign_title_id = self._insert_source("title_templates", channel_slug="channel-b")
        with self.assertRaises(channel_defaults_service.MetadataDefaultsError):
            channel_defaults_service.update_channel_defaults(
                self.conn,
                channel_slug="darkwood-reverie",
                default_title_template_id=foreign_title_id,
                default_description_template_id=desc_id,
                default_video_tag_preset_id=preset_id,
            )
        current = channel_defaults_service.read_channel_defaults(self.conn, channel_slug="darkwood-reverie")
        self.assertEqual(current["defaults"]["title_template"]["id"], title_id)

    def test_validation_codes(self) -> None:
        archived_id = self._insert_source("title_templates", status="ARCHIVED")
        invalid_id = self._insert_source("title_templates", validation_status="INVALID")
        foreign_id = self._insert_source("title_templates", channel_slug="channel-b")

        cases = [
            (archived_id, "MDO_DEFAULT_SOURCE_NOT_ACTIVE"),
            (invalid_id, "MDO_DEFAULT_SOURCE_INVALID"),
            (foreign_id, "MDO_DEFAULT_SOURCE_CHANNEL_MISMATCH"),
        ]
        for source_id, code in cases:
            with self.assertRaises(channel_defaults_service.MetadataDefaultsError) as ctx:
                channel_defaults_service.update_channel_defaults(
                    self.conn,
                    channel_slug="darkwood-reverie",
                    default_title_template_id=source_id,
                    default_description_template_id=None,
                    default_video_tag_preset_id=None,
                )
            self.assertEqual(ctx.exception.code, code)

    def test_wrong_field_type_code(self) -> None:
        desc_id = self._insert_source("description_templates")
        with self.assertRaises(channel_defaults_service.MetadataDefaultsError) as ctx:
            channel_defaults_service.update_channel_defaults(
                self.conn,
                channel_slug="darkwood-reverie",
                default_title_template_id=desc_id,
                default_description_template_id=None,
                default_video_tag_preset_id=None,
            )
        self.assertEqual(ctx.exception.code, "MDO_DEFAULT_FIELD_TYPE_MISMATCH")


if __name__ == "__main__":
    unittest.main()
