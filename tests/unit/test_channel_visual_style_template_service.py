from __future__ import annotations

import unittest

from services.common import db as dbm
from services.metadata import channel_visual_style_template_service as svc
from tests._helpers import seed_minimal_db, temp_env


class TestChannelVisualStyleTemplateService(unittest.TestCase):
    def test_set_default_unsets_previous_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                first = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="first",
                    template_payload=_payload("forest"),
                    make_default=True,
                )
                second = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="second",
                    template_payload=_payload("mist"),
                    make_default=False,
                )
                out = svc.set_default_channel_visual_style_template(conn, template_id=int(second["id"]))
                conn.commit()
                self.assertTrue(out["is_default"])

                row = conn.execute(
                    "SELECT COUNT(*) AS c FROM channel_visual_style_templates WHERE channel_slug=? AND status='ACTIVE' AND is_default=1",
                    ("darkwood-reverie",),
                ).fetchone()
                self.assertEqual(int(row["c"]), 1)

                first_row = dbm.get_channel_visual_style_template_by_id(conn, int(first["id"]))
                assert first_row is not None
                self.assertEqual(int(first_row["is_default"]), 0)
            finally:
                conn.close()

    def test_archived_cannot_be_default_and_activate_does_not_restore_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                created = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="archivable",
                    template_payload=_payload("forest"),
                    make_default=True,
                )
                template_id = int(created["id"])
                svc.archive_channel_visual_style_template(conn, template_id=template_id)
                conn.commit()

                with self.assertRaises(svc.ChannelVisualStyleTemplateError) as ctx:
                    svc.set_default_channel_visual_style_template(conn, template_id=template_id)
                self.assertEqual(ctx.exception.code, "CVST_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_DEFAULT")

                activated = svc.activate_channel_visual_style_template(conn, template_id=template_id)
                conn.commit()
                self.assertEqual(activated["status"], "ACTIVE")
                self.assertFalse(activated["is_default"])
            finally:
                conn.close()

    def test_invalid_payload_cannot_be_set_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                created = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="invalid-later",
                    template_payload=_payload("forest"),
                    make_default=False,
                )
                template_id = int(created["id"])
                bad_payload = _payload("forest")
                bad_payload.pop("branding_rules")
                conn.execute(
                    "UPDATE channel_visual_style_templates SET template_payload_json = ? WHERE id = ?",
                    (dbm.json_dumps(bad_payload), template_id),
                )

                with self.assertRaises(svc.ChannelVisualStyleTemplateError) as ctx:
                    svc.set_default_channel_visual_style_template(conn, template_id=template_id)
                self.assertEqual(ctx.exception.code, "CVST_PAYLOAD_REQUIRED_KEY")

                row = dbm.get_channel_visual_style_template_by_id(conn, template_id)
                assert row is not None
                self.assertEqual(str(row["validation_status"]), "INVALID")
            finally:
                conn.close()

    def test_resolution_release_override_wins_over_channel_default(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                default_item = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="default",
                    template_payload=_payload("forest"),
                    make_default=True,
                )
                override_item = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="override",
                    template_payload=_payload("mist"),
                    make_default=False,
                )
                release_id = _insert_release(conn, channel_slug="darkwood-reverie", suffix="override-wins")
                svc.set_release_visual_style_template_override(conn, release_id=release_id, template_id=int(override_item["id"]))
                out = svc.resolve_effective_channel_visual_style_template_for_release(conn, release_id=release_id)
                self.assertEqual(out["source"], "release_override")
                self.assertTrue(out["is_override"])
                self.assertTrue(out["has_override"])
                self.assertEqual(out["override_template_id"], int(override_item["id"]))
                self.assertEqual(out["default_template_id"], int(default_item["id"]))
                self.assertEqual((out["effective_template"] or {})["id"], int(override_item["id"]))
            finally:
                conn.close()

    def test_resolution_uses_channel_default_when_no_override(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                default_item = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="default",
                    template_payload=_payload("forest"),
                    make_default=True,
                )
                release_id = _insert_release(conn, channel_slug="darkwood-reverie", suffix="default-only")
                out = svc.resolve_effective_channel_visual_style_template_for_release(conn, release_id=release_id)
                self.assertEqual(out["source"], "channel_default")
                self.assertFalse(out["is_override"])
                self.assertFalse(out["has_override"])
                self.assertIsNone(out["override_template_id"])
                self.assertEqual(out["default_template_id"], int(default_item["id"]))
                self.assertEqual((out["effective_template"] or {})["id"], int(default_item["id"]))
            finally:
                conn.close()

    def test_resolution_returns_none_when_no_default_and_no_override(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = _insert_release(conn, channel_slug="darkwood-reverie", suffix="none")
                out = svc.resolve_effective_channel_visual_style_template_for_release(conn, release_id=release_id)
                self.assertEqual(out["source"], "none")
                self.assertFalse(out["is_override"])
                self.assertFalse(out["has_override"])
                self.assertIsNone(out["effective_template"])
                self.assertIsNone(out["override_template_id"])
                self.assertIsNone(out["default_template_id"])
            finally:
                conn.close()

    def test_archived_template_cannot_be_used_as_release_override(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                created = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="archived",
                    template_payload=_payload("forest"),
                    make_default=False,
                )
                template_id = int(created["id"])
                svc.archive_channel_visual_style_template(conn, template_id=template_id)
                release_id = _insert_release(conn, channel_slug="darkwood-reverie", suffix="archived")
                with self.assertRaises(svc.ChannelVisualStyleTemplateError) as ctx:
                    svc.set_release_visual_style_template_override(conn, release_id=release_id, template_id=template_id)
                self.assertEqual(ctx.exception.code, "CVST_TEMPLATE_ARCHIVED_NOT_ALLOWED_AS_OVERRIDE")
            finally:
                conn.close()

    def test_cross_channel_release_override_rejected(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                other = svc.create_channel_visual_style_template(
                    conn,
                    channel_slug="channel-b",
                    template_name="other",
                    template_payload=_payload("night"),
                    make_default=False,
                )
                release_id = _insert_release(conn, channel_slug="darkwood-reverie", suffix="cross")
                with self.assertRaises(svc.ChannelVisualStyleTemplateError) as ctx:
                    svc.set_release_visual_style_template_override(conn, release_id=release_id, template_id=int(other["id"]))
                self.assertEqual(ctx.exception.code, "CVST_TEMPLATE_CHANNEL_MISMATCH")
            finally:
                conn.close()


def _payload(motif: str) -> dict[str, object]:
    return {
        "palette_guidance": "Muted earth tones",
        "typography_rules": "Use clean sans serif titles",
        "text_layout_rules": "Center align title block",
        "composition_framing_rules": "Subject centered with margin",
        "allowed_motifs": [motif],
        "banned_motifs": ["neon"],
        "branding_rules": "Keep logo in lower right",
        "output_profile_guidance": "16:9 high contrast",
        "background_compatibility_guidance": "Works on dark backgrounds",
        "cover_composition_guidance": "Leave top third for text",
    }


def _insert_release(conn, *, channel_slug: str, suffix: str) -> int:
    channel = dbm.get_channel_by_slug(conn, channel_slug)
    assert channel is not None
    cur = conn.execute(
        """
        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
        VALUES(?, 'r', 'd', '[]', NULL, NULL, ?, 1.0)
        """,
        (int(channel["id"]), f"meta-{suffix}"),
    )
    return int(cur.lastrowid)


if __name__ == "__main__":
    unittest.main()
