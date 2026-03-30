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


if __name__ == "__main__":
    unittest.main()
