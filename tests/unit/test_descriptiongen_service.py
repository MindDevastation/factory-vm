from __future__ import annotations

import unittest

from services.common import db as dbm
from services.metadata import description_template_service, descriptiongen_service
from tests._helpers import seed_minimal_db, temp_env


class TestDescriptionGenService(unittest.TestCase):
    def _seed_release(
        self,
        *,
        planned_at: str | None = "2026-04-09T18:30:00Z",
        title: str = " Existing title ",
        description: str = " Existing description ",
    ) -> tuple:
        td_cm = temp_env()
        td, env = td_cm.__enter__()
        self.addCleanup(lambda: td_cm.__exit__(None, None, None))
        seed_minimal_db(env)
        conn = dbm.connect(env)
        self.addCleanup(conn.close)
        ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, description, "[]", planned_at, "f", "meta-dg-1", dbm.now_ts()),
        )
        release_id = int(cur.lastrowid)
        return conn, release_id

    def _insert_template(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        body: str = "{{channel_display_name}}\n\n{{release_title}}",
        status: str = "ACTIVE",
        validation_status: str = "VALID",
        is_default: bool = True,
        name: str = "main",
        updated_at: str = "2026-01-01T00:00:00+00:00",
    ) -> int:
        return dbm.create_description_template(
            conn,
            channel_slug=channel_slug,
            template_name=name,
            template_body=body,
            status=status,
            is_default=is_default,
            validation_status=validation_status,
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at=updated_at,
            archived_at=None,
        )

    def test_default_template_resolution(self) -> None:
        conn, release_id = self._seed_release()
        self._insert_template(conn, is_default=True, name="default")
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        self.assertTrue(result.used_template["is_default_channel_template"])

    def test_explicit_template_override_validation(self) -> None:
        conn, release_id = self._seed_release()
        tid = self._insert_template(conn, is_default=False, body="{{channel_slug}}")
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=tid)
        self.assertEqual(result.used_template["id"], tid)

    def test_channel_mismatch_rejection(self) -> None:
        conn, release_id = self._seed_release()
        tid = self._insert_template(conn, channel_slug="channel-b", is_default=False)
        with self.assertRaises(descriptiongen_service.DescriptionGenError) as ctx:
            descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=tid)
        self.assertEqual(ctx.exception.code, "MTD_TEMPLATE_CHANNEL_MISMATCH")

    def test_archived_and_invalid_template_rejection(self) -> None:
        conn, release_id = self._seed_release()
        archived = self._insert_template(conn, status="ARCHIVED", is_default=False, name="arch")
        invalid = self._insert_template(conn, validation_status="INVALID", is_default=False, name="inv")

        with self.assertRaises(descriptiongen_service.DescriptionGenError) as arch_ctx:
            descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=archived)
        self.assertEqual(arch_ctx.exception.code, "MTD_TEMPLATE_NOT_ACTIVE")

        with self.assertRaises(descriptiongen_service.DescriptionGenError) as inv_ctx:
            descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=invalid)
        self.assertEqual(inv_ctx.exception.code, "MTD_TEMPLATE_INVALID")

    def test_release_title_usable_unusable_behavior(self) -> None:
        conn, release_id = self._seed_release(title="  Good  ")
        self._insert_template(conn, body="{{release_title}}")
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        self.assertEqual(result.proposed_description, "Good")

        conn2, release_id2 = self._seed_release(title="   ")
        self._insert_template(conn2, body="{{release_title}}")
        with self.assertRaises(descriptiongen_service.DescriptionGenError) as ctx:
            descriptiongen_service.generate_description_preview(conn2, release_id=release_id2, template_id=None)
        self.assertEqual(ctx.exception.code, "MTD_RELEASE_TITLE_NOT_USABLE")

    def test_date_context_resolution_from_canonical_release_planned_at(self) -> None:
        conn, release_id = self._seed_release(planned_at="2026-12-31T23:59:59-05:00", title="x")
        self._insert_template(conn, body="{{release_year}}-{{release_month_number}}-{{release_day_number}}")
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        self.assertEqual(result.proposed_description, "2026-12-31")

    def test_missing_release_date_context_explicit_error(self) -> None:
        conn, release_id = self._seed_release(planned_at=None, title="x")
        self._insert_template(conn, body="{{release_year}}")
        with self.assertRaises(descriptiongen_service.DescriptionGenError) as ctx:
            descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        self.assertEqual(ctx.exception.code, "MTD_RELEASE_DATE_CONTEXT_MISSING")

    def test_overwrite_detection_from_trimmed_current_description(self) -> None:
        conn, release_id = self._seed_release(description="   ", title="x")
        self._insert_template(conn, body="{{channel_display_name}}")
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        self.assertFalse(result.overwrite_required)

    def test_fingerprint_creation(self) -> None:
        conn, release_id = self._seed_release(title="x", description="")
        t1 = self._insert_template(conn, is_default=False, updated_at="2026-01-01T00:00:00+00:00", body="{{channel_display_name}}")
        t2 = self._insert_template(conn, is_default=False, updated_at="2026-01-02T00:00:00+00:00", body="{{channel_display_name}}")
        r1 = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=t1)
        r2 = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=t2)
        self.assertNotEqual(r1.generation_fingerprint, r2.generation_fingerprint)

    def test_fingerprint_changes_when_release_context_changes(self) -> None:
        conn, release_id = self._seed_release(title="title-a", description="")
        tid = self._insert_template(conn, is_default=False, body="{{channel_display_name}}")
        before = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=tid)

        conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("title-b", release_id))
        conn.commit()

        after = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=tid)
        self.assertEqual(before.proposed_description, after.proposed_description)
        self.assertNotEqual(before.generation_fingerprint, after.generation_fingerprint)

    def test_multiline_normalization_parity_with_builder(self) -> None:
        conn, release_id = self._seed_release(title="Night Ritual", description="")
        template_body = " \r\n{{channel_display_name}}   \r\n\r\n{{release_title}}  \r\n"
        self._insert_template(conn, body=template_body)
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        preview = description_template_service.preview_description_template(
            channel={"slug": "darkwood-reverie", "display_name": "Darkwood Reverie", "kind": "music"},
            template_body=template_body,
            release_row={"title": "Night Ritual", "planned_at": "2026-04-09T18:30:00Z"},
        )
        self.assertEqual(result.proposed_description, preview.rendered_description_preview)

    def test_generate_overwrite_false_when_normalized_description_is_same(self) -> None:
        conn, release_id = self._seed_release(
            title="Night Ritual",
            description=" Darkwood Reverie  \r\n\r\nNight Ritual \r\n",
        )
        self._insert_template(conn, body="{{channel_display_name}}\n\n{{release_title}}")
        result = descriptiongen_service.generate_description_preview(conn, release_id=release_id, template_id=None)
        self.assertFalse(result.overwrite_required)
        self.assertEqual(result.warnings, [])


if __name__ == "__main__":
    unittest.main()
