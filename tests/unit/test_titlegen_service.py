from __future__ import annotations

import unittest

from services.common import db as dbm
from services.metadata import titlegen_service
from tests._helpers import seed_minimal_db, temp_env


class TestTitleGenService(unittest.TestCase):
    def _seed_release(self, *, planned_at: str | None = "2026-04-09T18:30:00Z", title: str = " Existing title ") -> tuple:
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
            (int(ch["id"]), title, "d", "[]", planned_at, "f", "meta-u-1", dbm.now_ts()),
        )
        release_id = int(cur.lastrowid)
        return conn, release_id

    def _insert_template(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        body: str = "{{channel_display_name}} {{release_year}}",
        status: str = "ACTIVE",
        validation_status: str = "VALID",
        is_default: bool = True,
        name: str = "main",
        updated_at: str = "2026-01-01T00:00:00+00:00",
    ) -> int:
        return dbm.create_title_template(
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

    def test_context_contract_shape(self) -> None:
        conn, release_id = self._seed_release(title="  already set  ")
        self._insert_template(conn, is_default=True, name="default")
        self._insert_template(conn, is_default=False, name="alt")
        self._insert_template(conn, status="ARCHIVED", is_default=False, name="old")

        context = titlegen_service.load_titlegen_context(conn, release_id=release_id)
        self.assertEqual(context.release_id, release_id)
        self.assertEqual(context.channel_slug, "darkwood-reverie")
        self.assertEqual(context.current_title, "  already set  ")
        self.assertTrue(context.has_existing_title)
        self.assertTrue(context.can_generate_with_default)
        self.assertIsNotNone(context.default_template)
        self.assertEqual(len(context.active_templates), 2)
        expected_keys = {"id", "template_name", "status", "is_default"}
        for item in context.active_templates:
            self.assertEqual(set(item.keys()), expected_keys)
            self.assertEqual(item["status"], "ACTIVE")

    def test_default_template_resolution_and_fingerprint_and_normalization(self) -> None:
        conn, release_id = self._seed_release()
        self._insert_template(conn, body="  {{channel_display_name}}    {{release_year}}  ")

        result = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=None)
        self.assertEqual(result.used_template["source"], "default")
        self.assertEqual(result.proposed_title, "Darkwood Reverie 2026")
        self.assertEqual(result.normalized_length, len("Darkwood Reverie 2026"))
        self.assertTrue(result.generation_fingerprint)

    def test_explicit_template_override_validation(self) -> None:
        conn, release_id = self._seed_release()
        tid = self._insert_template(conn, is_default=False)

        result = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=tid)
        self.assertEqual(result.used_template["source"], "explicit")

    def test_channel_mismatch_rejected(self) -> None:
        conn, release_id = self._seed_release()
        tid = self._insert_template(conn, channel_slug="channel-b", is_default=False)
        with self.assertRaises(titlegen_service.TitleGenError) as ctx:
            titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=tid)
        self.assertEqual(ctx.exception.code, "MTG_TEMPLATE_CHANNEL_MISMATCH")

    def test_archived_and_invalid_template_rejected(self) -> None:
        conn, release_id = self._seed_release()
        archived = self._insert_template(conn, status="ARCHIVED", is_default=False, name="arch")
        invalid = self._insert_template(conn, validation_status="INVALID", is_default=False, name="inv")

        with self.assertRaises(titlegen_service.TitleGenError) as arch_ctx:
            titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=archived)
        self.assertEqual(arch_ctx.exception.code, "MTG_TEMPLATE_NOT_ACTIVE")

        with self.assertRaises(titlegen_service.TitleGenError) as inv_ctx:
            titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=invalid)
        self.assertEqual(inv_ctx.exception.code, "MTG_TEMPLATE_INVALID")

    def test_missing_or_unparseable_planned_at_only_fails_for_date_variables(self) -> None:
        conn, release_id = self._seed_release(planned_at=None, title="")
        self._insert_template(conn, body="{{channel_display_name}} {{release_year}}")
        with self.assertRaises(titlegen_service.TitleGenError) as missing_ctx:
            titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=None)
        self.assertEqual(missing_ctx.exception.code, "MTG_REQUIRED_CONTEXT_MISSING")

        conn2, release_id2 = self._seed_release(planned_at="not-a-date", title="")
        self._insert_template(conn2, body="{{channel_display_name}}")
        ok = titlegen_service.generate_title_preview(conn2, release_id=release_id2, template_id=None)
        self.assertEqual(ok.proposed_title, "Darkwood Reverie")

    def test_overwrite_detection_trims_current_title(self) -> None:
        conn, release_id = self._seed_release(title="   ")
        self._insert_template(conn, body="{{channel_display_name}}")
        res = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=None)
        self.assertFalse(res.overwrite_required)

    def test_fingerprint_changes_with_template_updated_at_and_effective_context(self) -> None:
        conn, release_id = self._seed_release(planned_at="2026-04-09T18:30:00Z", title="")
        t1 = self._insert_template(conn, is_default=False, updated_at="2026-01-01T00:00:00+00:00")
        t2 = self._insert_template(conn, is_default=False, updated_at="2026-01-02T00:00:00+00:00")

        r1 = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=t1)
        r2 = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=t2)
        self.assertNotEqual(r1.generation_fingerprint, r2.generation_fingerprint)

        conn.execute("UPDATE releases SET planned_at = ? WHERE id = ?", ("2027-04-09T18:30:00Z", release_id))
        conn.commit()
        r3 = titlegen_service.generate_title_preview(conn, release_id=release_id, template_id=t1)
        self.assertNotEqual(r1.generation_fingerprint, r3.generation_fingerprint)


if __name__ == "__main__":
    unittest.main()
