from __future__ import annotations

import unittest
from unittest import mock

from services.common import db as dbm
from services.metadata import video_tag_preset_service, video_tagsgen_service
from tests._helpers import seed_minimal_db, temp_env


class TestVideoTagsGenService(unittest.TestCase):
    def _seed_release(
        self,
        *,
        channel_slug: str = "darkwood-reverie",
        planned_at: str | None = "2026-04-09T18:30:00Z",
        title: str = " Night Ritual ",
        tags_json: str = '["ambient", " ambient ", "night", "ambient"]',
    ):
        td_cm = temp_env()
        td, env = td_cm.__enter__()
        self.addCleanup(lambda: td_cm.__exit__(None, None, None))
        seed_minimal_db(env)
        conn = dbm.connect(env)
        self.addCleanup(conn.close)
        ch = dbm.get_channel_by_slug(conn, channel_slug)
        assert ch is not None
        cur = conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?,?,?,?,?,?,?,?)",
            (int(ch["id"]), title, "d", tags_json, planned_at, "f", "meta-vtg-1", dbm.now_ts()),
        )
        return conn, int(cur.lastrowid)

    def _insert_preset(
        self,
        conn,
        *,
        channel_slug: str = "darkwood-reverie",
        body: list[str] | None = None,
        status: str = "ACTIVE",
        validation_status: str = "VALID",
        is_default: bool = True,
        name: str = "main",
        updated_at: str = "2026-01-01T00:00:00+00:00",
    ) -> int:
        return dbm.create_video_tag_preset(
            conn,
            channel_slug=channel_slug,
            preset_name=name,
            preset_body_json=dbm.json_dumps(body or ["{{channel_display_name}}", "{{release_title}}", "ambient", "ambient"]),
            status=status,
            is_default=is_default,
            validation_status=validation_status,
            validation_errors_json=None,
            last_validated_at="2026-01-01T00:00:00+00:00",
            created_at="2026-01-01T00:00:00+00:00",
            updated_at=updated_at,
            archived_at=None,
        )

    def test_default_preset_resolution(self) -> None:
        conn, release_id = self._seed_release()
        self._insert_preset(conn, is_default=True)
        result = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        self.assertTrue(result.used_preset["is_default_channel_preset"])

    def test_context_excludes_invalid_default_from_usable_default_path(self) -> None:
        conn, release_id = self._seed_release()
        self._insert_preset(conn, is_default=True, validation_status="INVALID", name="invalid-default")
        self._insert_preset(conn, is_default=False, validation_status="VALID", name="valid-alt")
        context = video_tagsgen_service.load_video_tags_context(conn, release_id=release_id)
        self.assertIsNone(context.default_preset)
        self.assertFalse(context.can_generate_with_default)

    def test_context_active_presets_excludes_unusable_presets(self) -> None:
        conn, release_id = self._seed_release()
        valid_id = self._insert_preset(conn, is_default=False, validation_status="VALID", name="valid")
        self._insert_preset(conn, is_default=False, validation_status="INVALID", name="invalid")
        self._insert_preset(conn, is_default=False, status="ARCHIVED", validation_status="VALID", name="archived")
        context = video_tagsgen_service.load_video_tags_context(conn, release_id=release_id)
        self.assertEqual([item["id"] for item in context.active_presets], [valid_id])

    def test_explicit_override_validation(self) -> None:
        conn, release_id = self._seed_release()
        pid = self._insert_preset(conn, is_default=False, body=["{{channel_slug}}"])
        result = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=pid)
        self.assertEqual(result.used_preset["id"], pid)

    def test_channel_mismatch_rejection(self) -> None:
        conn, release_id = self._seed_release()
        pid = self._insert_preset(conn, channel_slug="channel-b", is_default=False)
        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
            video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=pid)
        self.assertEqual(ctx.exception.code, "MTV_PRESET_CHANNEL_MISMATCH")

    def test_archived_and_invalid_preset_rejection(self) -> None:
        conn, release_id = self._seed_release()
        archived = self._insert_preset(conn, status="ARCHIVED", is_default=False, name="arch")
        invalid = self._insert_preset(conn, validation_status="INVALID", is_default=False, name="inv")

        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as arch_ctx:
            video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=archived)
        self.assertEqual(arch_ctx.exception.code, "MTV_PRESET_NOT_ACTIVE")

        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as inv_ctx:
            video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=invalid)
        self.assertEqual(inv_ctx.exception.code, "MTV_PRESET_INVALID")

    def test_release_title_usable_behavior(self) -> None:
        conn, release_id = self._seed_release(title=" Good ")
        self._insert_preset(conn, body=["{{release_title}}"])
        result = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        self.assertEqual(result.proposed_tags_json, ["Good"])

        conn2, release_id2 = self._seed_release(title="  ")
        self._insert_preset(conn2, body=["{{release_title}}"])
        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
            video_tagsgen_service.generate_video_tags_preview(conn2, release_id=release_id2, preset_id=None)
        self.assertEqual(ctx.exception.code, "MTV_RELEASE_TITLE_NOT_USABLE")

    def test_date_context_from_canonical_release_planned_at(self) -> None:
        conn, release_id = self._seed_release(planned_at="2026-12-31T23:59:59-05:00", title="x")
        self._insert_preset(conn, body=["{{release_year}}", "{{release_month_number}}", "{{release_day_number}}"])
        result = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        self.assertEqual(result.proposed_tags_json, ["2026", "12", "31"])

    def test_missing_date_context_explicit_error(self) -> None:
        conn, release_id = self._seed_release(planned_at=None, title="x")
        self._insert_preset(conn, body=["{{release_year}}"])
        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
            video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        self.assertEqual(ctx.exception.code, "MTV_RELEASE_DATE_CONTEXT_MISSING")

    def test_overwrite_detection_and_duplicate_removal(self) -> None:
        conn, release_id = self._seed_release(tags_json='["ambient", "ambient", "  "]')
        self._insert_preset(conn, body=["ambient", "ambient", "dark"]) 
        result = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        self.assertTrue(result.overwrite_required)
        self.assertEqual(result.removed_duplicates, ["ambient"])
        self.assertEqual(result.proposed_tags_json, ["ambient", "dark"])

    def test_fingerprint_creation(self) -> None:
        conn, release_id = self._seed_release(title="x")
        p1 = self._insert_preset(conn, is_default=False, updated_at="2026-01-01T00:00:00+00:00", body=["{{channel_display_name}}"])
        p2 = self._insert_preset(conn, is_default=False, updated_at="2026-01-02T00:00:00+00:00", body=["{{channel_display_name}}"])
        r1 = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=p1)
        r2 = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=p2)
        self.assertNotEqual(r1.generation_fingerprint, r2.generation_fingerprint)

    def test_generator_normalization_parity_with_preview(self) -> None:
        conn, release_id = self._seed_release(title="Night Ritual", tags_json="[]")
        body = ["  {{channel_display_name}}  ", "{{release_title}}", "", "{{release_title}}"]
        self._insert_preset(conn, body=body)
        generated = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        preview = video_tag_preset_service.preview_video_tag_preset(
            channel={"slug": "darkwood-reverie", "display_name": "Darkwood Reverie", "kind": "music"},
            preset_body=body,
            release_row={"title": "Night Ritual", "planned_at": "2026-04-09T18:30:00Z"},
        )
        self.assertEqual(generated.proposed_tags_json, preview.final_normalized_tags)

    def test_render_time_validation_failure_maps_to_preset_invalid(self) -> None:
        conn, release_id = self._seed_release()
        self._insert_preset(conn, is_default=True, body=["ok"])
        preview = video_tag_preset_service.PreviewResult(
            syntax_valid=True,
            structurally_valid=False,
            render_status="FULL",
            used_variables=[],
            resolved_values={},
            missing_variables=[],
            rendered_items_before_normalization=["x" * 501],
            dropped_empty_items=[],
            removed_duplicates=[],
            final_normalized_tags=["x" * 501],
            normalized_count=1,
            validation_errors=[{"code": "MTV_TAG_ITEM_TOO_LONG", "message": "too long"}],
        )
        with mock.patch("services.metadata.video_tagsgen_service.video_tag_preset_service.preview_video_tag_preset", return_value=preview):
            with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
                video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        self.assertEqual(ctx.exception.code, "MTV_PRESET_INVALID")

    def test_apply_updates_only_release_tags_json(self) -> None:
        conn, release_id = self._seed_release(tags_json='["ambient", "night"]')
        self._insert_preset(conn, body=["{{channel_display_name}}", "{{release_title}}", "ambient"])
        generated = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)

        before = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
        assert before is not None
        applied = video_tagsgen_service.apply_generated_video_tags(
            conn,
            release_id=release_id,
            preset_id=None,
            generation_fingerprint=generated.generation_fingerprint,
            overwrite_confirmed=True,
        )
        self.assertTrue(applied.tags_updated)

        after = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
        assert after is not None
        self.assertEqual(before["title"], after["title"])
        self.assertEqual(before["description"], after["description"])
        self.assertEqual(after["tags_json"], dbm.json_dumps(applied.tags_after))

    def test_apply_requires_overwrite_confirmation(self) -> None:
        conn, release_id = self._seed_release(tags_json='["ambient", "night"]')
        self._insert_preset(conn, body=["new-tag"])
        generated = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
            video_tagsgen_service.apply_generated_video_tags(
                conn,
                release_id=release_id,
                preset_id=None,
                generation_fingerprint=generated.generation_fingerprint,
                overwrite_confirmed=False,
            )
        self.assertEqual(ctx.exception.code, "MTV_OVERWRITE_CONFIRMATION_REQUIRED")

    def test_apply_blocks_stale_fingerprint_when_preset_changes(self) -> None:
        conn, release_id = self._seed_release(tags_json='["ambient"]')
        preset_id = self._insert_preset(conn, body=["{{release_title}}"], updated_at="2026-01-01T00:00:00+00:00")
        generated = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=preset_id)
        conn.execute("UPDATE video_tag_presets SET updated_at = ? WHERE id = ?", ("2026-01-02T00:00:00+00:00", preset_id))
        conn.commit()

        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
            video_tagsgen_service.apply_generated_video_tags(
                conn,
                release_id=release_id,
                preset_id=preset_id,
                generation_fingerprint=generated.generation_fingerprint,
                overwrite_confirmed=True,
            )
        self.assertEqual(ctx.exception.code, "MTV_PREVIEW_STALE")

    def test_apply_blocks_stale_fingerprint_when_release_context_changes(self) -> None:
        conn, release_id = self._seed_release(title="Night Ritual", tags_json='["ambient"]')
        self._insert_preset(conn, body=["{{release_title}}"])
        generated = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        conn.execute("UPDATE releases SET title = ? WHERE id = ?", ("Changed title", release_id))
        conn.commit()
        with self.assertRaises(video_tagsgen_service.VideoTagsGenError) as ctx:
            video_tagsgen_service.apply_generated_video_tags(
                conn,
                release_id=release_id,
                preset_id=None,
                generation_fingerprint=generated.generation_fingerprint,
                overwrite_confirmed=True,
            )
        self.assertEqual(ctx.exception.code, "MTV_PREVIEW_STALE")

    def test_apply_same_tags_returns_no_op_without_overwrite_confirmation(self) -> None:
        conn, release_id = self._seed_release(tags_json='["ambient", "night"]')
        self._insert_preset(conn, body=["ambient", "night"])
        generated = video_tagsgen_service.generate_video_tags_preview(conn, release_id=release_id, preset_id=None)
        applied = video_tagsgen_service.apply_generated_video_tags(
            conn,
            release_id=release_id,
            preset_id=None,
            generation_fingerprint=generated.generation_fingerprint,
            overwrite_confirmed=False,
        )
        self.assertFalse(applied.tags_updated)
        self.assertEqual(applied.message, "Release tags already match generated result.")


if __name__ == "__main__":
    unittest.main()
