from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner.monthly_planning_template_service import (
    MonthlyPlanningTemplateError,
    MonthlyPlanningTemplateListParams,
    MonthlyPlanningTemplateService,
)
from tests._helpers import seed_minimal_db, temp_env


class TestMonthlyPlanningTemplateService(unittest.TestCase):
    def _base_payload(self) -> dict:
        return {
            "channel_id": 1,
            "template_name": "April core batch",
            "content_type": "LONG",
            "items": [
                {
                    "item_key": "day-01-main",
                    "slot_code": "day_01_main",
                    "position": 1,
                    "title": "Release 01",
                    "day_of_month": 1,
                    "notes": "optional",
                }
            ],
        }

    def _create_template(self, svc: MonthlyPlanningTemplateService) -> dict:
        return svc.create_template(**self._base_payload())

    def test_content_type_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["content_type"] = "   "
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx:
                    svc.create_template(**payload)
                self.assertEqual(ctx.exception.code, "MPT_INVALID_CONTENT_TYPE")
            finally:
                conn.close()

    def test_template_name_and_empty_items_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["template_name"] = ""
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_name:
                    svc.create_template(**payload)
                self.assertEqual(ctx_name.exception.code, "MPT_INVALID_TEMPLATE_NAME")

                payload = self._base_payload()
                payload["items"] = []
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_empty:
                    svc.create_template(**payload)
                self.assertEqual(ctx_empty.exception.code, "MPT_EMPTY_TEMPLATE")
            finally:
                conn.close()

    def test_max_items_and_uniqueness_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["items"] = [
                    {
                        "item_key": f"k{i}",
                        "slot_code": f"s{i}",
                        "position": i,
                        "title": f"T{i}",
                        "day_of_month": 1,
                        "notes": None,
                    }
                    for i in range(1, 202)
                ]
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_max:
                    svc.create_template(**payload)
                self.assertEqual(ctx_max.exception.code, "MPT_TOO_MANY_ITEMS")

                dup = self._base_payload()
                dup["items"] = [
                    {
                        "item_key": "same",
                        "slot_code": "slot1",
                        "position": 1,
                        "title": "t1",
                        "day_of_month": 1,
                        "notes": None,
                    },
                    {
                        "item_key": "same",
                        "slot_code": "slot2",
                        "position": 2,
                        "title": "t2",
                        "day_of_month": 2,
                        "notes": None,
                    },
                ]
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_dup:
                    svc.create_template(**dup)
                self.assertEqual(ctx_dup.exception.code, "MPT_DUPLICATE_ITEM_KEY")

                dup_position = self._base_payload()
                dup_position["items"] = [
                    {
                        "item_key": "a1",
                        "slot_code": "slot1",
                        "position": 1,
                        "title": "t1",
                        "day_of_month": 1,
                        "notes": None,
                    },
                    {
                        "item_key": "a2",
                        "slot_code": "slot2",
                        "position": 1,
                        "title": "t2",
                        "day_of_month": 2,
                        "notes": None,
                    },
                ]
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_dup_position:
                    svc.create_template(**dup_position)
                self.assertEqual(ctx_dup_position.exception.code, "MPT_DUPLICATE_POSITION")
                self.assertIn("unique within template", ctx_dup_position.exception.message)
            finally:
                conn.close()

    def test_item_schema_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["items"][0]["item_key"] = "Bad Key"
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_item_key:
                    svc.create_template(**payload)
                self.assertEqual(ctx_item_key.exception.code, "MPT_INVALID_ITEM_KEY")

                payload = self._base_payload()
                payload["items"][0]["slot_code"] = "bad slot"
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_slot:
                    svc.create_template(**payload)
                self.assertEqual(ctx_slot.exception.code, "MPT_INVALID_SLOT_CODE")

                payload = self._base_payload()
                payload["items"][0]["day_of_month"] = 44
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_day:
                    svc.create_template(**payload)
                self.assertEqual(ctx_day.exception.code, "MPT_INVALID_ITEM_DAY")

                payload = self._base_payload()
                payload["items"][0]["position"] = "bad"
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx_position:
                    svc.create_template(**payload)
                self.assertEqual(ctx_position.exception.code, "MPT_INVALID_ITEM_POSITION")
                self.assertIn("integer >= 1", ctx_position.exception.message)
            finally:
                conn.close()

    def test_archive_and_visibility(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = svc.create_template(**self._base_payload())
                archived = svc.archive_template(created["id"], archived_by="tester")
                self.assertEqual(archived["status"], "ARCHIVED")

                listed = svc.list_templates(MonthlyPlanningTemplateListParams(status="ARCHIVED", limit=10, offset=0))
                self.assertEqual(listed["total"], 1)
                self.assertEqual(listed["items"][0]["status"], "ARCHIVED")
            finally:
                conn.close()

    def test_preview_target_month_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx:
                    svc.preview_apply(created["id"], channel_id=1, target_month="2026-13")
                self.assertEqual(ctx.exception.code, "MPT_INVALID_TARGET_MONTH")
            finally:
                conn.close()

    def test_preview_resolves_planned_date_and_blocks_invalid_day_for_month(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["items"] = [
                    {**payload["items"][0], "item_key": "day-30", "slot_code": "day_30", "position": 1, "day_of_month": 30},
                    {**payload["items"][0], "item_key": "day-31", "slot_code": "day_31", "position": 2, "day_of_month": 31},
                ]
                created = svc.create_template(**payload)
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(preview["items"][0]["planned_date"], "2026-04-30")
                self.assertEqual(preview["items"][0]["outcome"], "WOULD_CREATE")
                self.assertIsNone(preview["items"][1]["planned_date"])
                self.assertEqual(preview["items"][1]["outcome"], "BLOCKED_INVALID_DATE")
                self.assertEqual(preview["items"][1]["reasons"][0]["code"], "MPT_INVALID_ITEM_DAY_FOR_MONTH")
            finally:
                conn.close()

    def test_preview_hard_duplicate_by_planning_slot_code(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "day_01_main"),
                )
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(preview["summary"]["blocked_duplicates"], 1)
                self.assertEqual(preview["items"][0]["outcome"], "BLOCKED_DUPLICATE")
            finally:
                conn.close()

    def test_preview_hard_duplicate_by_provenance_keys_even_when_publish_at_month_differs(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                conn.execute(
                    """
                    INSERT INTO planned_releases(
                        channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at,
                        source_template_id, source_template_item_key, source_template_target_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "darkwood-reverie",
                        "LONG",
                        "existing",
                        "2026-03-02",
                        None,
                        "PLANNED",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        int(created["id"]),
                        "day-01-main",
                        "2026-04",
                    ),
                )
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(preview["items"][0]["outcome"], "BLOCKED_DUPLICATE")
            finally:
                conn.close()

    def test_preview_soft_overlap_detection_is_informational(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "another_slot"),
                )
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(preview["items"][0]["outcome"], "WOULD_CREATE")
                self.assertEqual(len(preview["items"][0]["overlap_warnings"]), 1)
                self.assertEqual(preview["summary"]["overlap_warnings"], 1)
            finally:
                conn.close()

    def test_preview_fingerprint_is_deterministic(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                one = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                two = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(one["preview_fingerprint"], two["preview_fingerprint"])

                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "day_01_main"),
                )
                three = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertNotEqual(one["preview_fingerprint"], three["preview_fingerprint"])
            finally:
                conn.close()

    def test_preview_summary_copy_friendly_counters_shape(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["items"] = [
                    {**payload["items"][0], "item_key": "a1", "slot_code": "s1", "position": 1, "day_of_month": 1},
                    {**payload["items"][0], "item_key": "a2", "slot_code": "s2", "position": 2, "day_of_month": 31},
                ]
                created = svc.create_template(**payload)
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(sorted(preview["summary"].keys()), ["blocked_duplicates", "blocked_invalid_dates", "overlap_warnings", "total_items", "would_create"])
                self.assertEqual(preview["summary"]["total_items"], 2)
            finally:
                conn.close()

    def test_execute_apply_requires_preview_fingerprint(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                with self.assertRaises(MonthlyPlanningTemplateError) as ctx:
                    svc.execute_apply(
                        created["id"],
                        channel_id=1,
                        target_month="2026-04",
                        preview_fingerprint="",
                        request_id="r-1",
                    )
                self.assertEqual(ctx.exception.code, "MPT_PREVIEW_FINGERPRINT_REQUIRED")
            finally:
                conn.close()

    def test_execute_apply_success_writes_provenance_and_audit(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                payload = self._base_payload()
                payload["items"] = [
                    {**payload["items"][0], "item_key": "a2", "slot_code": "s2", "position": 2, "day_of_month": 2},
                    {**payload["items"][0], "item_key": "a1", "slot_code": "s1", "position": 1, "day_of_month": 1},
                ]
                created = svc.create_template(**payload)
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                out = svc.execute_apply(
                    created["id"],
                    channel_id=1,
                    target_month="2026-04",
                    preview_fingerprint=preview["preview_fingerprint"],
                    request_id="r-2",
                )
                self.assertEqual(out["summary"]["created"], 2)
                self.assertEqual([row["item_key"] for row in out["items"]], ["a1", "a2"])

                run_row = conn.execute(
                    "SELECT status, created_count, blocked_duplicate_count, blocked_invalid_date_count, failed_count FROM monthly_planning_template_apply_runs WHERE id = ?",
                    (out["apply_run_id"],),
                ).fetchone()
                self.assertEqual(run_row["status"], "COMPLETED")
                self.assertEqual(int(run_row["created_count"]), 2)

                created_rows = conn.execute(
                    """
                    SELECT source_template_id, source_template_item_key, source_template_target_month, source_template_apply_run_id, planning_slot_code
                    FROM planned_releases
                    WHERE source_template_apply_run_id = ?
                    ORDER BY source_template_item_key
                    """,
                    (out["apply_run_id"],),
                ).fetchall()
                self.assertEqual(len(created_rows), 2)
                self.assertEqual(str(created_rows[0]["source_template_target_month"]), "2026-04")
                self.assertEqual(int(created_rows[0]["source_template_apply_run_id"]), int(out["apply_run_id"]))
            finally:
                conn.close()

    def test_execute_apply_stale_preview_and_duplicate_translation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "day_01_main"),
                )
                with self.assertRaises(MonthlyPlanningTemplateError) as stale:
                    svc.execute_apply(
                        created["id"],
                        channel_id=1,
                        target_month="2026-04",
                        preview_fingerprint=preview["preview_fingerprint"],
                        request_id="r-3",
                    )
                self.assertEqual(stale.exception.code, "MPT_PREVIEW_STALE")

                fresh = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                out = svc.execute_apply(
                    created["id"],
                    channel_id=1,
                    target_month="2026-04",
                    preview_fingerprint=fresh["preview_fingerprint"],
                    request_id="r-4",
                )
                self.assertEqual(out["summary"]["created"], 0)
                self.assertEqual(out["summary"]["blocked_duplicates"], 1)
                self.assertEqual(out["items"][0]["outcome"], "BLOCKED_DUPLICATE")
            finally:
                conn.close()

    def test_execute_apply_conflict_translation_is_deterministic_for_uniqueness_race_like_condition(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                svc = MonthlyPlanningTemplateService(conn)
                created = self._create_template(svc)
                stale_preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")

                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "day_01_main"),
                )

                with self.assertRaises(MonthlyPlanningTemplateError) as stale:
                    svc.execute_apply(
                        created["id"],
                        channel_id=1,
                        target_month="2026-04",
                        preview_fingerprint=stale_preview["preview_fingerprint"],
                        request_id="r-race-stale",
                    )
                self.assertEqual(stale.exception.code, "MPT_PREVIEW_STALE")

                fresh_preview = svc.preview_apply(created["id"], channel_id=1, target_month="2026-04")
                self.assertEqual(fresh_preview["items"][0]["outcome"], "BLOCKED_DUPLICATE")
                self.assertEqual(fresh_preview["items"][0]["reasons"][0]["code"], "MPT_DUPLICATE_PLANNING_SLOT")

                out = svc.execute_apply(
                    created["id"],
                    channel_id=1,
                    target_month="2026-04",
                    preview_fingerprint=fresh_preview["preview_fingerprint"],
                    request_id="r-race-fresh",
                )
                self.assertEqual(out["summary"]["created"], 0)
                self.assertEqual(out["summary"]["blocked_duplicates"], 1)
                self.assertEqual(out["items"][0]["outcome"], "BLOCKED_DUPLICATE")
                self.assertEqual(out["items"][0]["reasons"][0]["code"], "MPT_DUPLICATE_PLANNING_SLOT")

                run_item = conn.execute(
                    """
                    SELECT outcome, reason_code
                    FROM monthly_planning_template_apply_run_items
                    WHERE apply_run_id = ?
                    """,
                    (int(out["apply_run_id"]),),
                ).fetchone()
                self.assertEqual(str(run_item["outcome"]), "BLOCKED_DUPLICATE")
                self.assertEqual(str(run_item["reason_code"]), "MPT_DUPLICATE_PLANNING_SLOT")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
