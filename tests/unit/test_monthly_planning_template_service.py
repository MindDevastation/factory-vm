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


if __name__ == "__main__":
    unittest.main()
