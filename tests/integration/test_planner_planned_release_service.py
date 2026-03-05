from __future__ import annotations

import unittest

from services.common import db as dbm
from tests._helpers import temp_env
from services.planner.planned_release_service import (
    PlannedReleaseListParams,
    PlannedReleaseLockedError,
    PlannedReleaseNotFoundError,
    PlannedReleaseService,
)


class TestPlannedReleaseService(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_env_ctx = temp_env()
        self._td, self.env = self._temp_env_ctx.__enter__()
        self.conn = dbm.connect(self.env)
        dbm.migrate(self.conn)
        self.service = PlannedReleaseService(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self._temp_env_ctx.__exit__(None, None, None)

    def test_create_and_get_by_id(self) -> None:
        created = self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Launch One",
            publish_at="2027-01-01T00:00:00Z",
            notes="note",
        )

        self.assertEqual(created["status"], "PLANNED")
        self.assertIsNotNone(created["created_at"])
        self.assertIsNotNone(created["updated_at"])

        fetched = self.service.get_by_id(int(created["id"]))
        self.assertEqual(fetched["title"], "Launch One")

    def test_get_by_id_raises_not_found(self) -> None:
        with self.assertRaises(PlannedReleaseNotFoundError):
            self.service.get_by_id(12345)

    def test_list_filters_sort_search_and_pagination(self) -> None:
        self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Alpha Title",
            publish_at="2027-01-01T00:00:00Z",
            notes=None,
        )
        self.service.create(
            channel_slug="channel-a",
            content_type="short",
            title="Beta Title",
            publish_at="2027-01-02T00:00:00Z",
            notes=None,
        )
        self.service.create(
            channel_slug="channel-b",
            content_type="video",
            title="Gamma",
            publish_at="2027-01-03T00:00:00Z",
            notes=None,
        )

        result = self.service.list(
            PlannedReleaseListParams(
                channel_slug="channel-a",
                content_type="video",
                search="alpha",
                sort_by="publish_at",
                sort_dir="asc",
                limit=1,
                offset=0,
            )
        )

        self.assertEqual(result["total"], 1)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(result["items"][0]["title"], "Alpha Title")

        fallback_sort = self.service.list(
            PlannedReleaseListParams(sort_by="not_allowed", sort_dir="asc", limit=2, offset=1)
        )
        self.assertEqual(fallback_sort["limit"], 2)
        self.assertEqual(fallback_sort["offset"], 1)
        self.assertEqual(len(fallback_sort["items"]), 2)

    def test_update_only_editable_fields_and_status_not_editable(self) -> None:
        created = self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Initial",
            publish_at=None,
            notes=None,
        )
        rid = int(created["id"])

        updated = self.service.update(
            rid,
            {
                "channel_slug": "channel-z",
                "content_type": "short",
                "title": "Updated",
                "publish_at": "2027-01-10T00:00:00Z",
                "notes": "new note",
                "status": "FAILED",
            },
        )

        self.assertEqual(updated["channel_slug"], "channel-z")
        self.assertEqual(updated["content_type"], "short")
        self.assertEqual(updated["title"], "Updated")
        self.assertEqual(updated["publish_at"], "2027-01-10T00:00:00Z")
        self.assertEqual(updated["notes"], "new note")
        self.assertEqual(updated["status"], "PLANNED")

    def test_update_noop_does_not_raise_when_rowcount_zero(self) -> None:
        created = self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Initial",
            publish_at=None,
            notes=None,
        )
        rid = int(created["id"])

        self.service._now_iso = lambda: str(created["updated_at"])
        updated = self.service.update(rid, {"title": created["title"]})

        self.assertEqual(updated["status"], "PLANNED")
        self.assertEqual(updated["title"], created["title"])

    def test_update_and_delete_sql_are_status_gated(self) -> None:
        created = self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Initial",
            publish_at=None,
            notes=None,
        )
        rid = int(created["id"])

        traced_sql: list[str] = []
        self.conn.set_trace_callback(traced_sql.append)
        try:
            self.service.update(rid, {"title": "Updated"})
            self.service.delete(rid)
        finally:
            self.conn.set_trace_callback(None)

        normalized = [sql.lower() for sql in traced_sql]
        update_sql = next(sql for sql in normalized if "update planned_releases" in sql)
        delete_sql = next(sql for sql in normalized if "delete from planned_releases" in sql)

        self.assertIn("and status", update_sql)
        self.assertIn("planned", update_sql)
        self.assertIn("and status", delete_sql)
        self.assertIn("planned", delete_sql)

    def test_update_and_delete_raise_locked_when_status_not_planned(self) -> None:
        created = self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Initial",
            publish_at=None,
            notes=None,
        )
        rid = int(created["id"])
        self.conn.execute("UPDATE planned_releases SET status = 'LOCKED' WHERE id = ?", (rid,))

        with self.assertRaises(PlannedReleaseLockedError):
            self.service.update(rid, {"title": "nope"})

        with self.assertRaises(PlannedReleaseLockedError):
            self.service.delete(rid)

    def test_delete_planned_release(self) -> None:
        created = self.service.create(
            channel_slug="channel-a",
            content_type="video",
            title="Initial",
            publish_at=None,
            notes=None,
        )
        rid = int(created["id"])

        self.service.delete(rid)

        with self.assertRaises(PlannedReleaseNotFoundError):
            self.service.get_by_id(rid)


if __name__ == "__main__":
    unittest.main()
