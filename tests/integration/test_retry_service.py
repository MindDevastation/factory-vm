from __future__ import annotations

import unittest

from services.common import db as dbm
from services.ui_jobs.retry_service import (
    UiJobRetryNotFoundError,
    UiJobRetryStatusError,
    retry_failed_ui_job,
)
from tests._helpers import seed_minimal_db, temp_env


class TestUiJobRetryService(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_env_ctx = temp_env()
        self._td, self.env = self._temp_env_ctx.__enter__()
        seed_minimal_db(self.env)
        self.conn = dbm.connect(self.env)

    def tearDown(self) -> None:
        self.conn.close()
        self._temp_env_ctx.__exit__(None, None, None)

    def _channel_id(self) -> int:
        row = self.conn.execute("SELECT id FROM channels WHERE slug = ?", ("darkwood-reverie",)).fetchone()
        assert row is not None
        return int(row["id"])

    def _create_ui_job(self) -> int:
        return dbm.create_ui_job_draft(
            self.conn,
            channel_id=self._channel_id(),
            title="Retry Test",
            description="desc",
            tags_csv="a,b",
            cover_name="cover",
            cover_ext="png",
            background_name="bg",
            background_ext="png",
            audio_ids_text="1,2",
            job_type="UI",
        )

    def _mark_failed(self, job_id: int) -> None:
        dbm.update_job_state(self.conn, job_id, state="FAILED", stage="RENDER", error_reason="boom")

    def test_not_found_raises_typed_error(self) -> None:
        with self.assertRaises(UiJobRetryNotFoundError):
            retry_failed_ui_job(self.conn, source_job_id=999_999, enqueue_retry_child=lambda _c, _j: None)

    def test_non_failed_raises_typed_error(self) -> None:
        source_job_id = self._create_ui_job()

        with self.assertRaises(UiJobRetryStatusError):
            retry_failed_ui_job(self.conn, source_job_id=source_job_id, enqueue_retry_child=lambda _c, _j: None)

    def test_failed_without_existing_child_creates_retry_with_lineage(self) -> None:
        source_job_id = self._create_ui_job()
        self._mark_failed(source_job_id)

        called: list[int] = []

        def _enqueue(_conn, retry_job_id: int) -> None:
            called.append(retry_job_id)

        result = retry_failed_ui_job(self.conn, source_job_id=source_job_id, enqueue_retry_child=_enqueue)

        self.assertTrue(result.created)
        self.assertEqual(called, [result.retry_job_id])

        row = self.conn.execute(
            """
            SELECT retry_of_job_id, root_job_id, attempt_no, force_refetch_inputs, state, stage
            FROM jobs
            WHERE id = ?
            """,
            (result.retry_job_id,),
        ).fetchone()
        assert row is not None
        self.assertEqual(int(row["retry_of_job_id"]), source_job_id)
        self.assertEqual(int(row["root_job_id"]), source_job_id)
        self.assertEqual(int(row["attempt_no"]), 2)
        self.assertEqual(int(row["force_refetch_inputs"]), 1)
        self.assertEqual(str(row["state"]), "DRAFT")
        self.assertEqual(str(row["stage"]), "DRAFT")

        draft = self.conn.execute(
            "SELECT title, background_name, audio_ids_text FROM ui_job_drafts WHERE job_id = ?",
            (result.retry_job_id,),
        ).fetchone()
        assert draft is not None
        self.assertEqual(str(draft["title"]), "Retry Test")
        self.assertEqual(str(draft["background_name"]), "bg")
        self.assertEqual(str(draft["audio_ids_text"]), "1,2")

    def test_failed_with_existing_child_is_noop_and_returns_same_retry_job_id(self) -> None:
        source_job_id = self._create_ui_job()
        self._mark_failed(source_job_id)

        first = retry_failed_ui_job(self.conn, source_job_id=source_job_id, enqueue_retry_child=lambda _c, _j: None)
        second = retry_failed_ui_job(self.conn, source_job_id=source_job_id, enqueue_retry_child=lambda _c, _j: None)

        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(second.retry_job_id, first.retry_job_id)

    def test_enqueue_failure_rolls_back_without_orphan_retry_child(self) -> None:
        source_job_id = self._create_ui_job()
        self._mark_failed(source_job_id)

        with self.assertRaises(RuntimeError):
            retry_failed_ui_job(
                self.conn,
                source_job_id=source_job_id,
                enqueue_retry_child=lambda _c, _j: (_ for _ in ()).throw(RuntimeError("enqueue failed")),
            )

        child = self.conn.execute("SELECT id FROM jobs WHERE retry_of_job_id = ?", (source_job_id,)).fetchone()
        self.assertIsNone(child)
        child_draft = self.conn.execute(
            "SELECT job_id FROM ui_job_drafts WHERE job_id IN (SELECT id FROM jobs WHERE retry_of_job_id = ?)",
            (source_job_id,),
        ).fetchone()
        self.assertIsNone(child_draft)

    def test_retry_of_failed_retry_increments_attempt_and_preserves_root(self) -> None:
        source_job_id = self._create_ui_job()
        self._mark_failed(source_job_id)

        first_retry = retry_failed_ui_job(self.conn, source_job_id=source_job_id, enqueue_retry_child=lambda _c, _j: None)
        self._mark_failed(first_retry.retry_job_id)

        second_retry = retry_failed_ui_job(
            self.conn,
            source_job_id=first_retry.retry_job_id,
            enqueue_retry_child=lambda _c, _j: None,
        )

        row = self.conn.execute(
            "SELECT retry_of_job_id, root_job_id, attempt_no FROM jobs WHERE id = ?",
            (second_retry.retry_job_id,),
        ).fetchone()
        assert row is not None
        self.assertEqual(int(row["retry_of_job_id"]), first_retry.retry_job_id)
        self.assertEqual(int(row["root_job_id"]), source_job_id)
        self.assertEqual(int(row["attempt_no"]), 3)


if __name__ == "__main__":
    unittest.main()
