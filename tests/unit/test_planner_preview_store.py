from __future__ import annotations

import unittest
from unittest.mock import patch

from services.planner.preview_store import (
    MAX_PREVIEWS,
    PREVIEW_TTL_SECONDS,
    PreviewAlreadyUsedError,
    PreviewExpiredError,
    PreviewNotFoundError,
    PreviewStore,
    PreviewUsernameMismatchError,
)


class TestPlannerPreviewStore(unittest.TestCase):
    @patch("services.planner.preview_store.time.time", return_value=100.0)
    def test_put_and_get_for_same_username(self, _mock_time) -> None:
        store = PreviewStore()
        preview_id = store.put("alice", {"title": "draft"})

        preview = store.get("alice", preview_id)

        self.assertEqual(preview, {"title": "draft"})

    @patch("services.planner.preview_store.time.time", side_effect=[100.0, 100.0])
    def test_get_rejects_username_mismatch(self, _mock_time) -> None:
        store = PreviewStore()
        preview_id = store.put("alice", {"title": "draft"})

        with self.assertRaises(PreviewUsernameMismatchError):
            store.get("bob", preview_id)

    @patch("services.planner.preview_store.time.time", side_effect=[100.0, 100.0 + PREVIEW_TTL_SECONDS + 1])
    def test_get_raises_expired(self, _mock_time) -> None:
        store = PreviewStore()
        preview_id = store.put("alice", {"title": "draft"})

        with self.assertRaises(PreviewExpiredError):
            store.get("alice", preview_id)

    @patch("services.planner.preview_store.time.time", return_value=100.0)
    def test_mark_used_makes_preview_single_use(self, _mock_time) -> None:
        store = PreviewStore()
        preview_id = store.put("alice", {"title": "draft"})

        store.mark_used(preview_id)

        with self.assertRaises(PreviewAlreadyUsedError):
            store.get("alice", preview_id)

    def test_store_capacity_is_bounded(self) -> None:
        current = 1000.0

        def _now() -> float:
            nonlocal current
            current += 1
            return current

        store = PreviewStore(max_previews=MAX_PREVIEWS, now_fn=_now)
        first_id = store.put("alice", {"idx": 0})
        for idx in range(1, MAX_PREVIEWS + 1):
            store.put("alice", {"idx": idx})

        with self.assertRaises(PreviewNotFoundError):
            store.get("alice", first_id)

    @patch("services.planner.preview_store.time.time", return_value=100.0)
    def test_mark_used_missing_id_raises_not_found(self, _mock_time) -> None:
        store = PreviewStore()

        with self.assertRaises(PreviewNotFoundError):
            store.mark_used("missing")


if __name__ == "__main__":
    unittest.main()
