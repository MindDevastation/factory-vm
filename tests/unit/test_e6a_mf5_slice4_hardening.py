from __future__ import annotations

import unittest

from services.telegram_inbox import build_batch_preview, resolve_bounded_targets


class TestE6AMf5Slice4Hardening(unittest.TestCase):
    def test_bounded_target_set_is_enforced(self) -> None:
        with self.assertRaises(ValueError):
            resolve_bounded_targets(selected_job_ids=list(range(1, 30)), max_targets=5)

    def test_preview_before_confirm_contract(self) -> None:
        preview = build_batch_preview(action="retry", selected_job_ids=[1, 2, 2])
        self.assertEqual(preview["targets"], [1, 2])
        self.assertTrue(bool(preview["requires_confirmation"]))


if __name__ == "__main__":
    unittest.main()
