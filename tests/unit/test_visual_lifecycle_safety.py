from __future__ import annotations

import unittest

from services.visual_domain import (
    VisualLifecycleError,
    approved_preview_is_distinct_from_applied,
    build_apply_tokens,
    preview_snapshot_is_non_live,
    validate_apply_safety,
)


class TestVisualLifecycleSafety(unittest.TestCase):
    def _snapshot_row(self) -> dict[str, object]:
        return {
            "id": "pv-1",
            "release_id": 55,
            "intent_snapshot_json": '{"background":{"asset_id":111},"cover":{"asset_id":222}}',
            "preview_package_json": '{"background_asset_id":111,"cover_asset_id":222}',
        }

    def _approved_row(self, preview_id: str = "pv-1") -> dict[str, object]:
        return {
            "release_id": 55,
            "preview_id": preview_id,
        }

    def test_apply_without_preview_is_forbidden(self) -> None:
        with self.assertRaises(VisualLifecycleError) as ctx:
            validate_apply_safety(
                release_id=55,
                snapshot_row=None,
                approved_preview_row=self._approved_row(),
                applied_package_row=None,
                current_intent_config_json={"background": {"asset_id": 111}},
                provided_stale_token="x",
                provided_conflict_token="y",
            )
        self.assertEqual(ctx.exception.code, "VISUAL_PREVIEW_REQUIRED")

    def test_stale_preview_detection_rejects_apply(self) -> None:
        snapshot = self._snapshot_row()
        tokens = build_apply_tokens(
            release_id=55,
            snapshot_row=snapshot,
            current_intent_config_json={"background": {"asset_id": 111}, "cover": {"asset_id": 222}},
        )
        with self.assertRaises(VisualLifecycleError) as ctx:
            validate_apply_safety(
                release_id=55,
                snapshot_row=snapshot,
                approved_preview_row=self._approved_row(),
                applied_package_row=None,
                current_intent_config_json={"background": {"asset_id": 111}, "cover": {"asset_id": 222}},
                provided_stale_token=f"stale-{tokens.stale_token}",
                provided_conflict_token=tokens.conflict_token,
            )
        self.assertEqual(ctx.exception.code, "VISUAL_PREVIEW_STALE")

    def test_conflict_token_mismatch_rejects_apply(self) -> None:
        snapshot = self._snapshot_row()
        tokens = build_apply_tokens(
            release_id=55,
            snapshot_row=snapshot,
            current_intent_config_json={"background": {"asset_id": 111}, "cover": {"asset_id": 222}},
        )

        with self.assertRaises(VisualLifecycleError) as ctx:
            validate_apply_safety(
                release_id=55,
                snapshot_row=snapshot,
                approved_preview_row=self._approved_row(),
                applied_package_row=None,
                current_intent_config_json={"background": {"asset_id": 777}, "cover": {"asset_id": 222}},
                provided_stale_token=tokens.stale_token,
                provided_conflict_token=tokens.conflict_token,
            )
        self.assertEqual(ctx.exception.code, "VISUAL_APPLY_CONFLICT")

    def test_approved_preview_remains_distinct_from_applied_package(self) -> None:
        approved = self._approved_row(preview_id="pv-2")
        self.assertTrue(approved_preview_is_distinct_from_applied(approved_preview_row=approved, applied_package_row=None))
        self.assertTrue(
            approved_preview_is_distinct_from_applied(
                approved_preview_row=approved,
                applied_package_row={"source_preview_id": "pv-1"},
            )
        )
        self.assertFalse(
            approved_preview_is_distinct_from_applied(
                approved_preview_row=approved,
                applied_package_row={"source_preview_id": "pv-2"},
            )
        )

    def test_preview_snapshot_is_non_live_until_apply(self) -> None:
        preview_id = "pv-4"
        self.assertTrue(preview_snapshot_is_non_live(preview_id=preview_id, applied_package_row=None))
        self.assertTrue(
            preview_snapshot_is_non_live(
                preview_id=preview_id,
                applied_package_row={"source_preview_id": "pv-older"},
            )
        )
        self.assertFalse(
            preview_snapshot_is_non_live(
                preview_id=preview_id,
                applied_package_row={"source_preview_id": "pv-4"},
            )
        )


if __name__ == "__main__":
    unittest.main()
