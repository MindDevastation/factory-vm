from __future__ import annotations

import unittest

from services.visual_domain import build_visual_package_summary, validate_applied_package_shape


class TestVisualDomain(unittest.TestCase):
    def test_accepts_exact_background_and_cover_shape(self) -> None:
        validate_applied_package_shape({"background_asset_id": 1, "cover_asset_id": 2})

    def test_rejects_missing_background(self) -> None:
        with self.assertRaisesRegex(ValueError, "background_asset_id"):
            validate_applied_package_shape({"cover_asset_id": 2})

    def test_rejects_missing_cover(self) -> None:
        with self.assertRaisesRegex(ValueError, "cover_asset_id"):
            validate_applied_package_shape({"background_asset_id": 1})

    def test_rejects_any_extra_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "unexpected applied package keys"):
            validate_applied_package_shape(
                {"background_asset_id": 1, "cover_asset_id": 2, "extra": "not-allowed"}
            )

    def test_rejects_thumbnail_key(self) -> None:
        with self.assertRaisesRegex(ValueError, "thumbnail"):
            validate_applied_package_shape(
                {"background_asset_id": 1, "cover_asset_id": 2, "thumbnail_asset_id": 3}
            )

    def test_build_visual_package_summary_returns_canonical_structure(self) -> None:
        summary = build_visual_package_summary(
            release_id=77,
            package={
                "background_asset_id": 101,
                "cover_asset_id": 202,
                "is_auto_assisted": True,
                "operator_override_fields": ["cover_asset_id", "cover_asset_id", "template_ref"],
                "warnings": ["cover from operator override"],
            },
            template_ref={"template_id": "tmpl-1", "name": "default"},
        )

        self.assertEqual(summary["release"], {"release_id": 77})
        self.assertEqual(summary["background_asset"], {"asset_id": 101})
        self.assertEqual(summary["cover_asset"], {"asset_id": 202})
        self.assertEqual(summary["thumbnail_source"], {"source_kind": "cover_asset", "asset_id": 202})
        self.assertEqual(summary["template_ref"], {"template_id": "tmpl-1", "name": "default"})
        self.assertEqual(
            summary["markers"],
            {
                "is_auto_assisted": True,
                "operator_overrides": ["cover_asset_id", "template_ref"],
            },
        )
        self.assertEqual(summary["warnings"], ["cover from operator override"])


if __name__ == "__main__":
    unittest.main()
