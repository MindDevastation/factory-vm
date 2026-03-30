from __future__ import annotations

import unittest

from services.visual_domain import validate_applied_package_shape


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


if __name__ == "__main__":
    unittest.main()
