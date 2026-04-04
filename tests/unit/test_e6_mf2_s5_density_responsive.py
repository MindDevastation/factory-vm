from __future__ import annotations

import unittest

from services.factory_api.density_responsive import density_profile_contract, density_responsive_catalog, responsive_fallback_contract


class TestE6Mf2S5DensityResponsive(unittest.TestCase):
    def test_density_profile_contract(self) -> None:
        density = density_profile_contract()
        self.assertEqual(density["density_mode"], "DESKTOP_COMPACT")
        self.assertTrue(density["anti_showcase"])

    def test_responsive_fallback_contract(self) -> None:
        fallback = responsive_fallback_contract()
        self.assertGreater(fallback["tablet_breakpoint_px"], fallback["mobile_breakpoint_px"])
        self.assertEqual(fallback["fallback_mode"], "READABLE_STACKED")

    def test_catalog_shape(self) -> None:
        catalog = density_responsive_catalog()
        self.assertIn("density", catalog)
        self.assertIn("responsive_fallback", catalog)


if __name__ == "__main__":
    unittest.main()
