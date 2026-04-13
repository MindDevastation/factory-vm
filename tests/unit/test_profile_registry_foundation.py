from __future__ import annotations

import unittest

from services.analytics_center.profile_registry import (
    CORE_ANALYZER_MODE,
    CHANNEL_STRATEGY_PROFILES,
    FORMAT_PROFILES,
    build_profile_registry_contract,
    profile_hook_fingerprint,
    resolve_profile_bundle,
)


class TestProfileRegistryFoundation(unittest.TestCase):
    def test_profile_registry_contract_and_invariant(self) -> None:
        contract = build_profile_registry_contract()
        self.assertEqual(contract["core_analyzer_mode"], CORE_ANALYZER_MODE)
        self.assertEqual(contract["core_analyzer_mode"], "ONE_ANALYZER_MANY_PROFILES")
        self.assertEqual(contract["channel_strategy_profiles"], list(CHANNEL_STRATEGY_PROFILES))
        self.assertEqual(contract["format_profiles"], list(FORMAT_PROFILES))
        self.assertEqual(contract["foundations_affected"], ["weighting", "baseline", "prediction", "recommendation", "planning"])
        self.assertEqual(len(contract["sample_hook_fingerprints"]), 2)
        self.assertNotEqual(contract["sample_hook_fingerprints"][0], contract["sample_hook_fingerprints"][1])

    def test_profile_bundle_changes_foundation_hooks(self) -> None:
        long_form = resolve_profile_bundle(
            channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
            format_profile="LONG_FORM",
        )
        singles = resolve_profile_bundle(
            channel_strategy_profile="DAILY_SINGLE_TRACK_RELEASES",
            format_profile="SINGLE_TRACK",
        )

        self.assertNotEqual(long_form.weighting_hooks, singles.weighting_hooks)
        self.assertNotEqual(long_form.baseline_hooks, singles.baseline_hooks)
        self.assertNotEqual(long_form.prediction_hooks, singles.prediction_hooks)
        self.assertNotEqual(long_form.recommendation_hooks, singles.recommendation_hooks)
        self.assertNotEqual(long_form.planning_hooks, singles.planning_hooks)
        self.assertNotEqual(profile_hook_fingerprint(long_form), profile_hook_fingerprint(singles))

    def test_unknown_profile_rejected(self) -> None:
        with self.assertRaises(ValueError):
            resolve_profile_bundle(channel_strategy_profile="UNKNOWN", format_profile="LONG_FORM")
        with self.assertRaises(ValueError):
            resolve_profile_bundle(channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC", format_profile="UNKNOWN")
