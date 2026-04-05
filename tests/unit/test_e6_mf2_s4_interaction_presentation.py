from __future__ import annotations

import unittest

from services.factory_api.interaction_presentation import (
    button_hierarchy_contract,
    destructive_affordance_policy,
    interaction_mode_policy,
    result_presentation_contract,
)


class TestE6Mf2S4InteractionPresentation(unittest.TestCase):
    def test_button_hierarchy_contract(self) -> None:
        order = button_hierarchy_contract()["priority_order"]
        self.assertEqual(order, ["PRIMARY", "SECONDARY", "TERTIARY", "DESTRUCTIVE"])

    def test_inline_vs_modal_policy(self) -> None:
        self.assertEqual(interaction_mode_policy(action_kind="SINGLE", is_destructive=False), "INLINE_ALLOWED")
        self.assertEqual(interaction_mode_policy(action_kind="BATCH", is_destructive=False), "MODAL_CONFIRM")
        self.assertEqual(interaction_mode_policy(action_kind="SINGLE", is_destructive=True), "MODAL_CONFIRM")

    def test_destructive_affordance_rules(self) -> None:
        policy = destructive_affordance_policy(action_name="delete")
        self.assertTrue(policy["requires_confirmation"])
        self.assertTrue(policy["requires_explicit_label"])
        self.assertEqual(policy["default_variant"], "danger")

    def test_result_presentation_contract(self) -> None:
        self.assertEqual(result_presentation_contract(success=True, partial=False, blocked=False)["state"], "SUCCESS")
        self.assertEqual(result_presentation_contract(success=False, partial=True, blocked=False)["state"], "PARTIAL")
        self.assertEqual(result_presentation_contract(success=False, partial=False, blocked=True)["state"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
