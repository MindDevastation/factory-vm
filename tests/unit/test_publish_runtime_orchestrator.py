from __future__ import annotations

import unittest

from services.publish_runtime.domain import PUBLISH_STATE_VALUES
from services.publish_runtime.orchestrator import (
    ENRICHMENT_IMMUTABLE_FIELDS,
    ENRICHMENT_MUTABLE_FIELDS,
    PublishTransitionError,
    PublishTransitionRequest,
    is_publish_transition_allowed,
    validate_publish_transition,
)


ALLOWED: dict[str, dict[str, set[str]]] = {
    "system_automatic": {
        "private_uploaded": set(),
        "policy_blocked": set(),
        "waiting_for_schedule": {"ready_to_publish"},
        "ready_to_publish": {"publish_in_progress"},
        "publish_in_progress": {
            "published_public",
            "published_unlisted",
            "retry_pending",
            "manual_handoff_pending",
            "publish_failed_terminal",
        },
        "retry_pending": {"ready_to_publish"},
        "manual_handoff_pending": set(),
        "manual_handoff_acknowledged": set(),
        "manual_publish_completed": set(),
        "published_public": set(),
        "published_unlisted": set(),
        "publish_failed_terminal": set(),
        "publish_state_drift_detected": set(),
    },
    "system_internal": {
        "private_uploaded": {"policy_blocked", "waiting_for_schedule", "ready_to_publish", "publish_state_drift_detected"},
        "policy_blocked": {"waiting_for_schedule", "ready_to_publish", "manual_handoff_pending", "publish_state_drift_detected"},
        "waiting_for_schedule": {"policy_blocked", "publish_state_drift_detected"},
        "ready_to_publish": {"policy_blocked", "waiting_for_schedule", "publish_state_drift_detected"},
        "publish_in_progress": {"publish_state_drift_detected"},
        "retry_pending": {"policy_blocked", "manual_handoff_pending", "publish_state_drift_detected"},
        "manual_handoff_pending": {"publish_state_drift_detected"},
        "manual_handoff_acknowledged": {"publish_state_drift_detected"},
        "manual_publish_completed": set(),
        "published_public": set(),
        "published_unlisted": set(),
        "publish_failed_terminal": set(),
        "publish_state_drift_detected": {"manual_handoff_pending", "published_public", "published_unlisted"},
    },
    "operator_manual": {
        "private_uploaded": {"manual_handoff_pending"},
        "policy_blocked": {"waiting_for_schedule", "ready_to_publish", "manual_handoff_pending"},
        "waiting_for_schedule": {"policy_blocked", "ready_to_publish", "manual_handoff_pending"},
        "ready_to_publish": {"policy_blocked", "waiting_for_schedule", "manual_handoff_pending"},
        "publish_in_progress": set(),
        "retry_pending": {"policy_blocked", "waiting_for_schedule", "ready_to_publish", "manual_handoff_pending"},
        "manual_handoff_pending": {"manual_handoff_acknowledged"},
        "manual_handoff_acknowledged": {"manual_publish_completed", "publish_failed_terminal"},
        "manual_publish_completed": set(),
        "published_public": set(),
        "published_unlisted": set(),
        "publish_failed_terminal": set(),
        "publish_state_drift_detected": {"manual_handoff_pending", "manual_publish_completed"},
    },
}


class TestPublishRuntimeOrchestrator(unittest.TestCase):
    def test_matrix_exact_allow_and_deny_for_non_enrichment_actors(self) -> None:
        actors = ("system_automatic", "system_internal", "operator_manual")
        for actor in actors:
            for from_state in PUBLISH_STATE_VALUES:
                allowed_targets = ALLOWED[actor][from_state]
                for to_state in PUBLISH_STATE_VALUES:
                    expected = to_state in allowed_targets
                    actual = is_publish_transition_allowed(
                        from_publish_state=from_state,
                        to_publish_state=to_state,
                        transition_actor_class=actor,
                        job_state="WAIT_APPROVAL",
                    )
                    self.assertEqual(
                        actual,
                        expected,
                        msg=f"actor={actor} from={from_state} to={to_state}",
                    )

    def test_same_state_allowed_only_for_enrichment(self) -> None:
        for state in PUBLISH_STATE_VALUES:
            self.assertTrue(
                is_publish_transition_allowed(
                    from_publish_state=state,
                    to_publish_state=state,
                    transition_actor_class="enrichment_only",
                    job_state="WAIT_APPROVAL",
                    changed_fields={"publish_reason_detail"},
                )
            )
            for actor in ("system_automatic", "system_internal", "operator_manual"):
                self.assertFalse(
                    is_publish_transition_allowed(
                        from_publish_state=state,
                        to_publish_state=state,
                        transition_actor_class=actor,
                        job_state="WAIT_APPROVAL",
                    )
                )

    def test_terminal_success_immutable_except_enrichment(self) -> None:
        terminals = ("manual_publish_completed", "published_public", "published_unlisted")
        for state in terminals:
            for actor in ("system_automatic", "system_internal", "operator_manual"):
                for to_state in PUBLISH_STATE_VALUES:
                    self.assertFalse(
                        is_publish_transition_allowed(
                            from_publish_state=state,
                            to_publish_state=to_state,
                            transition_actor_class=actor,
                            job_state="WAIT_APPROVAL",
                        )
                    )
            self.assertTrue(
                is_publish_transition_allowed(
                    from_publish_state=state,
                    to_publish_state=state,
                    transition_actor_class="enrichment_only",
                    job_state="WAIT_APPROVAL",
                    changed_fields={"publish_manual_url"},
                )
            )

    def test_publish_failed_terminal_has_no_state_changing_transition(self) -> None:
        for actor in ("system_automatic", "system_internal", "operator_manual"):
            for to_state in PUBLISH_STATE_VALUES:
                self.assertFalse(
                    is_publish_transition_allowed(
                        from_publish_state="publish_failed_terminal",
                        to_publish_state=to_state,
                        transition_actor_class=actor,
                        job_state="WAIT_APPROVAL",
                    )
                )

    def test_drift_failed_terminal_separation_is_enforced(self) -> None:
        for actor in ("system_automatic", "system_internal", "operator_manual", "enrichment_only"):
            self.assertFalse(
                is_publish_transition_allowed(
                    from_publish_state="publish_state_drift_detected",
                    to_publish_state="publish_failed_terminal",
                    transition_actor_class=actor,
                    job_state="WAIT_APPROVAL",
                )
            )
            self.assertFalse(
                is_publish_transition_allowed(
                    from_publish_state="publish_failed_terminal",
                    to_publish_state="publish_state_drift_detected",
                    transition_actor_class=actor,
                    job_state="WAIT_APPROVAL",
                )
            )

    def test_cancelled_blocks_all_non_enrichment_transitions(self) -> None:
        for actor in ("system_automatic", "system_internal", "operator_manual"):
            for from_state in PUBLISH_STATE_VALUES:
                allowed_targets = ALLOWED[actor][from_state]
                for to_state in allowed_targets:
                    self.assertFalse(
                        is_publish_transition_allowed(
                            from_publish_state=from_state,
                            to_publish_state=to_state,
                            transition_actor_class=actor,
                            job_state="CANCELLED",
                        )
                    )

        self.assertTrue(
            is_publish_transition_allowed(
                from_publish_state="ready_to_publish",
                to_publish_state="ready_to_publish",
                transition_actor_class="enrichment_only",
                job_state="CANCELLED",
                changed_fields={"publish_reason_detail"},
            )
        )

    def test_enrichment_fields_allowlist_and_denylist(self) -> None:
        for field in ENRICHMENT_MUTABLE_FIELDS:
            self.assertTrue(
                is_publish_transition_allowed(
                    from_publish_state="publish_in_progress",
                    to_publish_state="publish_in_progress",
                    transition_actor_class="enrichment_only",
                    job_state="WAIT_APPROVAL",
                    changed_fields={field},
                )
            )

        for field in ENRICHMENT_IMMUTABLE_FIELDS:
            self.assertFalse(
                is_publish_transition_allowed(
                    from_publish_state="publish_in_progress",
                    to_publish_state="publish_in_progress",
                    transition_actor_class="enrichment_only",
                    job_state="WAIT_APPROVAL",
                    changed_fields={field},
                )
            )

        self.assertFalse(
            is_publish_transition_allowed(
                from_publish_state="publish_in_progress",
                to_publish_state="publish_in_progress",
                transition_actor_class="enrichment_only",
                job_state="WAIT_APPROVAL",
                changed_fields={"random_field"},
            )
        )

    def test_enrichment_cannot_change_state(self) -> None:
        with self.assertRaises(PublishTransitionError):
            validate_publish_transition(
                PublishTransitionRequest(
                    from_publish_state="ready_to_publish",
                    to_publish_state="publish_in_progress",
                    transition_actor_class="enrichment_only",
                    job_state="WAIT_APPROVAL",
                    changed_fields=frozenset({"publish_reason_code"}),
                )
            )


if __name__ == "__main__":
    unittest.main()
