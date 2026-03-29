from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Collection, Literal, Mapping

from services.publish_runtime.domain import PUBLISH_STATE_VALUES, validate_publish_state

TransitionActorClass = Literal["system_automatic", "system_internal", "operator_manual", "enrichment_only"]

TERMINAL_SUCCESS_STATES = frozenset({"manual_publish_completed", "published_public", "published_unlisted"})
ENRICHMENT_MUTABLE_FIELDS = frozenset(
    {
        "publish_reason_code",
        "publish_reason_detail",
        "publish_last_error_code",
        "publish_last_error_message",
        "publish_manual_ack_at",
        "publish_manual_completed_at",
        "publish_manual_published_at",
        "publish_manual_video_id",
        "publish_manual_url",
        "publish_drift_detected_at",
        "publish_observed_visibility",
    }
)
ENRICHMENT_IMMUTABLE_FIELDS = frozenset(
    {
        "publish_state",
        "publish_target_visibility",
        "publish_delivery_mode_effective",
        "publish_resolved_scope",
        "publish_scheduled_at",
        "publish_attempt_count",
        "publish_retry_at",
        "publish_in_progress_at",
        "publish_last_transition_at",
        "publish_hold_active",
        "publish_hold_reason_code",
    }
)

_ALLOWED_TRANSITIONS: dict[TransitionActorClass, dict[str, frozenset[str]]] = {
    "system_automatic": {
        "private_uploaded": frozenset(),
        "policy_blocked": frozenset(),
        "waiting_for_schedule": frozenset({"ready_to_publish"}),
        "ready_to_publish": frozenset({"publish_in_progress"}),
        "publish_in_progress": frozenset(
            {
                "published_public",
                "published_unlisted",
                "retry_pending",
                "manual_handoff_pending",
                "publish_failed_terminal",
            }
        ),
        "retry_pending": frozenset({"ready_to_publish"}),
        "manual_handoff_pending": frozenset(),
        "manual_handoff_acknowledged": frozenset(),
        "manual_publish_completed": frozenset(),
        "published_public": frozenset(),
        "published_unlisted": frozenset(),
        "publish_failed_terminal": frozenset(),
        "publish_state_drift_detected": frozenset(),
    },
    "system_internal": {
        "private_uploaded": frozenset({"policy_blocked", "waiting_for_schedule", "ready_to_publish", "publish_state_drift_detected"}),
        "policy_blocked": frozenset({"waiting_for_schedule", "ready_to_publish", "manual_handoff_pending", "publish_state_drift_detected"}),
        "waiting_for_schedule": frozenset({"policy_blocked", "publish_state_drift_detected"}),
        "ready_to_publish": frozenset({"policy_blocked", "waiting_for_schedule", "publish_state_drift_detected"}),
        "publish_in_progress": frozenset({"publish_state_drift_detected"}),
        "retry_pending": frozenset({"policy_blocked", "manual_handoff_pending", "publish_state_drift_detected"}),
        "manual_handoff_pending": frozenset({"publish_state_drift_detected"}),
        "manual_handoff_acknowledged": frozenset({"publish_state_drift_detected"}),
        "manual_publish_completed": frozenset(),
        "published_public": frozenset(),
        "published_unlisted": frozenset(),
        "publish_failed_terminal": frozenset(),
        "publish_state_drift_detected": frozenset({"manual_handoff_pending", "published_public", "published_unlisted"}),
    },
    "operator_manual": {
        "private_uploaded": frozenset({"manual_handoff_pending"}),
        "policy_blocked": frozenset({"waiting_for_schedule", "ready_to_publish", "manual_handoff_pending"}),
        "waiting_for_schedule": frozenset({"policy_blocked", "ready_to_publish", "manual_handoff_pending"}),
        "ready_to_publish": frozenset({"policy_blocked", "waiting_for_schedule", "manual_handoff_pending"}),
        "publish_in_progress": frozenset(),
        "retry_pending": frozenset({"policy_blocked", "waiting_for_schedule", "ready_to_publish", "manual_handoff_pending"}),
        "manual_handoff_pending": frozenset({"manual_handoff_acknowledged"}),
        "manual_handoff_acknowledged": frozenset({"manual_publish_completed", "publish_failed_terminal"}),
        "manual_publish_completed": frozenset(),
        "published_public": frozenset(),
        "published_unlisted": frozenset(),
        "publish_failed_terminal": frozenset(),
        "publish_state_drift_detected": frozenset({"manual_handoff_pending", "manual_publish_completed"}),
    },
    "enrichment_only": {state: frozenset({state}) for state in PUBLISH_STATE_VALUES},
}


class PublishTransitionError(ValueError):
    pass


@dataclass(frozen=True)
class PublishTransitionRequest:
    from_publish_state: str
    to_publish_state: str
    transition_actor_class: TransitionActorClass
    job_state: str
    changed_fields: frozenset[str] = frozenset()


def _normalize_job_state(value: Any) -> str:
    return str(value or "").strip().upper()


def validate_publish_transition(request: PublishTransitionRequest) -> None:
    from_state = validate_publish_state(request.from_publish_state)
    to_state = validate_publish_state(request.to_publish_state)
    actor = request.transition_actor_class

    if _normalize_job_state(request.job_state) == "CANCELLED" and actor != "enrichment_only":
        raise PublishTransitionError("jobs.state=CANCELLED forbids non-enrichment publish transitions")

    if actor != "enrichment_only" and from_state == to_state:
        raise PublishTransitionError("same-state transition allowed only for enrichment_only")

    if actor == "enrichment_only":
        if from_state != to_state:
            raise PublishTransitionError("enrichment_only cannot change publish_state")
        _validate_enrichment_fields(request.changed_fields)

    # Explicit invariant guard (also covered by matrix) to avoid future accidental collapse.
    drift_pair = {from_state, to_state}
    if drift_pair == {"publish_state_drift_detected", "publish_failed_terminal"}:
        raise PublishTransitionError("drift and failed-terminal states are separate and non-interchangeable")

    allowed_targets = _ALLOWED_TRANSITIONS[actor][from_state]
    if to_state not in allowed_targets:
        raise PublishTransitionError(f"forbidden publish transition: actor={actor} from={from_state} to={to_state}")


def _validate_enrichment_fields(changed_fields: Collection[str]) -> None:
    for field in changed_fields:
        if field in ENRICHMENT_IMMUTABLE_FIELDS:
            raise PublishTransitionError(f"enrichment_only cannot mutate field: {field}")
        if field not in ENRICHMENT_MUTABLE_FIELDS:
            raise PublishTransitionError(f"enrichment_only field is not allowed: {field}")


def is_publish_transition_allowed(
    *,
    from_publish_state: str,
    to_publish_state: str,
    transition_actor_class: TransitionActorClass,
    job_state: str,
    changed_fields: Collection[str] | None = None,
) -> bool:
    try:
        validate_publish_transition(
            PublishTransitionRequest(
                from_publish_state=from_publish_state,
                to_publish_state=to_publish_state,
                transition_actor_class=transition_actor_class,
                job_state=job_state,
                changed_fields=frozenset(changed_fields or ()),
            )
        )
    except (ValueError, PublishTransitionError):
        return False
    return True


__all__ = [
    "TransitionActorClass",
    "TERMINAL_SUCCESS_STATES",
    "ENRICHMENT_MUTABLE_FIELDS",
    "ENRICHMENT_IMMUTABLE_FIELDS",
    "PublishTransitionError",
    "PublishTransitionRequest",
    "is_publish_transition_allowed",
    "validate_publish_transition",
]
