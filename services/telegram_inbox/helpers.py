from __future__ import annotations

import hashlib
import json
from typing import Any

from .literals import (
    ensure_actionability_class,
    ensure_category,
    ensure_delivery_behavior,
    ensure_message_family,
    ensure_severity,
)


def classify_quiet_noisy(*, message_family: str, severity: str) -> str:
    family = ensure_message_family(message_family)
    sev = ensure_severity(severity)
    if sev == "CRITICAL" or family == "CRITICAL_ALERT":
        return "IMMEDIATE"
    if family == "SUMMARY_DIGEST":
        return "DIGEST"
    if family == "UNRESOLVED_FOLLOW_UP":
        return "FOLLOW_UP_ONLY"
    return "IMMEDIATE" if sev in {"HIGH", "MEDIUM"} else "DIGEST"


def build_notification_dedupe_key(
    *,
    message_family: str,
    category: str,
    target_entity_type: str,
    target_entity_ref: str,
    upstream_event_family: str,
    upstream_event_ref: str | None,
) -> str:
    payload = {
        "message_family": ensure_message_family(message_family),
        "category": ensure_category(category),
        "target_entity_type": str(target_entity_type),
        "target_entity_ref": str(target_entity_ref),
        "upstream_event_family": str(upstream_event_family),
        "upstream_event_ref": str(upstream_event_ref or ""),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_target_context_summary(*, target_entity_type: str, target_entity_ref: str, attributes: dict[str, Any] | None = None) -> dict[str, Any]:
    attrs = dict(attributes or {})
    compact = {str(k): attrs[k] for k in sorted(attrs) if attrs[k] is not None}
    return {
        "entity_type": str(target_entity_type),
        "entity_ref": str(target_entity_ref),
        "attributes": compact,
        "summary": f"{target_entity_type}:{target_entity_ref}",
    }


def validate_classification(
    *,
    category: str,
    severity: str,
    message_family: str,
    actionability_class: str,
    delivery_behavior: str,
) -> dict[str, str]:
    return {
        "category": ensure_category(category),
        "severity": ensure_severity(severity),
        "message_family": ensure_message_family(message_family),
        "actionability_class": ensure_actionability_class(actionability_class),
        "delivery_behavior": ensure_delivery_behavior(delivery_behavior),
    }
