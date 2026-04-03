from __future__ import annotations

import hashlib
import json
from typing import Any

from services.telegram_operator.persistence import (
    persist_action_audit_record,
    persist_action_safety_event,
    persist_ops_action_confirmation,
    persist_ops_action_context,
    persist_ops_action_result,
)


_OPS_POLICY: dict[str, dict[str, Any]] = {
    "retry": {"canonical_action": "retry", "scope": "single", "confirm_required": True, "enabled": True},
    "acknowledge": {"canonical_action": "acknowledge", "scope": "single", "confirm_required": False, "enabled": True},
    "unblock": {"canonical_action": "unblock", "scope": "single", "confirm_required": True, "enabled": True},
    "reschedule": {"canonical_action": "reschedule", "scope": "single", "confirm_required": True, "enabled": True},
    "dangerous_reset_db": {"canonical_action": None, "scope": "web_only", "confirm_required": True, "enabled": False},
}


def ops_action_policy(action: str) -> dict[str, Any]:
    if action not in _OPS_POLICY:
        raise ValueError("unsupported ops action")
    return dict(_OPS_POLICY[action])


def build_confirmation_envelope(*, action: str, confirm: bool, reason: str, request_id: str) -> dict[str, Any]:
    policy = ops_action_policy(action)
    if not policy["enabled"]:
        raise ValueError("action is out of scope")
    if policy["confirm_required"] and not confirm:
        raise ValueError("confirmation required")
    if not str(reason or "").strip():
        raise ValueError("reason required")
    if not str(request_id or "").strip():
        raise ValueError("request_id required")
    return {"action": action, "confirm": bool(confirm), "reason": reason.strip(), "request_id": request_id.strip()}

from fastapi.responses import JSONResponse
from services.factory_api.publish_job_actions import execute_publish_job_action


def execute_single_ops_action(
    conn: Any,
    *,
    job_id: int,
    action: str,
    actor: str,
    confirm: bool,
    reason: str,
    request_id: str,
    extra_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    envelope = build_confirmation_envelope(action=action, confirm=confirm, reason=reason, request_id=request_id)
    policy = ops_action_policy(action)
    action_ref = f"ops:{action}:{job_id}:{request_id}"
    actor_ref = str(actor)
    operator_id = actor_ref.split(":", 1)[1] if ":" in actor_ref else actor_ref
    persist_ops_action_context(
        conn,
        action_ref=action_ref,
        action_type=action,
        product_operator_id=operator_id,
        telegram_user_id=int(operator_id) if operator_id.isdigit() else 0,
        target_entity_type="publish_job",
        target_entity_ref=str(int(job_id)),
        context={"request_id": request_id, "reason": reason},
    )
    token = hashlib.sha256(json.dumps(envelope, sort_keys=True).encode("utf-8")).hexdigest()
    persist_ops_action_confirmation(
        conn,
        action_ref=action_ref,
        confirmation_token=token,
        confirmation_status="CONFIRMED" if confirm else "PENDING",
    )
    out = execute_publish_job_action(
        conn,
        job_id=int(job_id),
        action_type=str(policy["canonical_action"]),
        actor=str(actor),
        request_id=str(envelope["request_id"]),
        reason=str(envelope["reason"]),
        extra_payload=extra_payload,
    )
    if isinstance(out, JSONResponse):
        result = {"status": "FAILED", "action": action, "job_id": int(job_id), "changed": None, "error": "E3_ACTION_NOT_ALLOWED"}
        persist_ops_action_result(conn, action_ref=action_ref, result_status="FAILED", error_code="E3_ACTION_NOT_ALLOWED", result_payload=result)
        persist_action_safety_event(
            conn,
            safety_event_type="OPS_ACTION_FAILED",
            action_ref=action_ref,
            request_id=request_id,
            reason_code="E3_ACTION_NOT_ALLOWED",
            details=result,
        )
        return result
    result = out.get("result", {}) if isinstance(out, dict) else {}
    response = {
        "status": "SUCCESS",
        "action": action,
        "job_id": int(job_id),
        "changed": result.get("publish_state_after"),
        "error": None,
    }
    persist_ops_action_result(conn, action_ref=action_ref, result_status="OK", error_code=None, result_payload=response)
    persist_action_audit_record(
        conn,
        record_type="OPS_ACTION_APPLIED",
        action_ref=action_ref,
        request_id=request_id,
        correlation_id=None,
        actor_ref=actor_ref,
        payload=response,
    )
    return response


def resolve_bounded_targets(*, selected_job_ids: list[int], max_targets: int = 20) -> list[int]:
    ids = [int(i) for i in selected_job_ids]
    if len(ids) == 0:
        raise ValueError("selected_job_ids required")
    unique = sorted(set(ids))
    if len(unique) > max_targets:
        raise ValueError("target set too large")
    return unique


def build_batch_preview(*, action: str, selected_job_ids: list[int]) -> dict[str, Any]:
    targets = resolve_bounded_targets(selected_job_ids=selected_job_ids)
    return {"action": action, "target_count": len(targets), "targets": targets, "requires_confirmation": True}


def execute_batch_ops_action(
    conn: Any,
    *,
    action: str,
    selected_job_ids: list[int],
    actor: str,
    confirm: bool,
    reason: str,
    request_id: str,
) -> dict[str, Any]:
    preview = build_batch_preview(action=action, selected_job_ids=selected_job_ids)
    if not confirm:
        raise ValueError("confirmation required")
    items: list[dict[str, Any]] = []
    for idx, job_id in enumerate(preview["targets"]):
        item = execute_single_ops_action(
            conn,
            job_id=job_id,
            action=action,
            actor=actor,
            confirm=True,
            reason=reason,
            request_id=f"{request_id}:{idx}",
        )
        items.append(item)
    return {
        "action": action,
        "preview": preview,
        "summary": {
            "executed_count": len(items),
            "succeeded_count": sum(1 for i in items if i["status"] == "SUCCESS"),
            "failed_count": sum(1 for i in items if i["status"] != "SUCCESS"),
        },
        "items": items,
    }
