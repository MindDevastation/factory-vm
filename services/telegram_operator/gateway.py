from __future__ import annotations

import json
from typing import Any, Callable

from .core import TelegramOperatorRegistry
from .envelope import is_envelope_expired
from .error_mapper import to_telegram_safe_error
from .errors import (
    E6A_ACTION_EXPIRED,
    E6A_CHAT_BINDING_DISABLED,
    E6A_CHAT_BINDING_MISSING,
    E6A_CHAT_BINDING_REVOKED,
    E6A_GATEWAY_CONTEXT_INVALID,
    E6A_OPERATOR_INACTIVE,
    E6A_OPERATOR_REVOKED,
    E6A_PERMISSION_DENIED,
    E6A_TELEGRAM_IDENTITY_UNBOUND,
)
from .literals import GATEWAY_RESULTS, ensure_gateway_result
from .permissions import permission_allows
from .audit import emit_audit_event


TargetResolver = Callable[[dict[str, Any]], dict[str, Any]]
StaleHook = Callable[[dict[str, Any]], dict[str, Any]]
IdempotencyHook = Callable[[dict[str, Any]], dict[str, Any]]


def build_gateway_decision(
    *,
    gateway_result: str,
    resolved_operator_status: str | None,
    resolved_binding_status: str | None,
    permission_result: str,
    action_class: str,
    target_resolution_result: str,
    stale_precheck_hook_ref: str,
    idempotency_hook_ref: str,
    error_code: str | None = None,
) -> dict[str, Any]:
    result = ensure_gateway_result(gateway_result)
    return {
        "gateway_result": result,
        "resolved_operator_status": resolved_operator_status,
        "resolved_binding_status": resolved_binding_status,
        "permission_result": permission_result,
        "action_class": action_class,
        "target_resolution_result": target_resolution_result,
        "stale_precheck_hook_ref": stale_precheck_hook_ref,
        "idempotency_hook_ref": idempotency_hook_ref,
        "error": to_telegram_safe_error(error_code) if error_code else None,
    }


class TelegramActionGateway:
    def __init__(self, conn: Any):
        self._conn = conn
        self._registry = TelegramOperatorRegistry(conn)

    def evaluate(
        self,
        envelope: dict[str, Any],
        *,
        target_resolver: TargetResolver,
        stale_precheck_hook: StaleHook,
        idempotency_hook: IdempotencyHook,
    ) -> dict[str, Any]:
        identity = self._registry.get_identity(telegram_user_id=int(envelope["telegram_user_id"]))
        if identity is None:
            return self._deny(envelope, error_code=E6A_TELEGRAM_IDENTITY_UNBOUND, operator_status=None, binding_status=None)

        operator_status = str(identity["telegram_access_status"])
        if operator_status == "INACTIVE":
            return self._deny(envelope, error_code=E6A_OPERATOR_INACTIVE, operator_status=operator_status, binding_status=None)
        if operator_status == "REVOKED":
            return self._deny(envelope, error_code=E6A_OPERATOR_REVOKED, operator_status=operator_status, binding_status=None)

        binding = self._registry.read_binding(
            product_operator_id=str(identity["product_operator_id"]),
            telegram_user_id=int(envelope["telegram_user_id"]),
            chat_id=int(envelope["chat_id"]),
            thread_id=envelope.get("thread_id"),
        )
        if binding is None:
            return self._deny(envelope, error_code=E6A_CHAT_BINDING_MISSING, operator_status=operator_status, binding_status=None)
        binding_status = str(binding["binding_status"])
        if binding_status == "DISABLED":
            return self._deny(envelope, error_code=E6A_CHAT_BINDING_DISABLED, operator_status=operator_status, binding_status=binding_status)
        if binding_status == "REVOKED":
            return self._deny(envelope, error_code=E6A_CHAT_BINDING_REVOKED, operator_status=operator_status, binding_status=binding_status)
        if binding_status != "ACTIVE":
            return self._deny(envelope, error_code=E6A_GATEWAY_CONTEXT_INVALID, operator_status=operator_status, binding_status=binding_status)

        if is_envelope_expired(envelope):
            return self._decision(
                envelope,
                build_gateway_decision(
                    gateway_result="EXPIRED",
                    resolved_operator_status=operator_status,
                    resolved_binding_status=binding_status,
                    permission_result="DENIED",
                    action_class=str(envelope["action_class"]),
                    target_resolution_result="SKIPPED",
                    stale_precheck_hook_ref=getattr(stale_precheck_hook, "__name__", "stale_hook"),
                    idempotency_hook_ref=getattr(idempotency_hook, "__name__", "idempotency_hook"),
                    error_code=E6A_ACTION_EXPIRED,
                ),
                binding_id=int(binding["id"]),
            )

        if not permission_allows(granted=str(identity["max_permission_class"]), requested=str(envelope["action_class"])):
            return self._deny(envelope, error_code=E6A_PERMISSION_DENIED, operator_status=operator_status, binding_status=binding_status, binding_id=int(binding["id"]))

        target_resolution = target_resolver(envelope)
        if target_resolution.get("result") not in {"FOUND", "OK"}:
            return self._deny(envelope, error_code=E6A_GATEWAY_CONTEXT_INVALID, operator_status=operator_status, binding_status=binding_status, binding_id=int(binding["id"]))

        stale_state = stale_precheck_hook(envelope)
        if stale_state.get("result") == "STALE":
            return self._decision(
                envelope,
                build_gateway_decision(
                    gateway_result="STALE",
                    resolved_operator_status=operator_status,
                    resolved_binding_status=binding_status,
                    permission_result="ALLOWED",
                    action_class=str(envelope["action_class"]),
                    target_resolution_result=str(target_resolution.get("result")),
                    stale_precheck_hook_ref=getattr(stale_precheck_hook, "__name__", "stale_hook"),
                    idempotency_hook_ref=getattr(idempotency_hook, "__name__", "idempotency_hook"),
                    error_code="E6A_TARGET_STALE",
                ),
                binding_id=int(binding["id"]),
            )

        idempotency_state = idempotency_hook(envelope)
        if idempotency_state.get("result") == "CONFLICT":
            return self._deny(envelope, error_code="E6A_IDEMPOTENCY_CONFLICT", operator_status=operator_status, binding_status=binding_status, binding_id=int(binding["id"]))

        decision = build_gateway_decision(
            gateway_result="ALLOWED",
            resolved_operator_status=operator_status,
            resolved_binding_status=binding_status,
            permission_result="ALLOWED",
            action_class=str(envelope["action_class"]),
            target_resolution_result=str(target_resolution.get("result")),
            stale_precheck_hook_ref=getattr(stale_precheck_hook, "__name__", "stale_hook"),
            idempotency_hook_ref=getattr(idempotency_hook, "__name__", "idempotency_hook"),
            error_code=None,
        )
        return self._decision(envelope, decision, binding_id=int(binding["id"]), product_operator_id=str(identity["product_operator_id"]))

    def _deny(
        self,
        envelope: dict[str, Any],
        *,
        error_code: str,
        operator_status: str | None,
        binding_status: str | None,
        binding_id: int | None = None,
    ) -> dict[str, Any]:
        if error_code == E6A_ACTION_EXPIRED:
            result = "EXPIRED"
        else:
            result = "DENIED"
        decision = build_gateway_decision(
            gateway_result=result,
            resolved_operator_status=operator_status,
            resolved_binding_status=binding_status,
            permission_result="DENIED",
            action_class=str(envelope["action_class"]),
            target_resolution_result="SKIPPED",
            stale_precheck_hook_ref="stale_precheck_hook",
            idempotency_hook_ref="idempotency_hook",
            error_code=error_code,
        )
        return self._decision(envelope, decision, binding_id=binding_id)

    def _decision(
        self,
        envelope: dict[str, Any],
        decision: dict[str, Any],
        *,
        binding_id: int | None,
        product_operator_id: str | None = None,
    ) -> dict[str, Any]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                INSERT INTO telegram_action_gateway_events(
                    telegram_user_id, product_operator_id, binding_id, action_transport_type,
                    action_transport_id, action_type, action_class, target_entity_type, target_entity_ref,
                    gateway_result, gateway_error_code, correlation_id, idempotency_key,
                    freshness_context, event_time, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(envelope["telegram_user_id"]),
                    product_operator_id,
                    binding_id,
                    str(envelope["action_transport_type"]),
                    str(envelope["action_transport_id"]),
                    str(envelope["action_type"]),
                    str(envelope["action_class"]),
                    str(envelope["target_entity_type"]),
                    str(envelope["target_entity_ref"]),
                    str(decision["gateway_result"]),
                    decision["error"]["code"] if decision.get("error") else None,
                    str(envelope["correlation_id"]),
                    envelope.get("idempotency_key"),
                    json.dumps(envelope.get("freshness_context") or {}, ensure_ascii=False, sort_keys=True),
                    str(envelope["created_at"]),
                    str(envelope["created_at"]),
                ),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

        out = dict(decision)
        out["allow"] = out["gateway_result"] == "ALLOWED"
        out["binding_id"] = binding_id
        out["product_operator_id"] = product_operator_id
        emit_audit_event(
            self._conn,
            event_type=(
                "TELEGRAM_ACTION_EXPIRED" if out["gateway_result"] == "EXPIRED"
                else "TELEGRAM_ACTION_STALE" if out["gateway_result"] == "STALE"
                else "TELEGRAM_GATEWAY_DENIED" if out["gateway_result"] in {"DENIED", "INVALID"}
                else "TELEGRAM_GATEWAY_EVALUATED"
            ),
            telegram_user_id=int(envelope["telegram_user_id"]),
            resolved_product_operator_id=product_operator_id,
            chat_id=int(envelope["chat_id"]),
            thread_id=envelope.get("thread_id"),
            binding_id=binding_id,
            action_type=str(envelope["action_type"]),
            action_class=str(envelope["action_class"]),
            target_entity_type=str(envelope["target_entity_type"]),
            target_entity_ref=str(envelope["target_entity_ref"]),
            gateway_result=str(out["gateway_result"]),
            gateway_error_code=(out["error"]["code"] if out.get("error") else None),
            correlation_id=str(envelope["correlation_id"]),
            idempotency_key=envelope.get("idempotency_key"),
            payload={"permission_result": out["permission_result"]},
        )
        return out


def downstream_mutation_stub(*, gateway_result: dict[str, Any]) -> dict[str, Any]:
    if not bool(gateway_result.get("allow")):
        return {"executed": False, "reason": gateway_result.get("error")}
    return {"executed": True, "reason": None}
