from __future__ import annotations

import json
import sqlite3
import hashlib
import re
from datetime import datetime, timezone
from typing import Any

from services.prompt_registry.contracts import (
    EXPORT_SCHEMA_VERSION,
    ensure_binding_scope,
    ensure_binding_status,
    ensure_import_mode,
    ensure_lifecycle_transition,
    ensure_linked_action_status,
    ensure_linked_action_target_kind,
    ensure_linked_action_type,
    ensure_non_empty,
    ensure_record_status,
    ensure_record_type,
    ensure_safety_class,
    ensure_usage_event_status,
    ensure_usage_event_type,
    ensure_validation_status,
)
from services.prompt_registry.errors import (
    PromptRegistryConflictError,
    PromptRegistryNotFoundError,
    PromptRegistryValidationError,
)


class PromptRegistryService:
    _REDACTED_EXPORT_DEFAULT = ""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _write_audit_event(
        self,
        *,
        prompt_id: int,
        event_type: str,
        actor: str,
        version_id: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO prompt_audit_events(prompt_id,version_id,event_type,actor,payload_json,created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (
                prompt_id,
                version_id,
                event_type,
                actor,
                json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                self._now_iso(),
            ),
        )

    @staticmethod
    def _safe_variables_schema(version_variables: list[dict[str, Any]]) -> list[dict[str, Any]]:
        safe_items: list[dict[str, Any]] = []
        for variable in version_variables:
            safe_items.append(
                {
                    "name": str(variable.get("name") or ""),
                    "safety_class": str(variable.get("safety_class") or ""),
                    "required": bool(int(variable.get("required") or 0)),
                    "has_default": bool(str(variable.get("default_value") or "")),
                }
            )
        return safe_items

    def _write_usage_event(
        self,
        *,
        event_type: str,
        status: str,
        prompt_id: int | None = None,
        version_id: int | None = None,
        binding_id: int | None = None,
        render_fingerprint: str | None = None,
        context: dict[str, Any] | None = None,
        variables_schema: list[dict[str, Any]] | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        normalized_event_type = ensure_usage_event_type(event_type)
        normalized_status = ensure_usage_event_status(status)
        self._conn.execute(
            """
            INSERT INTO prompt_usage_events(
                prompt_id,version_id,binding_id,event_type,source,status,render_fingerprint,
                context_json,variables_schema_json,diagnostics_json,created_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                prompt_id,
                version_id,
                binding_id,
                normalized_event_type,
                "api",
                normalized_status,
                self._nullable_text(render_fingerprint),
                json.dumps(context or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                json.dumps(variables_schema or [], ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                json.dumps(diagnostics or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                self._now_iso(),
            ),
        )

    @staticmethod
    def _nullable_text(value: Any) -> str | None:
        text = str(value or "").strip()
        return text or None

    def _validate_binding_target(self, payload: dict[str, Any]) -> dict[str, Any]:
        scope = ensure_binding_scope(payload.get("binding_scope"))
        workflow_slug = self._nullable_text(payload.get("workflow_slug"))
        channel_slug = self._nullable_text(payload.get("channel_slug"))
        item_type = self._nullable_text(payload.get("item_type"))
        item_ref = self._nullable_text(payload.get("item_ref"))

        if scope == "global":
            if any((workflow_slug, channel_slug, item_type, item_ref)):
                raise PromptRegistryValidationError("global binding_scope cannot include workflow/channel/item target fields")
        elif scope == "workflow":
            if not workflow_slug:
                raise PromptRegistryValidationError("workflow binding_scope requires workflow_slug")
            if any((channel_slug, item_type, item_ref)):
                raise PromptRegistryValidationError("workflow binding_scope cannot include channel/item target fields")
        elif scope == "channel":
            if not channel_slug:
                raise PromptRegistryValidationError("channel binding_scope requires channel_slug")
            if any((workflow_slug, item_type, item_ref)):
                raise PromptRegistryValidationError("channel binding_scope cannot include workflow/item target fields")
        else:
            if not item_type or not item_ref:
                raise PromptRegistryValidationError("item binding_scope requires item_type and item_ref")
            if any((workflow_slug, channel_slug)):
                raise PromptRegistryValidationError("item binding_scope cannot include workflow/channel target fields")

        return {
            "binding_scope": scope,
            "workflow_slug": workflow_slug,
            "channel_slug": channel_slug,
            "item_type": item_type,
            "item_ref": item_ref,
        }

    def list_records(self) -> list[dict[str, Any]]:
        return list(self._conn.execute("SELECT * FROM prompt_records ORDER BY updated_at DESC, id DESC").fetchall())

    def list_bindings(
        self,
        *,
        prompt_id: int | None = None,
        binding_scope: str | None = None,
        binding_status: str | None = None,
    ) -> list[dict[str, Any]]:
        where_parts: list[str] = []
        params: list[Any] = []
        if prompt_id is not None:
            where_parts.append("prompt_id = ?")
            params.append(int(prompt_id))
        if binding_scope is not None:
            where_parts.append("binding_scope = ?")
            params.append(ensure_binding_scope(binding_scope))
        if binding_status is not None:
            where_parts.append("binding_status = ?")
            params.append(ensure_binding_status(binding_status))
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        query = f"SELECT * FROM prompt_bindings{where_clause} ORDER BY updated_at DESC, id DESC"
        return list(self._conn.execute(query, tuple(params)).fetchall())

    def get_record(self, prompt_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM prompt_records WHERE id = ?", (prompt_id,)).fetchone()
        if row is None:
            raise PromptRegistryNotFoundError(f"prompt record {prompt_id} not found")
        return row

    def get_binding(self, binding_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM prompt_bindings WHERE id = ?", (binding_id,)).fetchone()
        if row is None:
            raise PromptRegistryNotFoundError(f"prompt binding {binding_id} not found")
        return row

    @staticmethod
    def _validate_linked_action_config(config_payload: Any) -> dict[str, Any]:
        if not isinstance(config_payload, dict):
            raise PromptRegistryValidationError("config_json must be an object")
        blocked_tokens = ("secret", "token", "password", "api_key", "authorization")

        def _validate_nested(value: Any) -> None:
            if isinstance(value, dict):
                for key, nested in value.items():
                    lowered = str(key).strip().lower()
                    if any(token in lowered for token in blocked_tokens):
                        raise PromptRegistryValidationError("config_json must not include secret/token/password-like keys")
                    _validate_nested(nested)
            elif isinstance(value, list):
                for nested in value:
                    _validate_nested(nested)

        _validate_nested(config_payload)
        return config_payload

    @staticmethod
    def _validate_linked_action_request_context(context_payload: Any) -> dict[str, Any]:
        if not isinstance(context_payload, dict):
            raise PromptRegistryValidationError("request_context_json must be an object")
        blocked_tokens = ("secret", "token", "password", "api_key", "authorization")

        def _validate_nested(value: Any) -> None:
            if isinstance(value, dict):
                for key, nested in value.items():
                    lowered = str(key).strip().lower()
                    if any(token in lowered for token in blocked_tokens):
                        raise PromptRegistryValidationError("request_context_json must not include secret/token/password-like keys")
                    _validate_nested(nested)
            elif isinstance(value, list):
                for nested in value:
                    _validate_nested(nested)

        _validate_nested(context_payload)
        return context_payload

    def get_linked_action(self, action_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM prompt_linked_actions WHERE id = ?", (action_id,)).fetchone()
        if row is None:
            raise PromptRegistryNotFoundError(f"prompt linked action {action_id} not found")
        item = dict(row)
        raw_config = item.get("config_json")
        if isinstance(raw_config, str):
            try:
                parsed = json.loads(raw_config)
            except json.JSONDecodeError:
                parsed = {}
            item["config"] = parsed if isinstance(parsed, dict) else {}
        else:
            item["config"] = {}
        return item

    @staticmethod
    def _linked_action_expected_target_kind(action_type: str) -> str | None:
        expected_by_type = {
            "ui_action": "route",
            "api_endpoint": "endpoint",
            "workflow": "workflow",
            "codex_prompt": "prompt_template",
            "external_note": "note",
        }
        return expected_by_type.get(action_type)

    @staticmethod
    def _safe_linked_action_config_summary(config: Any, *, max_items: int = 4) -> str:
        if not isinstance(config, dict) or not config:
            return "—"
        masked_tokens = ("secret", "token", "password", "api_key", "authorization")
        items: list[str] = []
        for key in sorted(config.keys()):
            if len(items) >= max_items:
                break
            key_text = str(key)
            lowered = key_text.lower()
            raw = config.get(key)
            if any(token in lowered for token in masked_tokens):
                rendered = "***MASKED***"
            elif isinstance(raw, dict):
                rendered = f"{{{len(raw)} keys}}"
            elif isinstance(raw, list):
                rendered = f"[{len(raw)} items]"
            elif raw is None:
                rendered = "null"
            else:
                rendered = str(raw).strip()
                if len(rendered) > 48:
                    rendered = f"{rendered[:45]}..."
            items.append(f"{key_text}={rendered}")
        if len(config.keys()) > len(items):
            items.append(f"... (+{len(config.keys()) - len(items)} more)")
        return "; ".join(items) if items else "—"

    @staticmethod
    def _safe_compact_summary(value: Any, *, max_items: int = 6) -> str:
        if not isinstance(value, dict) or not value:
            return "—"
        masked_tokens = ("secret", "operator_only", "token", "password", "api_key", "authorization")
        items: list[str] = []
        for key in sorted(value.keys()):
            if len(items) >= max_items:
                break
            key_text = str(key)
            lowered = key_text.lower()
            raw = value.get(key)
            if any(token in lowered for token in masked_tokens):
                rendered = "***MASKED***"
            elif isinstance(raw, dict):
                rendered = f"{{{len(raw)} keys}}"
            elif isinstance(raw, list):
                rendered = f"[{len(raw)} items]"
            elif raw is None:
                rendered = "null"
            else:
                rendered = str(raw).strip()
                if len(rendered) > 48:
                    rendered = f"{rendered[:45]}..."
            items.append(f"{key_text}={rendered}")
        if len(value.keys()) > len(items):
            items.append(f"... (+{len(value.keys()) - len(items)} more)")
        return "; ".join(items) if items else "—"

    def preview_linked_action(self, action_id: int) -> dict[str, Any]:
        action = self.get_linked_action(action_id)
        action_type = str(action["action_type"])
        action_status = str(action["action_status"])
        target_kind = str(action["target_kind"])
        target_ref = self._nullable_text(action.get("target_ref"))
        config_summary = self._safe_linked_action_config_summary(action.get("config"))
        diagnostics: list[dict[str, str]] = []
        preview_status = "OK"
        has_blocking = False

        def _add_diagnostic(code: str, severity: str, message: str) -> None:
            nonlocal preview_status, has_blocking
            diagnostics.append({"code": code, "severity": severity, "message": message})
            if severity == "BLOCKING":
                preview_status = "INVALID"
                has_blocking = True
            elif severity == "WARNING" and preview_status == "OK":
                preview_status = "WARNING"

        expected_target_kind = self._linked_action_expected_target_kind(action_type)
        if expected_target_kind and expected_target_kind != target_kind:
            _add_diagnostic(
                "LINKED_ACTION_TARGET_KIND_MISMATCH",
                "WARNING",
                f"{action_type} should target {expected_target_kind}, got {target_kind}",
            )

        required_ref_kinds = {"route", "endpoint", "workflow", "prompt_template"}
        if target_kind in required_ref_kinds and not target_ref:
            _add_diagnostic(
                "LINKED_ACTION_TARGET_REF_REQUIRED",
                "BLOCKING",
                f"target_ref is required for target_kind={target_kind}",
            )
        elif target_kind == "note" and not target_ref:
            _add_diagnostic(
                "LINKED_ACTION_NOTE_TARGET_REF_OPTIONAL",
                "INFO",
                "target_ref is optional for note target_kind",
            )

        if action_status != "active":
            _add_diagnostic(
                "LINKED_ACTION_INACTIVE",
                "WARNING",
                "linked action is inactive and cannot be executed later until reactivated",
            )

        if not diagnostics:
            _add_diagnostic("LINKED_ACTION_PREVIEW_OK", "INFO", "linked action preview checks passed")

        normalized_display_ref = target_ref or "—"
        normalized_target = {
            "kind": target_kind,
            "ref": target_ref,
            "display_label": f"{target_kind}:{normalized_display_ref}",
        }
        can_execute_later = action_status == "active" and not has_blocking
        return {
            "action_id": int(action["id"]),
            "prompt_id": int(action["prompt_id"]),
            "action_key": str(action["action_key"]),
            "action_type": action_type,
            "action_status": action_status,
            "target_kind": target_kind,
            "target_ref": target_ref,
            "preview_status": preview_status,
            "can_execute_later": bool(can_execute_later),
            "diagnostics": diagnostics,
            "normalized_target": normalized_target,
            "config_summary": config_summary,
        }

    def list_linked_actions(self, prompt_id: int, *, include_inactive: bool = True) -> list[dict[str, Any]]:
        self.get_record(prompt_id)
        if include_inactive:
            rows = list(
                self._conn.execute(
                    "SELECT * FROM prompt_linked_actions WHERE prompt_id = ? ORDER BY updated_at DESC, id DESC",
                    (prompt_id,),
                ).fetchall()
            )
        else:
            rows = list(
                self._conn.execute(
                    "SELECT * FROM prompt_linked_actions WHERE prompt_id = ? AND action_status = 'active' ORDER BY updated_at DESC, id DESC",
                    (prompt_id,),
                ).fetchall()
            )
        return [self.get_linked_action(int(row["id"])) for row in rows]

    def create_linked_action_execution_request(self, action_id: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        preview = self.preview_linked_action(action_id)
        prompt_id = int(preview["prompt_id"])
        self.get_record(prompt_id)
        confirm_execution = bool(payload.get("confirm_execution", False))
        request_context = self._validate_linked_action_request_context(payload.get("request_context_json", {}))
        preview_status = str(preview.get("preview_status") or "INVALID")
        can_execute_later = bool(preview.get("can_execute_later"))
        diagnostics = preview.get("diagnostics") if isinstance(preview.get("diagnostics"), list) else []

        if preview_status == "INVALID" or not can_execute_later:
            request_status = "blocked"
        elif confirm_execution:
            request_status = "accepted"
        else:
            request_status = "preview_only"

        now = self._now_iso()
        cur = self._conn.execute(
            """
            INSERT INTO prompt_linked_action_execution_requests(
                action_id,prompt_id,request_status,requested_by,preview_status,can_execute_later,
                diagnostics_json,request_context_json,created_at,updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(action_id),
                prompt_id,
                request_status,
                actor_id,
                preview_status,
                1 if can_execute_later else 0,
                json.dumps(diagnostics, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                json.dumps(request_context, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                now,
                now,
            ),
        )
        request_id = int(cur.lastrowid)
        self._write_audit_event(
            prompt_id=prompt_id,
            event_type="linked_action_execution_requested",
            actor=actor_id,
            payload={
                "linked_action_id": int(action_id),
                "execution_request_id": request_id,
                "request_status": request_status,
                "preview_status": preview_status,
                "can_execute_later": can_execute_later,
                "confirm_execution": confirm_execution,
            },
        )
        return self.list_linked_action_execution_requests(action_id=int(action_id), limit=1)[0]

    def list_linked_action_execution_requests(
        self,
        prompt_id: int | None = None,
        action_id: int | None = None,
        request_status: str | None = None,
        preview_status: str | None = None,
        requested_by: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        parsed_limit = int(limit)
        if parsed_limit <= 0 or parsed_limit > 200:
            raise PromptRegistryValidationError("limit must be between 1 and 200")
        normalized_request_status: str | None = None
        normalized_preview_status: str | None = None
        normalized_requested_by: str | None = None
        if request_status is not None:
            normalized_request_status = str(request_status).strip()
            if normalized_request_status not in {"preview_only", "blocked", "accepted"}:
                raise PromptRegistryValidationError("request_status must be one of preview_only, blocked, accepted")
        if preview_status is not None:
            normalized_preview_status = str(preview_status).strip().upper()
            if normalized_preview_status not in {"OK", "WARNING", "INVALID"}:
                raise PromptRegistryValidationError("preview_status must be one of OK, WARNING, INVALID")
        if requested_by is not None:
            normalized_requested_by = ensure_non_empty(requested_by, field_name="requested_by")
        where_parts: list[str] = []
        params: list[Any] = []
        if prompt_id is not None:
            where_parts.append("prompt_id = ?")
            params.append(int(prompt_id))
        if action_id is not None:
            where_parts.append("action_id = ?")
            params.append(int(action_id))
        if normalized_request_status is not None:
            where_parts.append("request_status = ?")
            params.append(normalized_request_status)
        if normalized_preview_status is not None:
            where_parts.append("preview_status = ?")
            params.append(normalized_preview_status)
        if normalized_requested_by is not None:
            where_parts.append("requested_by = ?")
            params.append(normalized_requested_by)
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        query = (
            "SELECT * FROM prompt_linked_action_execution_requests"
            f"{where_clause} ORDER BY created_at DESC, id DESC LIMIT ?"
        )
        rows = list(self._conn.execute(query, (*params, parsed_limit)).fetchall())
        items: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            for field, default in (("diagnostics_json", []), ("request_context_json", {})):
                raw = item.get(field)
                parsed: Any = default
                if isinstance(raw, str):
                    try:
                        decoded = json.loads(raw)
                    except json.JSONDecodeError:
                        decoded = default
                    if field == "diagnostics_json" and isinstance(decoded, list):
                        parsed = decoded
                    if field == "request_context_json" and isinstance(decoded, dict):
                        parsed = decoded
                item[field.replace("_json", "")] = parsed
            item["can_execute_later"] = bool(int(item.get("can_execute_later") or 0))
            items.append(item)
        return items

    def preview_linked_action_dispatch_plan(self, execution_request_id: int) -> dict[str, Any]:
        row = self._conn.execute(
            "SELECT * FROM prompt_linked_action_execution_requests WHERE id = ?",
            (int(execution_request_id),),
        ).fetchone()
        if row is None:
            raise PromptRegistryNotFoundError("linked action execution request not found")

        item = dict(row)
        request_status = str(item.get("request_status") or "")
        action_id = int(item.get("action_id"))
        action = self.get_linked_action(action_id)
        preview = self.preview_linked_action(action_id)
        action_type = str(action.get("action_type") or "")
        target_kind = str(action.get("target_kind") or "")
        target_ref = self._nullable_text(action.get("target_ref"))

        parsed_request_context: dict[str, Any] = {}
        raw_request_context = item.get("request_context_json")
        if isinstance(raw_request_context, str):
            try:
                decoded_context = json.loads(raw_request_context)
            except json.JSONDecodeError:
                decoded_context = {}
            if isinstance(decoded_context, dict):
                parsed_request_context = decoded_context

        reason_codes: list[str] = []
        diagnostics: list[dict[str, str]] = []
        has_blocking = False

        def _add_reason(code: str) -> None:
            if code not in reason_codes:
                reason_codes.append(code)

        def _add_diag(code: str, severity: str, message: str) -> None:
            nonlocal has_blocking
            diagnostics.append({"code": code, "severity": severity, "message": message})
            if severity == "BLOCKING":
                has_blocking = True
                _add_reason(code)
            elif severity == "WARNING":
                _add_reason(code)

        if request_status != "accepted":
            _add_diag("REQUEST_NOT_ACCEPTED", "BLOCKING", "dispatch preview is blocked until request_status=accepted")

        if str(preview.get("preview_status") or "") == "INVALID":
            _add_diag("LINKED_ACTION_PREVIEW_INVALID", "BLOCKING", "linked action preview_status is INVALID")

        if not bool(preview.get("can_execute_later")):
            _add_diag("LINKED_ACTION_CANNOT_EXECUTE_LATER", "BLOCKING", "linked action can_execute_later is false")

        dispatch_map = {
            ("ui_action", "route"): "ui_route",
            ("api_endpoint", "endpoint"): "api_endpoint",
            ("workflow", "workflow"): "workflow_ref",
            ("codex_prompt", "prompt_template"): "codex_prompt_ref",
            ("external_note", "note"): "note_ref",
        }
        dispatch_kind = dispatch_map.get((action_type, target_kind), "unknown")
        if dispatch_kind == "unknown":
            _add_diag(
                "DISPATCH_KIND_UNKNOWN",
                "WARNING",
                f"no deterministic dispatch kind mapping for action_type={action_type}, target_kind={target_kind}",
            )

        if target_kind != "note" and not target_ref:
            _add_diag(
                "TARGET_REF_REQUIRED",
                "BLOCKING",
                f"target_ref is required for dispatch when target_kind={target_kind}",
            )
        elif target_kind == "note" and not target_ref:
            _add_diag("NOTE_TARGET_REF_OPTIONAL", "INFO", "target_ref is optional for note dispatch previews")

        if not diagnostics:
            diagnostics.append(
                {
                    "code": "DISPATCH_PREVIEW_READY",
                    "severity": "INFO",
                    "message": "dispatch preview is ready and remains read-only",
                }
            )

        return {
            "execution_request_id": int(item.get("id")),
            "request_status": request_status,
            "action_id": int(action.get("id")),
            "prompt_id": int(action.get("prompt_id")),
            "action_key": str(action.get("action_key") or ""),
            "action_type": action_type,
            "target_kind": target_kind,
            "target_ref": target_ref,
            "dispatch_status": "BLOCKED" if has_blocking else "READY",
            "dispatch_kind": dispatch_kind,
            "dispatch_target": target_ref or "",
            "reason_codes": reason_codes,
            "diagnostics": diagnostics,
            "safe_context_summary": self._safe_compact_summary(parsed_request_context),
            "safe_config_summary": self._safe_linked_action_config_summary(action.get("config")),
        }

    def create_linked_action(self, prompt_id: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        self.get_record(prompt_id)
        action_key = ensure_non_empty(payload.get("action_key"), field_name="action_key")
        action_type = ensure_linked_action_type(payload.get("action_type"))
        action_status = ensure_linked_action_status(payload.get("action_status", "active"))
        target_kind = ensure_linked_action_target_kind(payload.get("target_kind"))
        target_ref = self._nullable_text(payload.get("target_ref"))
        config_payload = self._validate_linked_action_config(payload.get("config_json", {}))
        now = self._now_iso()
        try:
            cur = self._conn.execute(
                """
                INSERT INTO prompt_linked_actions(prompt_id,action_key,action_type,action_status,target_kind,target_ref,config_json,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    prompt_id,
                    action_key,
                    action_type,
                    action_status,
                    target_kind,
                    target_ref,
                    json.dumps(config_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if (
                "idx_prompt_linked_actions_unique_active_key_per_prompt" in message
                or "prompt_linked_actions.prompt_id, prompt_linked_actions.action_key" in message
            ):
                raise PromptRegistryConflictError("duplicate active action_key for this prompt is not allowed") from None
            raise PromptRegistryValidationError("linked action create failed validation") from None
        action_id = int(cur.lastrowid)
        self._write_audit_event(
            prompt_id=prompt_id,
            event_type="linked_action_created",
            actor=actor_id,
            payload={"linked_action_id": action_id, "action_key": action_key, "action_status": action_status},
        )
        return self.get_linked_action(action_id)

    def update_linked_action_status(self, action_id: int, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        current = self.get_linked_action(action_id)
        next_status = ensure_linked_action_status(payload.get("action_status"))
        if str(current["action_status"]) == next_status:
            return current
        now = self._now_iso()
        try:
            self._conn.execute(
                "UPDATE prompt_linked_actions SET action_status = ?, updated_at = ? WHERE id = ?",
                (next_status, now, action_id),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if (
                "idx_prompt_linked_actions_unique_active_key_per_prompt" in message
                or "prompt_linked_actions.prompt_id, prompt_linked_actions.action_key" in message
            ):
                raise PromptRegistryConflictError("duplicate active action_key for this prompt is not allowed") from None
            raise PromptRegistryValidationError("linked action status update failed validation") from None
        updated = self.get_linked_action(action_id)
        self._write_audit_event(
            prompt_id=int(updated["prompt_id"]),
            event_type="linked_action_status_updated",
            actor=actor_id,
            payload={
                "linked_action_id": int(updated["id"]),
                "action_key": str(updated["action_key"]),
                "from_status": str(current["action_status"]),
                "to_status": next_status,
            },
        )
        return updated

    @staticmethod
    def _validated_actor(actor: str) -> str:
        return ensure_non_empty(actor, field_name="actor")

    def _ensure_record_can_be_active(self, record: dict[str, Any], *, new_status: str) -> None:
        if new_status != "active":
            return
        active_version_id = record.get("active_version_id")
        if active_version_id is None:
            raise PromptRegistryValidationError("active record requires active_version_id")
        row = self._conn.execute(
            "SELECT id,is_active FROM prompt_versions WHERE id = ? AND prompt_id = ?",
            (int(active_version_id), int(record["id"])),
        ).fetchone()
        if row is None or int(row.get("is_active", 0)) != 1:
            raise PromptRegistryValidationError("active record requires an active version")

    def _validate_variables_payload(self, variables: Any) -> list[dict[str, Any]]:
        if variables is None:
            return []
        if not isinstance(variables, list):
            raise PromptRegistryValidationError("variables must be a list")
        seen_names: set[str] = set()
        validated: list[dict[str, Any]] = []
        for item in variables:
            if not isinstance(item, dict):
                raise PromptRegistryValidationError("variable must be an object")
            name = ensure_non_empty(item.get("name"), field_name="variable.name")
            if name in seen_names:
                raise PromptRegistryValidationError(f"duplicate variable name: {name}")
            seen_names.add(name)
            validated.append(
                {
                    "name": name,
                    "safety_class": ensure_safety_class(item.get("safety_class")),
                    "required": 1 if bool(item.get("required", True)) else 0,
                    "default_value": str(item.get("default_value", "")),
                    "description": str(item.get("description", "")),
                }
            )
        return validated

    @staticmethod
    def _build_render_fingerprint(body_text: str, variables: list[dict[str, Any]]) -> str:
        normalized_variables = [
            {
                "name": str(item["name"]),
                "safety_class": str(item["safety_class"]),
                "required": int(item["required"]),
                "default_value": str(item["default_value"]),
                "description": str(item["description"]),
            }
            for item in sorted(variables, key=lambda value: str(value["name"]))
        ]
        payload = {"body_text": body_text, "variables": normalized_variables}
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _preview_render_fingerprint(
        *,
        version_id: int,
        prompt_id: int,
        rendered_text: str,
        used_variables: dict[str, str],
        preview_status: str,
    ) -> str:
        payload = {
            "version_id": version_id,
            "prompt_id": prompt_id,
            "rendered_text": rendered_text,
            "used_variables": {key: used_variables[key] for key in sorted(used_variables)},
            "preview_status": preview_status,
        }
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @staticmethod
    def _extract_placeholders(body_text: str) -> list[str]:
        return re.findall(r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}", body_text)

    def create_record(self, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        now = self._now_iso()
        actor_id = self._validated_actor(actor)
        slug = ensure_non_empty(payload.get("slug"), field_name="slug")
        code = ensure_non_empty(payload.get("code"), field_name="code")
        title = ensure_non_empty(payload.get("title"), field_name="title")
        record_type = ensure_record_type(payload.get("record_type"))
        status = ensure_record_status(payload.get("status", "draft"))
        validation_status = ensure_validation_status(payload.get("validation_status", "UNKNOWN"))
        if status == "active":
            raise PromptRegistryValidationError("active record requires active_version_id")
        try:
            cur = self._conn.execute(
                """
                INSERT INTO prompt_records(slug,code,title,record_type,status,validation_status,bridge_policy_hook,active_version_id,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    slug,
                    code,
                    title,
                    record_type,
                    status,
                    validation_status,
                    str(payload.get("bridge_policy_hook", "")).strip() or None,
                    None,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if "prompt_records.slug" in message:
                raise PromptRegistryConflictError(f"prompt record slug {slug} already exists") from None
            if "prompt_records.code" in message:
                raise PromptRegistryConflictError(f"prompt record code {code} already exists") from None
            raise
        created = self.get_record(int(cur.lastrowid))
        self._write_audit_event(
            prompt_id=int(created["id"]),
            event_type="record_created",
            actor=actor_id,
            payload={"status": status},
        )
        return created

    def update_record(self, prompt_id: int, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        current = self.get_record(prompt_id)
        merged = dict(current)
        merged.update(payload)
        if "status" in payload:
            new_status = ensure_record_status(merged.get("status"))
            ensure_lifecycle_transition(current_status=str(current.get("status")), new_status=new_status)
            self._ensure_record_can_be_active(merged, new_status=new_status)
            merged["status"] = new_status
        if "record_type" in payload:
            merged["record_type"] = ensure_record_type(merged.get("record_type"))
        if "validation_status" in payload:
            merged["validation_status"] = ensure_validation_status(merged.get("validation_status"))
        if "title" in payload:
            merged["title"] = ensure_non_empty(merged.get("title"), field_name="title")
        if "slug" in payload:
            merged["slug"] = ensure_non_empty(merged.get("slug"), field_name="slug")
        if "code" in payload:
            merged["code"] = ensure_non_empty(merged.get("code"), field_name="code")

        allowed = ("slug", "code", "title", "record_type", "status", "validation_status", "bridge_policy_hook")
        updates = {k: merged[k] for k in allowed if k in payload}
        if not updates:
            return current
        updates["updated_at"] = self._now_iso()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        try:
            self._conn.execute(f"UPDATE prompt_records SET {set_clause} WHERE id = ?", tuple(list(updates.values()) + [prompt_id]))
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if "prompt_records.slug" in message:
                raise PromptRegistryConflictError(f"prompt record slug {merged['slug']} already exists") from None
            if "prompt_records.code" in message:
                raise PromptRegistryConflictError(f"prompt record code {merged['code']} already exists") from None
            raise
        updated = self.get_record(prompt_id)
        self._write_audit_event(
            prompt_id=prompt_id,
            event_type="record_updated",
            actor=actor_id,
            payload={"fields": sorted(updates.keys())},
        )
        return updated

    def _next_version_no(self, prompt_id: int) -> int:
        row = self._conn.execute("SELECT COALESCE(MAX(version_no), 0) AS v FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()
        return int(row["v"]) + 1

    def create_version(self, prompt_id: int, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        self.get_record(prompt_id)
        now = self._now_iso()
        body_text = ensure_non_empty(payload.get("body_text"), field_name="body_text")
        status = ensure_record_status(payload.get("status", "draft"))
        if status == "active":
            raise PromptRegistryValidationError("active version cannot be created with is_active=0")
        validation_status = ensure_validation_status(payload.get("validation_status", "UNKNOWN"))
        validated_variables = self._validate_variables_payload(payload.get("variables", []))
        render_fingerprint = self._build_render_fingerprint(body_text, validated_variables)
        version_id: int | None = None
        in_txn = False
        try:
            self._conn.execute("BEGIN")
            in_txn = True
            version_id = self._create_version_in_current_transaction(
                prompt_id=prompt_id,
                body_text=body_text,
                status=status,
                validation_status=validation_status,
                validated_variables=validated_variables,
                actor=actor_id,
            )
            self._conn.execute("COMMIT")
            in_txn = False
        except Exception:
            if in_txn:
                self._conn.execute("ROLLBACK")
            raise
        if version_id is None:
            raise PromptRegistryValidationError("failed to create version")
        return self.get_version(version_id)

    def _create_version_in_current_transaction(
        self,
        *,
        prompt_id: int,
        body_text: str,
        status: str,
        validation_status: str,
        validated_variables: list[dict[str, Any]],
        actor: str,
    ) -> int:
        now = self._now_iso()
        render_fingerprint = self._build_render_fingerprint(body_text, validated_variables)
        version_no = self._next_version_no(prompt_id)
        cur = self._conn.execute(
            """
            INSERT INTO prompt_versions(prompt_id,version_no,body_text,render_fingerprint,status,validation_status,is_active,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (prompt_id, version_no, body_text, render_fingerprint, status, validation_status, 0, now, now),
        )
        version_id = int(cur.lastrowid)
        for variable in validated_variables:
            self._conn.execute(
                """
                INSERT INTO prompt_variables(prompt_version_id,name,safety_class,required,default_value,description,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    version_id,
                    variable["name"],
                    variable["safety_class"],
                    variable["required"],
                    variable["default_value"],
                    variable["description"],
                    now,
                    now,
                ),
            )
        self._write_audit_event(
            prompt_id=prompt_id,
            version_id=version_id,
            event_type="version_created",
            actor=actor,
            payload={"version_no": version_no},
        )
        return version_id

    def create_binding(self, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        prompt_id = int(payload.get("prompt_id") or 0)
        if prompt_id <= 0:
            raise PromptRegistryValidationError("prompt_id must be a positive integer")
        self.get_record(prompt_id)
        target = self._validate_binding_target(payload)
        binding_status = ensure_binding_status(payload.get("binding_status", "active"))
        now = self._now_iso()
        try:
            cur = self._conn.execute(
                """
                INSERT INTO prompt_bindings(prompt_id,binding_scope,workflow_slug,channel_slug,item_type,item_ref,binding_status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    prompt_id,
                    target["binding_scope"],
                    target["workflow_slug"],
                    target["channel_slug"],
                    target["item_type"],
                    target["item_ref"],
                    binding_status,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if "idx_prompt_bindings_unique_active_exact_target_prompt" in message:
                raise PromptRegistryConflictError("duplicate active binding for the same prompt and target is not allowed") from None
            raise
        binding_id = int(cur.lastrowid)
        self._write_audit_event(
            prompt_id=prompt_id,
            event_type="binding_created",
            actor=actor_id,
            payload={"binding_id": binding_id, "binding_scope": target["binding_scope"], "binding_status": binding_status},
        )
        return self.get_binding(binding_id)

    def list_versions(self, prompt_id: int) -> list[dict[str, Any]]:
        self.get_record(prompt_id)
        return list(self._conn.execute("SELECT * FROM prompt_versions WHERE prompt_id = ? ORDER BY version_no DESC", (prompt_id,)).fetchall())

    def get_version(self, version_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM prompt_versions WHERE id = ?", (version_id,)).fetchone()
        if row is None:
            raise PromptRegistryNotFoundError(f"prompt version {version_id} not found")
        item = dict(row)
        item["variables"] = list(
            self._conn.execute(
                "SELECT id,prompt_version_id,name,safety_class,required,default_value,description,created_at,updated_at FROM prompt_variables WHERE prompt_version_id = ? ORDER BY id ASC",
                (version_id,),
            ).fetchall()
        )
        return item

    def preview_version(self, version_id: int, payload: dict[str, Any], *, _write_usage: bool = True) -> dict[str, Any]:
        version = self.get_version(version_id)
        raw_variables = payload.get("variables", {})
        if raw_variables is None:
            raw_variables = {}
        if not isinstance(raw_variables, dict):
            raise PromptRegistryValidationError("variables must be an object")
        if "mask_sensitive" in payload and not isinstance(payload.get("mask_sensitive"), bool):
            raise PromptRegistryValidationError("mask_sensitive must be a boolean")
        mask_sensitive = bool(payload.get("mask_sensitive", True))

        declared_variables = version["variables"]
        variable_map = {str(item["name"]): item for item in declared_variables}
        sensitive_classes = {"secret", "operator_only"}
        defaults_used: list[str] = []
        missing_required: list[str] = []
        unknown_variables = sorted(str(name) for name in raw_variables.keys() if str(name) not in variable_map)

        resolved_values: dict[str, str] = {}
        for name, variable in variable_map.items():
            if name in raw_variables:
                resolved_values[name] = str(raw_variables[name])
                continue
            default_value = str(variable.get("default_value") or "")
            if default_value:
                resolved_values[name] = default_value
                defaults_used.append(name)
                continue
            if int(variable.get("required") or 0) == 1:
                missing_required.append(name)
                continue
            resolved_values[name] = ""

        used_variables: dict[str, str] = {}
        masked_variables: list[str] = []
        unresolved_placeholders: list[str] = []
        missing_required_set = set(missing_required)
        declared_placeholder_names = set(self._extract_placeholders(str(version["body_text"])))

        def _replace(match: re.Match[str]) -> str:
            name = match.group(1)
            normalized = name.strip()
            if normalized in variable_map:
                if normalized in missing_required_set:
                    unresolved_placeholders.append(normalized)
                    return match.group(0)
                rendered_value = resolved_values.get(normalized, "")
                safety_class = str(variable_map[normalized]["safety_class"])
                if mask_sensitive and safety_class in sensitive_classes:
                    if normalized not in masked_variables:
                        masked_variables.append(normalized)
                    used_variables[normalized] = "***MASKED***"
                    return "***MASKED***"
                used_variables[normalized] = rendered_value
                return rendered_value
            unresolved_placeholders.append(normalized)
            return match.group(0)

        rendered_text = re.sub(r"\{\{\s*([^{}]+?)\s*\}\}", _replace, str(version["body_text"]))
        unresolved_placeholders = sorted(set(unresolved_placeholders))
        preview_status = "INVALID" if missing_required or unresolved_placeholders else "OK"
        render_fingerprint = self._preview_render_fingerprint(
            version_id=int(version["id"]),
            prompt_id=int(version["prompt_id"]),
            rendered_text=rendered_text,
            used_variables=used_variables,
            preview_status=preview_status,
        )
        diagnostics = {
            "missing_required": sorted(missing_required),
            "defaults_used": sorted(defaults_used),
            "unknown_variables": unknown_variables,
            "masked_variables": sorted(masked_variables),
            "unresolved_placeholders": unresolved_placeholders,
            "declared_placeholders": sorted(declared_placeholder_names),
        }
        response = {
            "version_id": int(version["id"]),
            "prompt_id": int(version["prompt_id"]),
            "rendered_text": rendered_text,
            "missing_variables": sorted(missing_required),
            "used_variables": used_variables,
            "masked_variables": sorted(masked_variables),
            "render_fingerprint": render_fingerprint,
            "preview_status": preview_status,
            "diagnostics": diagnostics,
        }
        if _write_usage:
            self._write_usage_event(
                event_type="version_preview",
                status=preview_status,
                prompt_id=int(version["prompt_id"]),
                version_id=int(version["id"]),
                render_fingerprint=render_fingerprint,
                context={},
                variables_schema=self._safe_variables_schema(list(declared_variables)),
                diagnostics=diagnostics,
            )
        return response


    @staticmethod
    def _invalid_preview_payload(*, diagnostics: dict[str, Any], prompt_id: int | None = None) -> dict[str, Any]:
        return {
            "version_id": None,
            "prompt_id": prompt_id,
            "rendered_text": "",
            "missing_variables": [],
            "used_variables": {},
            "masked_variables": [],
            "render_fingerprint": "",
            "preview_status": "INVALID",
            "diagnostics": diagnostics,
        }

    def update_binding_status(self, binding_id: int, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        current = self.get_binding(binding_id)
        new_status = ensure_binding_status(payload.get("binding_status"))
        if str(current["binding_status"]) == new_status:
            return current
        now = self._now_iso()
        try:
            self._conn.execute(
                "UPDATE prompt_bindings SET binding_status = ?, updated_at = ? WHERE id = ?",
                (new_status, now, binding_id),
            )
        except sqlite3.IntegrityError as exc:
            message = str(exc)
            if "idx_prompt_bindings_unique_active_exact_target_prompt" in message:
                raise PromptRegistryConflictError("duplicate active binding for the same prompt and target is not allowed") from None
            raise
        updated = self.get_binding(binding_id)
        self._write_audit_event(
            prompt_id=int(updated["prompt_id"]),
            event_type="binding_status_updated",
            actor=actor_id,
            payload={
                "binding_id": int(updated["id"]),
                "from_status": str(current["binding_status"]),
                "to_status": new_status,
            },
        )
        return updated

    @staticmethod
    def _context_match(binding: dict[str, Any], context: dict[str, str | None]) -> tuple[bool, str, str]:
        scope = str(binding["binding_scope"])
        if str(binding["binding_status"]) != "active":
            return False, "ignored: binding_status is not active", "IGNORED_INACTIVE_BINDING"
        if scope == "item":
            item_type = context.get("item_type")
            item_ref = context.get("item_ref")
            if not item_type or not item_ref:
                return False, "ignored: item context missing", "IGNORED_ITEM_CONTEXT_MISSING"
            if str(binding.get("item_type") or "") != item_type or str(binding.get("item_ref") or "") != item_ref:
                return False, "ignored: item target mismatch", "IGNORED_ITEM_TARGET_MISMATCH"
            return True, "matched: item target exact match", "MATCHED_ITEM_EXACT"
        if scope == "channel":
            channel_slug = context.get("channel_slug")
            if not channel_slug:
                return False, "ignored: channel context missing", "IGNORED_CHANNEL_CONTEXT_MISSING"
            if str(binding.get("channel_slug") or "") != channel_slug:
                return False, "ignored: channel target mismatch", "IGNORED_CHANNEL_TARGET_MISMATCH"
            return True, "matched: channel target exact match", "MATCHED_CHANNEL_EXACT"
        if scope == "workflow":
            workflow_slug = context.get("workflow_slug")
            if not workflow_slug:
                return False, "ignored: workflow context missing", "IGNORED_WORKFLOW_CONTEXT_MISSING"
            if str(binding.get("workflow_slug") or "") != workflow_slug:
                return False, "ignored: workflow target mismatch", "IGNORED_WORKFLOW_TARGET_MISMATCH"
            return True, "matched: workflow target exact match", "MATCHED_WORKFLOW_EXACT"
        return True, "matched: global fallback", "MATCHED_GLOBAL_FALLBACK"

    def resolve_effective_prompt(self, payload: dict[str, Any]) -> dict[str, Any]:
        context = {
            "item_type": self._nullable_text(payload.get("item_type")),
            "item_ref": self._nullable_text(payload.get("item_ref")),
            "channel_slug": self._nullable_text(payload.get("channel_slug")),
            "workflow_slug": self._nullable_text(payload.get("workflow_slug")),
        }
        if (context["item_type"] and not context["item_ref"]) or (context["item_ref"] and not context["item_type"]):
            raise PromptRegistryValidationError("item_type and item_ref must be provided together or both omitted")
        rows = list(
            self._conn.execute(
                """
                SELECT b.*, r.slug AS prompt_slug, r.code AS prompt_code, r.title AS prompt_title, r.record_type AS prompt_record_type, r.status AS prompt_status, r.active_version_id
                FROM prompt_bindings b
                JOIN prompt_records r ON r.id = b.prompt_id
                ORDER BY b.updated_at DESC, b.id DESC
                """
            ).fetchall()
        )
        priority_order = ("item", "channel", "workflow", "global")
        candidates: list[dict[str, Any]] = []
        winner: dict[str, Any] | None = None
        winner_prompt: dict[str, Any] | None = None
        evaluated_order = 0
        for scope in priority_order:
            scoped = [row for row in rows if str(row["binding_scope"]) == scope]
            for row in scoped:
                evaluated_order += 1
                matched, reason, reason_code = self._context_match(row, context)
                candidate = {
                    "binding_id": int(row["id"]),
                    "prompt_id": int(row["prompt_id"]),
                    "binding_scope": str(row["binding_scope"]),
                    "binding_status": str(row["binding_status"]),
                    "workflow_slug": row.get("workflow_slug"),
                    "channel_slug": row.get("channel_slug"),
                    "item_type": row.get("item_type"),
                    "item_ref": row.get("item_ref"),
                    "priority_rank": priority_order.index(scope) + 1,
                    "evaluated_order": evaluated_order,
                    "matched": matched,
                    "reason": reason,
                    "reason_code": reason_code,
                }
                candidates.append(candidate)
                if matched and winner is None:
                    winner = candidate
                    winner_prompt = {
                        "id": int(row["prompt_id"]),
                        "slug": str(row["prompt_slug"]),
                        "code": str(row["prompt_code"]),
                        "title": str(row["prompt_title"]),
                        "record_type": str(row["prompt_record_type"]),
                        "status": str(row["prompt_status"]),
                        "active_version_id": row.get("active_version_id"),
                    }
                elif matched and winner is not None:
                    candidate["matched"] = False
                    if candidate["priority_rank"] > int(winner["priority_rank"]):
                        candidate["reason"] = "ignored: lower priority than winner"
                        candidate["reason_code"] = "IGNORED_LOWER_PRIORITY_THAN_WINNER"
                    else:
                        candidate["reason"] = "ignored: same scope older binding lost tie-breaker by updated_at/id"
                        candidate["reason_code"] = "IGNORED_SAME_SCOPE_OLDER_BINDING"
                        candidate["tie_break_note"] = (
                            "same_scope_tie_break=updated_at_desc_then_id_desc; older binding lost deterministically"
                        )
        return {
            "context": context,
            "winner_binding": winner,
            "winner_prompt": winner_prompt,
            "evaluated_candidates": candidates,
            "resolution_status": "matched" if winner is not None else "miss",
            "reason": "no matching active bindings for supplied context" if winner is None else "resolved_by_fixed_priority_order",
            "resolution_order": list(priority_order),
        }


    def preview_resolved_prompt(self, payload: dict[str, Any]) -> dict[str, Any]:
        raw_variables = payload.get("variables", {})
        if raw_variables is None:
            raw_variables = {}
        if not isinstance(raw_variables, dict):
            raise PromptRegistryValidationError("variables must be an object")
        if "mask_sensitive" in payload and not isinstance(payload.get("mask_sensitive"), bool):
            raise PromptRegistryValidationError("mask_sensitive must be a boolean")
        mask_sensitive = bool(payload.get("mask_sensitive", True))

        resolution = self.resolve_effective_prompt(payload)
        winner_prompt = resolution.get("winner_prompt")

        if resolution["resolution_status"] == "miss":
            preview = self._invalid_preview_payload(
                diagnostics={"errors": ["no matching prompt binding for supplied context"]}
            )
        else:
            active_version_id = winner_prompt.get("active_version_id") if isinstance(winner_prompt, dict) else None
            if active_version_id is None:
                preview = self._invalid_preview_payload(
                    prompt_id=int(winner_prompt["id"]) if isinstance(winner_prompt, dict) else None,
                    diagnostics={"errors": ["resolved prompt has no active version"]},
                )
            else:
                preview = self.preview_version(
                    int(active_version_id),
                    {"variables": raw_variables, "mask_sensitive": mask_sensitive},
                    _write_usage=False,
                )

        overall_status = "OK" if resolution["resolution_status"] == "matched" and preview["preview_status"] == "OK" else "INVALID"
        response = {
            "resolution": {
                "winner_binding": resolution.get("winner_binding"),
                "winner_prompt": resolution.get("winner_prompt"),
                "evaluated_candidates": resolution.get("evaluated_candidates", []),
                "resolution_status": resolution.get("resolution_status"),
                "resolution_order": resolution.get("resolution_order", []),
            },
            "preview": preview,
            "overall_status": overall_status,
        }
        winner_prompt = resolution.get("winner_prompt") if isinstance(resolution.get("winner_prompt"), dict) else None
        winner_binding = resolution.get("winner_binding") if isinstance(resolution.get("winner_binding"), dict) else None
        version_variables: list[dict[str, Any]] = []
        if preview.get("version_id") is not None:
            version_row = self.get_version(int(preview["version_id"]))
            version_variables = list(version_row.get("variables", []))
        usage_context = {
            "workflow_slug": self._nullable_text(payload.get("workflow_slug")),
            "channel_slug": self._nullable_text(payload.get("channel_slug")),
            "item_type": self._nullable_text(payload.get("item_type")),
            "item_ref": self._nullable_text(payload.get("item_ref")),
        }
        self._write_usage_event(
            event_type="resolved_preview",
            status=overall_status,
            prompt_id=int(winner_prompt["id"]) if isinstance(winner_prompt, dict) else None,
            version_id=int(preview["version_id"]) if preview.get("version_id") is not None else None,
            binding_id=int(winner_binding["binding_id"]) if isinstance(winner_binding, dict) else None,
            render_fingerprint=self._nullable_text(preview.get("render_fingerprint")),
            context=usage_context,
            variables_schema=self._safe_variables_schema(version_variables),
            diagnostics={
                "resolution_status": resolution.get("resolution_status"),
                "preview_status": preview.get("preview_status"),
                "preview_diagnostics": preview.get("diagnostics"),
            },
        )
        return response

    @staticmethod
    def _parse_json_object(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, str):
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _parse_json_array(raw: Any) -> list[dict[str, Any]]:
        if not isinstance(raw, str):
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [item for item in parsed if isinstance(item, dict)]

    @staticmethod
    def _require_object(value: Any, *, field_name: str) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise PromptRegistryValidationError(f"{field_name} must be an object")
        return value

    @staticmethod
    def _require_list(value: Any, *, field_name: str) -> list[dict[str, Any]]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise PromptRegistryValidationError(f"{field_name} must be a list")
        if any(not isinstance(item, dict) for item in value):
            raise PromptRegistryValidationError(f"{field_name} items must be objects")
        return [item for item in value if isinstance(item, dict)]

    def export_registry(
        self,
        *,
        prompt_id: int | None = None,
        include_inactive: bool = True,
        include_usage: bool = False,
    ) -> dict[str, Any]:
        where_parts: list[str] = []
        params: list[Any] = []
        if prompt_id is not None:
            where_parts.append("id = ?")
            params.append(int(prompt_id))
        if not include_inactive:
            where_parts.append("status != 'inactive'")
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        records_rows = list(
            self._conn.execute(
                f"SELECT * FROM prompt_records{where_clause} ORDER BY id ASC",
                tuple(params),
            ).fetchall()
        )
        if prompt_id is not None and not records_rows:
            raise PromptRegistryNotFoundError(f"prompt record {prompt_id} not found")
        exported_prompt_ids = [int(row["id"]) for row in records_rows]
        if not exported_prompt_ids:
            result: dict[str, Any] = {
                "schema_version": EXPORT_SCHEMA_VERSION,
                "exported_at": self._now_iso(),
                "records": [],
                "versions": [],
                "variables": [],
                "bindings": [],
            }
            if include_usage:
                result["usage_events_summary"] = self.usage_summary()
            return result

        prompt_slug_by_id = {int(row["id"]): str(row["slug"]) for row in records_rows}
        records = [
            {
                "slug": str(row["slug"]),
                "code": str(row["code"]),
                "title": str(row["title"]),
                "record_type": str(row["record_type"]),
                "status": str(row["status"]),
                "validation_status": str(row["validation_status"]),
                "bridge_policy_hook": row.get("bridge_policy_hook"),
            }
            for row in records_rows
        ]

        placeholders = ",".join(["?"] * len(exported_prompt_ids))
        versions_where = f"prompt_id IN ({placeholders})"
        versions_params: list[Any] = list(exported_prompt_ids)
        if not include_inactive:
            versions_where += " AND status != 'inactive'"
        version_rows = list(
            self._conn.execute(
                f"SELECT * FROM prompt_versions WHERE {versions_where} ORDER BY prompt_id ASC, version_no ASC, id ASC",
                tuple(versions_params),
            ).fetchall()
        )
        versions = [
            {
                "prompt_slug": prompt_slug_by_id[int(row["prompt_id"])],
                "version_number": int(row["version_no"]),
                "body_text": str(row["body_text"]),
                "status": str(row["status"]),
                "validation_status": str(row["validation_status"]),
            }
            for row in version_rows
        ]

        version_refs = {
            int(row["id"]): (prompt_slug_by_id[int(row["prompt_id"])], int(row["version_no"])) for row in version_rows
        }
        variables: list[dict[str, Any]] = []
        if version_refs:
            version_ids = sorted(version_refs)
            var_placeholders = ",".join(["?"] * len(version_ids))
            variable_rows = list(
                self._conn.execute(
                    f"SELECT * FROM prompt_variables WHERE prompt_version_id IN ({var_placeholders}) ORDER BY prompt_version_id ASC, id ASC",
                    tuple(version_ids),
                ).fetchall()
            )
            for row in variable_rows:
                prompt_slug, version_number = version_refs[int(row["prompt_version_id"])]
                variables.append(
                    {
                        "prompt_slug": prompt_slug,
                        "version_number": version_number,
                        "name": str(row["name"]),
                        "safety_class": str(row["safety_class"]),
                        "required": bool(int(row["required"])),
                        "default_value": (
                            self._REDACTED_EXPORT_DEFAULT
                            if str(row["safety_class"]) in {"secret", "operator_only"}
                            else str(row["default_value"] or "")
                        ),
                        "description": str(row["description"] or ""),
                    }
                )

        bindings_where = f"prompt_id IN ({placeholders})"
        bindings_params: list[Any] = list(exported_prompt_ids)
        if not include_inactive:
            bindings_where += " AND binding_status != 'inactive'"
        binding_rows = list(
            self._conn.execute(
                f"SELECT * FROM prompt_bindings WHERE {bindings_where} ORDER BY prompt_id ASC, id ASC",
                tuple(bindings_params),
            ).fetchall()
        )
        bindings = [
            {
                "prompt_slug": prompt_slug_by_id[int(row["prompt_id"])],
                "binding_scope": str(row["binding_scope"]),
                "workflow_slug": row.get("workflow_slug"),
                "channel_slug": row.get("channel_slug"),
                "item_type": row.get("item_type"),
                "item_ref": row.get("item_ref"),
                "binding_status": str(row["binding_status"]),
            }
            for row in binding_rows
        ]
        result = {
            "schema_version": EXPORT_SCHEMA_VERSION,
            "exported_at": self._now_iso(),
            "records": records,
            "versions": versions,
            "variables": variables,
            "bindings": bindings,
        }
        if include_usage:
            result["usage_events_summary"] = self.usage_summary(prompt_id=prompt_id)
        return result

    def _validate_import_payload(self, payload: Any, *, mode: Any) -> dict[str, Any]:
        ensure_import_mode(mode)
        root = self._require_object(payload, field_name="payload")
        schema_version = str(root.get("schema_version") or "").strip()
        records = self._require_list(root.get("records"), field_name="payload.records")
        versions = self._require_list(root.get("versions"), field_name="payload.versions")
        variables = self._require_list(root.get("variables"), field_name="payload.variables")
        bindings = self._require_list(root.get("bindings"), field_name="payload.bindings")
        if schema_version != EXPORT_SCHEMA_VERSION:
            raise PromptRegistryValidationError("invalid schema_version")
        return {
            "schema_version": schema_version,
            "records": records,
            "versions": versions,
            "variables": variables,
            "bindings": bindings,
        }

    def _preview_import_summary(self, payload: dict[str, Any], *, mode: str) -> dict[str, Any]:
        normalized = self._validate_import_payload(payload, mode=mode)
        records = normalized["records"]
        versions = normalized["versions"]
        bindings = normalized["bindings"]
        variables = normalized["variables"]
        conflicts: list[str] = []
        validation_errors: list[str] = []

        slug_counts: dict[str, int] = {}
        code_counts: dict[str, int] = {}
        for record in records:
            try:
                slug = ensure_non_empty(record.get("slug"), field_name="record.slug")
                code = ensure_non_empty(record.get("code"), field_name="record.code")
                ensure_non_empty(record.get("title"), field_name="record.title")
                ensure_record_type(record.get("record_type"))
                ensure_record_status(record.get("status", "draft"))
                ensure_validation_status(record.get("validation_status", "UNKNOWN"))
            except ValueError as exc:
                validation_errors.append(str(exc))
                continue
            slug_counts[slug] = slug_counts.get(slug, 0) + 1
            code_counts[code] = code_counts.get(code, 0) + 1
        for slug, count in slug_counts.items():
            if count > 1:
                validation_errors.append(f"duplicate record slug in payload: {slug}")
        for code, count in code_counts.items():
            if count > 1:
                validation_errors.append(f"duplicate record code in payload: {code}")

        existing_rows = list(self._conn.execute("SELECT id,slug,code FROM prompt_records").fetchall())
        existing_by_slug = {str(row["slug"]): row for row in existing_rows}
        existing_by_code = {str(row["code"]): row for row in existing_rows}

        records_to_create = 0
        records_to_update = 0
        imported_slugs: set[str] = set()
        for record in records:
            slug = str(record.get("slug") or "").strip()
            code = str(record.get("code") or "").strip()
            if not slug or not code:
                continue
            imported_slugs.add(slug)
            existing_slug_row = existing_by_slug.get(slug)
            existing_code_row = existing_by_code.get(code)
            if existing_slug_row is None:
                if existing_code_row is not None:
                    conflicts.append(f"record code conflict for slug {slug}: {code} already exists")
                    continue
                records_to_create += 1
            else:
                if str(existing_slug_row["code"]) != code:
                    conflicts.append(f"record slug conflict for {slug}: code mismatch")
                    continue
                records_to_update += 1

        seen_versions: set[tuple[str, int]] = set()
        versions_to_create = 0
        for version in versions:
            try:
                v_slug = ensure_non_empty(version.get("prompt_slug"), field_name="version.prompt_slug")
                v_number = int(version.get("version_number"))
                ensure_non_empty(version.get("body_text"), field_name="version.body_text")
                ensure_record_status(version.get("status", "draft"))
                ensure_validation_status(version.get("validation_status", "UNKNOWN"))
            except (TypeError, ValueError) as exc:
                validation_errors.append(str(exc))
                continue
            key = (v_slug, v_number)
            if key in seen_versions:
                validation_errors.append(f"duplicate version in payload: {v_slug}#{v_number}")
                continue
            seen_versions.add(key)
            if v_slug not in imported_slugs and v_slug not in existing_by_slug:
                conflicts.append(f"version references unknown prompt_slug: {v_slug}")
                continue
            existing_prompt = existing_by_slug.get(v_slug)
            if existing_prompt is not None:
                existing_version = self._conn.execute(
                    "SELECT id FROM prompt_versions WHERE prompt_id = ? AND version_no = ?",
                    (int(existing_prompt["id"]), v_number),
                ).fetchone()
                if existing_version is None:
                    versions_to_create += 1
            else:
                versions_to_create += 1

        seen_variables: set[tuple[str, int, str]] = set()
        for variable in variables:
            try:
                vv_slug = ensure_non_empty(variable.get("prompt_slug"), field_name="variable.prompt_slug")
                vv_number = int(variable.get("version_number"))
                vv_name = ensure_non_empty(variable.get("name"), field_name="variable.name")
                ensure_safety_class(variable.get("safety_class"))
            except (TypeError, ValueError) as exc:
                validation_errors.append(str(exc))
                continue
            var_key = (vv_slug, vv_number, vv_name)
            if var_key in seen_variables:
                validation_errors.append(f"duplicate variable in payload: {vv_slug}#{vv_number}:{vv_name}")
                continue
            seen_variables.add(var_key)

        seen_bindings: set[tuple[str, str, str | None, str | None, str | None, str | None]] = set()
        bindings_to_create = 0
        for binding in bindings:
            try:
                b_slug = ensure_non_empty(binding.get("prompt_slug"), field_name="binding.prompt_slug")
                b_scope = ensure_binding_scope(binding.get("binding_scope"))
                b_status = ensure_binding_status(binding.get("binding_status", "active"))
                target = self._validate_binding_target(binding)
            except ValueError as exc:
                validation_errors.append(str(exc))
                continue
            composite = (
                b_slug,
                b_scope,
                target.get("workflow_slug"),
                target.get("channel_slug"),
                target.get("item_type"),
                target.get("item_ref"),
            )
            if composite in seen_bindings:
                validation_errors.append(f"duplicate binding in payload for {b_slug}/{b_scope}")
                continue
            seen_bindings.add(composite)
            if b_status not in ("active", "inactive"):
                validation_errors.append("binding_status must be active or inactive")
                continue
            if b_slug not in imported_slugs and b_slug not in existing_by_slug:
                conflicts.append(f"binding references unknown prompt_slug: {b_slug}")
                continue
            existing_prompt = existing_by_slug.get(b_slug)
            if existing_prompt is None:
                bindings_to_create += 1
                continue
            existing_binding = self._conn.execute(
                """
                SELECT id FROM prompt_bindings
                WHERE prompt_id = ?
                  AND binding_scope = ?
                  AND IFNULL(workflow_slug,'') = IFNULL(?, '')
                  AND IFNULL(channel_slug,'') = IFNULL(?, '')
                  AND IFNULL(item_type,'') = IFNULL(?, '')
                  AND IFNULL(item_ref,'') = IFNULL(?, '')
                LIMIT 1
                """,
                (
                    int(existing_prompt["id"]),
                    b_scope,
                    target.get("workflow_slug"),
                    target.get("channel_slug"),
                    target.get("item_type"),
                    target.get("item_ref"),
                ),
            ).fetchone()
            if existing_binding is None:
                bindings_to_create += 1
        canonical = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        import_status = "INVALID" if validation_errors or conflicts else "OK"
        return {
            "import_status": import_status,
            "summary": {
                "records_to_create": records_to_create,
                "records_to_update": records_to_update,
                "versions_to_create": versions_to_create,
                "bindings_to_create": bindings_to_create,
                "conflicts": sorted(set(conflicts)),
                "validation_errors": sorted(set(validation_errors)),
            },
            "fingerprint": fingerprint,
        }

    def preview_import(self, payload: Any, *, mode: Any) -> dict[str, Any]:
        return self._preview_import_summary(self._require_object(payload, field_name="payload"), mode=str(mode or ""))

    def confirm_import(self, payload: Any, *, mode: Any, dry_run: bool = False, actor: str = "importer") -> dict[str, Any]:
        normalized_mode = ensure_import_mode(mode)
        preview = self.preview_import(payload, mode=normalized_mode)
        if dry_run or preview["import_status"] != "OK":
            return preview

        normalized_payload = self._validate_import_payload(payload, mode=normalized_mode)
        imported_records = normalized_payload["records"]
        imported_versions = normalized_payload["versions"]
        imported_variables = normalized_payload["variables"]
        imported_bindings = normalized_payload["bindings"]

        self._conn.execute("BEGIN")
        in_txn = True
        try:
            slug_to_prompt_id: dict[str, int] = {}
            for record in imported_records:
                slug = str(record["slug"]).strip()
                code = str(record["code"]).strip()
                existing = self._conn.execute("SELECT * FROM prompt_records WHERE slug = ?", (slug,)).fetchone()
                record_payload = {
                    "slug": slug,
                    "code": code,
                    "title": str(record.get("title") or ""),
                    "record_type": str(record.get("record_type") or "prompt_template"),
                    "status": str(record.get("status") or "draft"),
                    "validation_status": str(record.get("validation_status") or "UNKNOWN"),
                    "bridge_policy_hook": self._nullable_text(record.get("bridge_policy_hook")),
                }
                if existing is None:
                    created = self.create_record(record_payload, actor=actor)
                    slug_to_prompt_id[slug] = int(created["id"])
                else:
                    updated = self.update_record(int(existing["id"]), record_payload, actor=actor)
                    if str(updated["code"]) != code:
                        raise PromptRegistryValidationError(f"record slug conflict for {slug}: code mismatch")
                    slug_to_prompt_id[slug] = int(updated["id"])

            for row in self._conn.execute("SELECT id,slug FROM prompt_records").fetchall():
                slug_to_prompt_id[str(row["slug"])] = int(row["id"])

            variables_by_version: dict[tuple[str, int], list[dict[str, Any]]] = {}
            for variable in imported_variables:
                slug = str(variable["prompt_slug"]).strip()
                version_number = int(variable["version_number"])
                variables_by_version.setdefault((slug, version_number), []).append(variable)

            for version in imported_versions:
                slug = str(version["prompt_slug"]).strip()
                version_number = int(version["version_number"])
                prompt_id = slug_to_prompt_id.get(slug)
                if prompt_id is None:
                    raise PromptRegistryValidationError(f"version references unknown prompt_slug: {slug}")
                existing_version = self._conn.execute(
                    "SELECT id FROM prompt_versions WHERE prompt_id = ? AND version_no = ?",
                    (prompt_id, version_number),
                ).fetchone()
                if existing_version is not None:
                    continue
                imported_version_variables = self._validate_variables_payload(variables_by_version.get((slug, version_number), []))
                self._create_version_in_current_transaction(
                    prompt_id=prompt_id,
                    body_text=ensure_non_empty(version.get("body_text"), field_name="version.body_text"),
                    status=ensure_record_status(version.get("status", "draft")),
                    validation_status=ensure_validation_status(version.get("validation_status", "UNKNOWN")),
                    validated_variables=imported_version_variables,
                    actor=actor,
                )

            for binding in imported_bindings:
                slug = str(binding["prompt_slug"]).strip()
                prompt_id = slug_to_prompt_id.get(slug)
                if prompt_id is None:
                    raise PromptRegistryValidationError(f"binding references unknown prompt_slug: {slug}")
                target = self._validate_binding_target(binding)
                scope = ensure_binding_scope(binding.get("binding_scope"))
                existing_binding = self._conn.execute(
                    """
                    SELECT id FROM prompt_bindings
                    WHERE prompt_id = ?
                      AND binding_scope = ?
                      AND IFNULL(workflow_slug,'') = IFNULL(?, '')
                      AND IFNULL(channel_slug,'') = IFNULL(?, '')
                      AND IFNULL(item_type,'') = IFNULL(?, '')
                      AND IFNULL(item_ref,'') = IFNULL(?, '')
                    LIMIT 1
                    """,
                    (
                        prompt_id,
                        scope,
                        target.get("workflow_slug"),
                        target.get("channel_slug"),
                        target.get("item_type"),
                        target.get("item_ref"),
                    ),
                ).fetchone()
                if existing_binding is None:
                    self.create_binding(
                        {
                            "prompt_id": prompt_id,
                            "binding_scope": scope,
                            "workflow_slug": target.get("workflow_slug"),
                            "channel_slug": target.get("channel_slug"),
                            "item_type": target.get("item_type"),
                            "item_ref": target.get("item_ref"),
                            "binding_status": ensure_binding_status(binding.get("binding_status", "active")),
                        },
                        actor=actor,
                    )
            self._conn.execute("COMMIT")
            in_txn = False
        except Exception:
            if in_txn:
                self._conn.execute("ROLLBACK")
            raise
        return preview

    def list_usage_events(
        self,
        *,
        prompt_id: int | None = None,
        version_id: int | None = None,
        event_type: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        where_parts: list[str] = []
        params: list[Any] = []
        if prompt_id is not None:
            where_parts.append("prompt_id = ?")
            params.append(int(prompt_id))
        if version_id is not None:
            where_parts.append("version_id = ?")
            params.append(int(version_id))
        if event_type is not None:
            where_parts.append("event_type = ?")
            params.append(ensure_usage_event_type(event_type))
        if status is not None:
            where_parts.append("status = ?")
            params.append(ensure_usage_event_status(status))
        safe_limit = max(1, min(int(limit), 200))
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        rows = list(
            self._conn.execute(
                f"SELECT * FROM prompt_usage_events{where_clause} ORDER BY created_at DESC, id DESC LIMIT ?",
                tuple(params + [safe_limit]),
            ).fetchall()
        )
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "id": int(row["id"]),
                    "prompt_id": row.get("prompt_id"),
                    "version_id": row.get("version_id"),
                    "binding_id": row.get("binding_id"),
                    "event_type": str(row["event_type"]),
                    "source": str(row["source"]),
                    "status": str(row["status"]),
                    "render_fingerprint": row.get("render_fingerprint"),
                    "context": self._parse_json_object(row.get("context_json")),
                    "variables_schema": self._parse_json_array(row.get("variables_schema_json")),
                    "diagnostics": self._parse_json_object(row.get("diagnostics_json")),
                    "created_at": str(row["created_at"]),
                }
            )
        return items

    def usage_summary(
        self,
        *,
        prompt_id: int | None = None,
        version_id: int | None = None,
        event_type: str | None = None,
    ) -> dict[str, Any]:
        where_parts: list[str] = []
        params: list[Any] = []
        if prompt_id is not None:
            where_parts.append("prompt_id = ?")
            params.append(int(prompt_id))
        if version_id is not None:
            where_parts.append("version_id = ?")
            params.append(int(version_id))
        if event_type is not None:
            where_parts.append("event_type = ?")
            params.append(ensure_usage_event_type(event_type))
        where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
        rows = list(
            self._conn.execute(
                f"SELECT prompt_id,version_id,event_type,status,created_at FROM prompt_usage_events{where_clause} ORDER BY created_at DESC, id DESC",
                tuple(params),
            ).fetchall()
        )
        if not rows:
            return {
                "total_events": 0,
                "by_event_type": {},
                "by_status": {},
                "latest_event_at": None,
                "prompt_ids": [],
                "version_ids": [],
            }
        by_event_type: dict[str, int] = {}
        by_status: dict[str, int] = {}
        prompt_ids: set[int] = set()
        version_ids: set[int] = set()
        latest_event_at = str(rows[0]["created_at"])
        for row in rows:
            event_key = str(row["event_type"])
            status_key = str(row["status"])
            by_event_type[event_key] = by_event_type.get(event_key, 0) + 1
            by_status[status_key] = by_status.get(status_key, 0) + 1
            if row.get("prompt_id") is not None:
                prompt_ids.add(int(row["prompt_id"]))
            if row.get("version_id") is not None:
                version_ids.add(int(row["version_id"]))
        return {
            "total_events": len(rows),
            "by_event_type": by_event_type,
            "by_status": by_status,
            "latest_event_at": latest_event_at,
            "prompt_ids": sorted(prompt_ids),
            "version_ids": sorted(version_ids),
        }

    def activate_version(self, version_id: int, *, actor: str) -> dict[str, Any]:
        actor_id = self._validated_actor(actor)
        version = self.get_version(version_id)
        prompt_id = int(version["prompt_id"])
        now = self._now_iso()
        self._conn.execute("UPDATE prompt_versions SET is_active = 0, updated_at = ? WHERE prompt_id = ?", (now, prompt_id))
        self._conn.execute("UPDATE prompt_versions SET is_active = 1, updated_at = ?, status = 'active' WHERE id = ?", (now, version_id))
        self._conn.execute("UPDATE prompt_records SET active_version_id = ?, status = 'active', updated_at = ? WHERE id = ?", (version_id, now, prompt_id))
        self._write_audit_event(prompt_id=prompt_id, version_id=version_id, event_type="version_activated", actor=actor_id)
        return self.get_version(version_id)

    def list_audit_events(self, prompt_id: int, *, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 200))
        return list(
            self._conn.execute(
                "SELECT * FROM prompt_audit_events WHERE prompt_id = ? ORDER BY id ASC LIMIT ?",
                (prompt_id, safe_limit),
            ).fetchall()
        )

    def get_audit_diagnostics(self, prompt_id: int) -> dict[str, Any]:
        self.get_record(prompt_id)
        rows = self.list_audit_events(prompt_id)
        items: list[dict[str, Any]] = []
        for row in rows:
            payload: dict[str, Any]
            raw_payload = row.get("payload_json")
            if isinstance(raw_payload, str):
                try:
                    parsed_payload = json.loads(raw_payload)
                    payload = parsed_payload if isinstance(parsed_payload, dict) else {}
                except json.JSONDecodeError:
                    payload = {}
            else:
                payload = {}
            items.append(
                {
                    "id": int(row["id"]),
                    "event_type": str(row["event_type"]),
                    "actor": str(row["actor"]),
                    "version_id": row["version_id"],
                    "payload": payload,
                    "created_at": str(row["created_at"]),
                }
            )
        return {"prompt_id": prompt_id, "items": items}
