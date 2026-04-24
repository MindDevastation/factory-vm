from __future__ import annotations

import json
import sqlite3
import hashlib
import re
from datetime import datetime, timezone
from typing import Any

from services.prompt_registry.contracts import (
    ensure_binding_scope,
    ensure_binding_status,
    ensure_lifecycle_transition,
    ensure_non_empty,
    ensure_record_status,
    ensure_record_type,
    ensure_safety_class,
    ensure_validation_status,
)
from services.prompt_registry.errors import (
    PromptRegistryConflictError,
    PromptRegistryNotFoundError,
    PromptRegistryValidationError,
)


class PromptRegistryService:
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
                actor=actor_id,
                payload={"version_no": version_no},
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

    def preview_version(self, version_id: int, payload: dict[str, Any]) -> dict[str, Any]:
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
        return {
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
                )

        overall_status = "OK" if resolution["resolution_status"] == "matched" and preview["preview_status"] == "OK" else "INVALID"
        return {
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

    def list_audit_events(self, prompt_id: int) -> list[dict[str, Any]]:
        return list(self._conn.execute("SELECT * FROM prompt_audit_events WHERE prompt_id = ? ORDER BY id ASC", (prompt_id,)).fetchall())

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
