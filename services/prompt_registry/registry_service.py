from __future__ import annotations

import json
import sqlite3
import hashlib
from datetime import datetime, timezone
from typing import Any

from services.prompt_registry.contracts import (
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

    def list_records(self) -> list[dict[str, Any]]:
        return list(self._conn.execute("SELECT * FROM prompt_records ORDER BY updated_at DESC, id DESC").fetchall())

    def get_record(self, prompt_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM prompt_records WHERE id = ?", (prompt_id,)).fetchone()
        if row is None:
            raise PromptRegistryNotFoundError(f"prompt record {prompt_id} not found")
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
