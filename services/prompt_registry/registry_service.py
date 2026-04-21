from __future__ import annotations

import json
import sqlite3
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

    def create_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = self._now_iso()
        slug = ensure_non_empty(payload.get("slug"), field_name="slug")
        code = ensure_non_empty(payload.get("code"), field_name="code")
        title = ensure_non_empty(payload.get("title"), field_name="title")
        record_type = ensure_record_type(payload.get("record_type"))
        status = ensure_record_status(payload.get("status", "draft"))
        validation_status = ensure_validation_status(payload.get("validation_status", "UNKNOWN"))
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
        self._write_audit_event(prompt_id=int(created["id"]), event_type="record_created", actor="system", payload={"status": status})
        return created

    def update_record(self, prompt_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.get_record(prompt_id)
        merged = dict(current)
        merged.update(payload)
        if "status" in payload:
            new_status = ensure_record_status(merged.get("status"))
            ensure_lifecycle_transition(current_status=str(current.get("status")), new_status=new_status)
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
        self._write_audit_event(prompt_id=prompt_id, event_type="record_updated", actor="system", payload={"fields": sorted(updates.keys())})
        return updated

    def _next_version_no(self, prompt_id: int) -> int:
        row = self._conn.execute("SELECT COALESCE(MAX(version_no), 0) AS v FROM prompt_versions WHERE prompt_id = ?", (prompt_id,)).fetchone()
        return int(row["v"]) + 1

    def create_version(self, prompt_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        self.get_record(prompt_id)
        now = self._now_iso()
        body_text = ensure_non_empty(payload.get("body_text"), field_name="body_text")
        status = ensure_record_status(payload.get("status", "draft"))
        validation_status = ensure_validation_status(payload.get("validation_status", "UNKNOWN"))
        variables = payload.get("variables", [])
        if variables is None:
            variables = []
        if not isinstance(variables, list):
            raise PromptRegistryValidationError("variables must be a list")
        version_no = self._next_version_no(prompt_id)
        cur = self._conn.execute(
            """
            INSERT INTO prompt_versions(prompt_id,version_no,body_text,status,validation_status,is_active,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (prompt_id, version_no, body_text, status, validation_status, 0, now, now),
        )
        version_id = int(cur.lastrowid)
        for item in variables:
            if not isinstance(item, dict):
                raise PromptRegistryValidationError("variable must be an object")
            self._conn.execute(
                """
                INSERT INTO prompt_variables(prompt_version_id,name,safety_class,required,default_value,description,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    version_id,
                    ensure_non_empty(item.get("name"), field_name="variable.name"),
                    ensure_safety_class(item.get("safety_class")),
                    1 if bool(item.get("required", True)) else 0,
                    str(item.get("default_value", "")),
                    str(item.get("description", "")),
                    now,
                    now,
                ),
            )
        self._write_audit_event(prompt_id=prompt_id, version_id=version_id, event_type="version_created", actor="system", payload={"version_no": version_no})
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

    def activate_version(self, version_id: int) -> dict[str, Any]:
        version = self.get_version(version_id)
        prompt_id = int(version["prompt_id"])
        now = self._now_iso()
        self._conn.execute("UPDATE prompt_versions SET is_active = 0, updated_at = ? WHERE prompt_id = ?", (now, prompt_id))
        self._conn.execute("UPDATE prompt_versions SET is_active = 1, updated_at = ?, status = 'active' WHERE id = ?", (now, version_id))
        self._conn.execute("UPDATE prompt_records SET active_version_id = ?, status = 'active', updated_at = ? WHERE id = ?", (version_id, now, prompt_id))
        self._write_audit_event(prompt_id=prompt_id, version_id=version_id, event_type="version_activated", actor="system")
        return self.get_version(version_id)

    def list_audit_events(self, prompt_id: int) -> list[dict[str, Any]]:
        return list(self._conn.execute("SELECT * FROM prompt_audit_events WHERE prompt_id = ? ORDER BY id ASC", (prompt_id,)).fetchall())
