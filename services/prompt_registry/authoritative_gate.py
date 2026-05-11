from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

PERMISSION_CLASSES: tuple[str, ...] = (
    "runtime_view",
    "runtime_execute",
    "runtime_operate",
    "runtime_admin",
)
CAPABILITY_STATUSES: tuple[str, ...] = ("active", "disabled", "deprecated")

_PERMISSION_RANK = {name: rank for rank, name in enumerate(PERMISSION_CLASSES)}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def validate_permission_class(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized not in PERMISSION_CLASSES:
        raise ValueError(f"invalid permission_class: {normalized}")
    return normalized


def validate_capability_status(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized not in CAPABILITY_STATUSES:
        raise ValueError(f"invalid capability status: {normalized}")
    return normalized


def permission_class_satisfies(resolved: str | None, required: str | None) -> bool:
    if resolved not in _PERMISSION_RANK or required not in _PERMISSION_RANK:
        return False
    return _PERMISSION_RANK[str(resolved)] >= _PERMISSION_RANK[str(required)]


def _row_to_dict(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return dict(row)
    return {key: row[key] for key in row.keys()}


def _bool_int(value: Any) -> int:
    return 1 if bool(value) else 0


@dataclass(frozen=True)
class CapabilityGateResult:
    capability_code: str
    exists: bool
    execution_enabled: bool
    required_permission_class: str | None
    status: str | None
    admissible: bool
    failure_reason_code: str | None
    authoritative_source_metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "capability_code": self.capability_code,
            "exists": self.exists,
            "execution_enabled": self.execution_enabled,
            "required_permission_class": self.required_permission_class,
            "status": self.status,
            "admissible": self.admissible,
            "failure_reason_code": self.failure_reason_code,
            "authoritative_source_metadata": self.authoritative_source_metadata,
        }


@dataclass(frozen=True)
class OperatorPermissionResult:
    operator_subject: str
    exists: bool
    permission_class: str | None
    is_enabled: bool
    admissible: bool
    failure_reason_code: str | None
    authoritative_source_metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "operator_subject": self.operator_subject,
            "exists": self.exists,
            "permission_class": self.permission_class,
            "is_enabled": self.is_enabled,
            "admissible": self.admissible,
            "failure_reason_code": self.failure_reason_code,
            "authoritative_source_metadata": self.authoritative_source_metadata,
        }


class CapabilityGateService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get_row(self, capability_code: str) -> dict[str, Any] | None:
        code = str(capability_code or "").strip()
        if not code:
            return None
        return _row_to_dict(self._conn.execute("SELECT * FROM prompt_runtime_capability_registry WHERE capability_code=?", (code,)).fetchone())

    def list_rows(self) -> list[dict[str, Any]]:
        return [dict(row) for row in map(_row_to_dict, self._conn.execute("SELECT * FROM prompt_runtime_capability_registry ORDER BY capability_code ASC").fetchall()) if row]

    def evaluate(self, capability_code: str) -> CapabilityGateResult:
        code = str(capability_code or "").strip()
        row = self.get_row(code)
        if row is None:
            return CapabilityGateResult(code, False, False, None, None, False, "missing_capability_authority", {"source_table": "prompt_runtime_capability_registry"})
        enabled = bool(int(row.get("execution_enabled") or 0))
        status = str(row.get("status") or "")
        required = str(row.get("required_permission_class") or "")
        if not enabled:
            reason = "capability_execution_disabled"
        elif status != "active":
            reason = f"capability_status_{status or 'invalid'}"
        else:
            reason = None
        return CapabilityGateResult(
            code,
            True,
            enabled,
            required,
            status,
            reason is None,
            reason,
            {
                "source_table": "prompt_runtime_capability_registry",
                "source_id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "updated_by_operator": row.get("updated_by_operator"),
            },
        )

    def upsert(self, capability_code: str, payload: dict[str, Any], *, updated_by_operator: str) -> dict[str, Any]:
        code = str(capability_code or "").strip()
        if not code:
            raise ValueError("capability_code is required")
        required = validate_permission_class(str(payload.get("required_permission_class") or ""))
        status = validate_capability_status(str(payload.get("status") or ""))
        execution_enabled = _bool_int(payload.get("execution_enabled"))
        notes = payload.get("notes")
        now = utc_now_iso()
        self._conn.execute(
            """
            INSERT INTO prompt_runtime_capability_registry(capability_code,execution_enabled,required_permission_class,status,notes,updated_by_operator,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(capability_code) DO UPDATE SET
                execution_enabled=excluded.execution_enabled,
                required_permission_class=excluded.required_permission_class,
                status=excluded.status,
                notes=excluded.notes,
                updated_by_operator=excluded.updated_by_operator,
                updated_at=excluded.updated_at
            """,
            (code, execution_enabled, required, status, None if notes is None else str(notes), str(updated_by_operator), now, now),
        )
        row = self.get_row(code)
        assert row is not None
        return row


class OperatorPermissionService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get_row(self, operator_subject: str) -> dict[str, Any] | None:
        subject = str(operator_subject or "").strip()
        if not subject:
            return None
        return _row_to_dict(self._conn.execute("SELECT * FROM prompt_runtime_operator_permissions WHERE operator_subject=?", (subject,)).fetchone())

    def evaluate(self, operator_subject: str) -> OperatorPermissionResult:
        subject = str(operator_subject or "").strip()
        row = self.get_row(subject)
        if row is None:
            return OperatorPermissionResult(subject, False, None, False, False, "missing_operator_permission_authority", {"source_table": "prompt_runtime_operator_permissions"})
        enabled = bool(int(row.get("is_enabled") or 0))
        permission_class = str(row.get("permission_class") or "")
        reason = None if enabled else "operator_permission_disabled"
        return OperatorPermissionResult(
            subject,
            True,
            permission_class,
            enabled,
            reason is None,
            reason,
            {
                "source_table": "prompt_runtime_operator_permissions",
                "source_id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "updated_by_operator": row.get("updated_by_operator"),
            },
        )

    def upsert(self, operator_subject: str, payload: dict[str, Any], *, updated_by_operator: str) -> dict[str, Any]:
        subject = str(operator_subject or "").strip()
        if not subject:
            raise ValueError("operator_subject is required")
        permission = validate_permission_class(str(payload.get("permission_class") or ""))
        is_enabled = _bool_int(payload.get("is_enabled"))
        notes = payload.get("notes")
        now = utc_now_iso()
        self._conn.execute(
            """
            INSERT INTO prompt_runtime_operator_permissions(operator_subject,permission_class,is_enabled,notes,updated_by_operator,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(operator_subject) DO UPDATE SET
                permission_class=excluded.permission_class,
                is_enabled=excluded.is_enabled,
                notes=excluded.notes,
                updated_by_operator=excluded.updated_by_operator,
                updated_at=excluded.updated_at
            """,
            (subject, permission, is_enabled, None if notes is None else str(notes), str(updated_by_operator), now, now),
        )
        row = self.get_row(subject)
        assert row is not None
        return row
