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

VALIDATION_STATUSES: tuple[str, ...] = ("passed", "failed", "error", "superseded")


def validate_render_validation_status(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized not in VALIDATION_STATUSES:
        raise ValueError(f"invalid validation_status: {normalized}")
    return normalized


def _secret_safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    lowered = text.lower()
    secret_markers = ("token", "secret", "password", "api_key", "apikey", "authorization", "bearer", "credential", "private_key")
    if any(marker in lowered for marker in secret_markers):
        return "[redacted]"
    return text


@dataclass(frozen=True)
class RenderValidationEvaluation:
    prompt_version_id: int
    binding_fingerprint: str
    render_result_hash: str
    verdict: str
    trusted: bool
    latest_validation_record: dict[str, Any] | None
    failure_reason_code: str | None
    authoritative_source_metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "prompt_version_id": self.prompt_version_id,
            "binding_fingerprint": self.binding_fingerprint,
            "render_result_hash": self.render_result_hash,
            "verdict": self.verdict,
            "trusted": self.trusted,
            "latest_validation_record": self.latest_validation_record,
            "failure_reason_code": self.failure_reason_code,
            "authoritative_source_metadata": self.authoritative_source_metadata,
        }


class RenderValidationService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record_validation(
        self,
        *,
        prompt_record_id: int,
        prompt_version_id: int,
        binding_fingerprint: str,
        render_result_hash: str,
        validation_status: str,
        validation_schema_version: str,
        validator_code: str,
        validated_at: str | None = None,
        invalid_reason_code: str | None = None,
        invalid_reason_detail: str | None = None,
        superseded_by_validation_id: int | None = None,
    ) -> dict[str, Any]:
        status = validate_render_validation_status(validation_status)
        binding = str(binding_fingerprint or "").strip()
        render_hash = str(render_result_hash or "").strip()
        schema_version = str(validation_schema_version or "").strip()
        validator = str(validator_code or "").strip()
        if int(prompt_record_id) <= 0:
            raise ValueError("prompt_record_id is required")
        if int(prompt_version_id) <= 0:
            raise ValueError("prompt_version_id is required")
        if not binding:
            raise ValueError("binding_fingerprint is required")
        if not render_hash:
            raise ValueError("render_result_hash is required")
        if not schema_version:
            raise ValueError("validation_schema_version is required")
        if not validator:
            raise ValueError("validator_code is required")
        now = utc_now_iso()
        effective_validated_at = str(validated_at or now).strip()
        cur = self._conn.execute(
            """
            INSERT INTO prompt_runtime_render_validation_ledger(
                prompt_record_id,prompt_version_id,binding_fingerprint,render_result_hash,
                validation_status,validation_schema_version,validator_code,validated_at,
                invalid_reason_code,invalid_reason_detail,superseded_by_validation_id,created_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(prompt_record_id),
                int(prompt_version_id),
                binding,
                render_hash,
                status,
                schema_version,
                validator,
                effective_validated_at,
                None if invalid_reason_code is None else str(invalid_reason_code),
                _secret_safe_text(invalid_reason_detail),
                None if superseded_by_validation_id is None else int(superseded_by_validation_id),
                now,
            ),
        )
        row = _row_to_dict(self._conn.execute("SELECT * FROM prompt_runtime_render_validation_ledger WHERE id=?", (int(cur.lastrowid),)).fetchone())
        assert row is not None
        return row

    def get_latest_validation(self, *, prompt_version_id: int, binding_fingerprint: str, render_result_hash: str) -> dict[str, Any] | None:
        binding = str(binding_fingerprint or "").strip()
        render_hash = str(render_result_hash or "").strip()
        if int(prompt_version_id) <= 0 or not binding or not render_hash:
            return None
        return _row_to_dict(
            self._conn.execute(
                """
                SELECT * FROM prompt_runtime_render_validation_ledger
                WHERE prompt_version_id=? AND binding_fingerprint=? AND render_result_hash=?
                ORDER BY validated_at DESC, id DESC
                LIMIT 1
                """,
                (int(prompt_version_id), binding, render_hash),
            ).fetchone()
        )

    def evaluate(self, *, prompt_version_id: int, binding_fingerprint: str, render_result_hash: str) -> RenderValidationEvaluation:
        latest = self.get_latest_validation(prompt_version_id=prompt_version_id, binding_fingerprint=binding_fingerprint, render_result_hash=render_result_hash)
        binding = str(binding_fingerprint or "").strip()
        render_hash = str(render_result_hash or "").strip()
        if latest is None:
            return RenderValidationEvaluation(
                int(prompt_version_id or 0),
                binding,
                render_hash,
                "missing",
                False,
                None,
                "missing_render_validation_authority",
                {"source_table": "prompt_runtime_render_validation_ledger"},
            )
        status = str(latest.get("validation_status") or "")
        superseded_by = latest.get("superseded_by_validation_id")
        if status == "passed" and superseded_by is None:
            verdict = "trusted"
            trusted = True
            reason = None
        elif superseded_by is not None:
            verdict = "untrusted"
            trusted = False
            reason = "render_validation_superseded"
        else:
            verdict = "untrusted"
            trusted = False
            reason = f"render_validation_{status or 'invalid'}"
        return RenderValidationEvaluation(
            int(prompt_version_id),
            binding,
            render_hash,
            verdict,
            trusted,
            latest,
            reason,
            {
                "source_table": "prompt_runtime_render_validation_ledger",
                "source_id": latest.get("id"),
                "validated_at": latest.get("validated_at"),
            },
        )

    def list_validations(
        self,
        *,
        prompt_version_id: int | None = None,
        binding_fingerprint: str | None = None,
        render_result_hash: str | None = None,
        validation_status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if prompt_version_id is not None:
            clauses.append("prompt_version_id=?")
            params.append(int(prompt_version_id))
        if binding_fingerprint:
            clauses.append("binding_fingerprint=?")
            params.append(str(binding_fingerprint))
        if render_result_hash:
            clauses.append("render_result_hash=?")
            params.append(str(render_result_hash))
        if validation_status:
            clauses.append("validation_status=?")
            params.append(validate_render_validation_status(str(validation_status)))
        safe_limit = min(max(int(limit or 100), 1), 500)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM prompt_runtime_render_validation_ledger{where} ORDER BY validated_at DESC, id DESC LIMIT ?",
            tuple(params + [safe_limit]),
        ).fetchall()
        return [row for row in map(_row_to_dict, rows) if row is not None]

COMPATIBILITY_STATUSES: tuple[str, ...] = ("allowed", "blocked", "deprecated")


def validate_compatibility_status(value: str) -> str:
    normalized = str(value or "").strip()
    if normalized not in COMPATIBILITY_STATUSES:
        raise ValueError(f"invalid compatibility_status: {normalized}")
    return normalized


def _normalized_authority_key(value: str, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


@dataclass(frozen=True)
class TargetResolverEvaluation:
    capability_code: str
    target_type: str
    exists: bool
    resolver_code: str | None
    snapshot_schema_version: str | None
    is_enabled: bool
    admissible: bool
    failure_reason_code: str | None
    authoritative_source_metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "capability_code": self.capability_code,
            "target_type": self.target_type,
            "exists": self.exists,
            "resolver_code": self.resolver_code,
            "snapshot_schema_version": self.snapshot_schema_version,
            "is_enabled": self.is_enabled,
            "admissible": self.admissible,
            "failure_reason_code": self.failure_reason_code,
            "authoritative_source_metadata": self.authoritative_source_metadata,
        }


@dataclass(frozen=True)
class TargetCompatibilityEvaluation:
    capability_code: str
    target_type: str
    exists: bool
    compatibility_status: str | None
    policy_code: str | None
    admissible: bool
    failure_reason_code: str | None
    authoritative_source_metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "capability_code": self.capability_code,
            "target_type": self.target_type,
            "exists": self.exists,
            "compatibility_status": self.compatibility_status,
            "policy_code": self.policy_code,
            "admissible": self.admissible,
            "failure_reason_code": self.failure_reason_code,
            "authoritative_source_metadata": self.authoritative_source_metadata,
        }


class TargetResolverRegistryService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get_row(self, capability_code: str, target_type: str) -> dict[str, Any] | None:
        capability = str(capability_code or "").strip()
        target = str(target_type or "").strip()
        if not capability or not target:
            return None
        return _row_to_dict(
            self._conn.execute(
                "SELECT * FROM prompt_runtime_target_resolver_registry WHERE capability_code=? AND target_type=?",
                (capability, target),
            ).fetchone()
        )

    def evaluate(self, capability_code: str, target_type: str) -> TargetResolverEvaluation:
        capability = str(capability_code or "").strip()
        target = str(target_type or "").strip()
        row = self.get_row(capability, target)
        if row is None:
            return TargetResolverEvaluation(
                capability,
                target,
                False,
                None,
                None,
                False,
                False,
                "missing_target_resolver_authority",
                {"source_table": "prompt_runtime_target_resolver_registry"},
            )
        resolver_code = str(row.get("resolver_code") or "").strip()
        snapshot_schema_version = str(row.get("snapshot_schema_version") or "").strip()
        is_enabled = bool(int(row.get("is_enabled") or 0))
        if not is_enabled:
            reason = "target_resolver_disabled"
        elif not resolver_code:
            reason = "target_resolver_code_missing"
        elif not snapshot_schema_version:
            reason = "target_resolver_snapshot_schema_missing"
        else:
            reason = None
        return TargetResolverEvaluation(
            capability,
            target,
            True,
            resolver_code or None,
            snapshot_schema_version or None,
            is_enabled,
            reason is None,
            reason,
            {
                "source_table": "prompt_runtime_target_resolver_registry",
                "source_id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "updated_by_operator": row.get("updated_by_operator"),
            },
        )

    def upsert(self, capability_code: str, target_type: str, payload: dict[str, Any], *, updated_by_operator: str) -> dict[str, Any]:
        capability = _normalized_authority_key(capability_code, "capability_code")
        target = _normalized_authority_key(target_type, "target_type")
        resolver_code = _normalized_authority_key(str(payload.get("resolver_code") or ""), "resolver_code")
        snapshot_schema_version = _normalized_authority_key(str(payload.get("snapshot_schema_version") or ""), "snapshot_schema_version")
        notes = payload.get("notes")
        now = utc_now_iso()
        self._conn.execute(
            """
            INSERT INTO prompt_runtime_target_resolver_registry(
                capability_code,target_type,resolver_code,snapshot_schema_version,is_enabled,notes,updated_by_operator,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(capability_code,target_type) DO UPDATE SET
                resolver_code=excluded.resolver_code,
                snapshot_schema_version=excluded.snapshot_schema_version,
                is_enabled=excluded.is_enabled,
                notes=excluded.notes,
                updated_by_operator=excluded.updated_by_operator,
                updated_at=excluded.updated_at
            """,
            (capability, target, resolver_code, snapshot_schema_version, _bool_int(payload.get("is_enabled")), None if notes is None else str(notes), str(updated_by_operator), now, now),
        )
        row = self.get_row(capability, target)
        assert row is not None
        return row

    def list_rows(self, *, capability_code: str | None = None, target_type: str | None = None, is_enabled: bool | None = None, limit: int = 100) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if capability_code:
            clauses.append("capability_code=?")
            params.append(str(capability_code).strip())
        if target_type:
            clauses.append("target_type=?")
            params.append(str(target_type).strip())
        if is_enabled is not None:
            clauses.append("is_enabled=?")
            params.append(_bool_int(is_enabled))
        safe_limit = min(max(int(limit or 100), 1), 500)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM prompt_runtime_target_resolver_registry{where} ORDER BY capability_code ASC, target_type ASC LIMIT ?",
            tuple(params + [safe_limit]),
        ).fetchall()
        return [row for row in map(_row_to_dict, rows) if row is not None]


class TargetCompatibilityService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def get_row(self, capability_code: str, target_type: str) -> dict[str, Any] | None:
        capability = str(capability_code or "").strip()
        target = str(target_type or "").strip()
        if not capability or not target:
            return None
        return _row_to_dict(
            self._conn.execute(
                "SELECT * FROM prompt_runtime_target_compatibility_policy WHERE capability_code=? AND target_type=?",
                (capability, target),
            ).fetchone()
        )

    def evaluate(self, capability_code: str, target_type: str) -> TargetCompatibilityEvaluation:
        capability = str(capability_code or "").strip()
        target = str(target_type or "").strip()
        row = self.get_row(capability, target)
        if row is None:
            return TargetCompatibilityEvaluation(
                capability,
                target,
                False,
                None,
                None,
                False,
                "missing_target_compatibility_authority",
                {"source_table": "prompt_runtime_target_compatibility_policy"},
            )
        compatibility_status = str(row.get("compatibility_status") or "").strip()
        policy_code = str(row.get("policy_code") or "").strip()
        if compatibility_status != "allowed":
            reason = f"target_compatibility_{compatibility_status or 'invalid'}"
        elif not policy_code:
            reason = "target_compatibility_policy_code_missing"
        else:
            reason = None
        return TargetCompatibilityEvaluation(
            capability,
            target,
            True,
            compatibility_status or None,
            policy_code or None,
            reason is None,
            reason,
            {
                "source_table": "prompt_runtime_target_compatibility_policy",
                "source_id": row.get("id"),
                "updated_at": row.get("updated_at"),
                "updated_by_operator": row.get("updated_by_operator"),
            },
        )

    def upsert(self, capability_code: str, target_type: str, payload: dict[str, Any], *, updated_by_operator: str) -> dict[str, Any]:
        capability = _normalized_authority_key(capability_code, "capability_code")
        target = _normalized_authority_key(target_type, "target_type")
        compatibility_status = validate_compatibility_status(str(payload.get("compatibility_status") or ""))
        policy_code = _normalized_authority_key(str(payload.get("policy_code") or ""), "policy_code")
        notes = payload.get("notes")
        now = utc_now_iso()
        self._conn.execute(
            """
            INSERT INTO prompt_runtime_target_compatibility_policy(
                capability_code,target_type,compatibility_status,policy_code,notes,updated_by_operator,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(capability_code,target_type) DO UPDATE SET
                compatibility_status=excluded.compatibility_status,
                policy_code=excluded.policy_code,
                notes=excluded.notes,
                updated_by_operator=excluded.updated_by_operator,
                updated_at=excluded.updated_at
            """,
            (capability, target, compatibility_status, policy_code, None if notes is None else str(notes), str(updated_by_operator), now, now),
        )
        row = self.get_row(capability, target)
        assert row is not None
        return row

    def list_rows(
        self,
        *,
        capability_code: str | None = None,
        target_type: str | None = None,
        compatibility_status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if capability_code:
            clauses.append("capability_code=?")
            params.append(str(capability_code).strip())
        if target_type:
            clauses.append("target_type=?")
            params.append(str(target_type).strip())
        if compatibility_status:
            clauses.append("compatibility_status=?")
            params.append(validate_compatibility_status(str(compatibility_status)))
        safe_limit = min(max(int(limit or 100), 1), 500)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM prompt_runtime_target_compatibility_policy{where} ORDER BY capability_code ASC, target_type ASC LIMIT ?",
            tuple(params + [safe_limit]),
        ).fetchall()
        return [row for row in map(_row_to_dict, rows) if row is not None]
