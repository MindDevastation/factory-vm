from __future__ import annotations

import json
from typing import Any

from services.analytics_center.errors import AnalyticsDomainError, E5A_INVALID_PAYLOAD_JSON


def normalized_scope_identity(*, entity_type: str, entity_ref: str, source_family: str, window_type: str) -> str:
    return f"{entity_type.strip().upper()}::{entity_ref.strip()}::{source_family.strip().upper()}::{window_type.strip().upper()}"


def validate_json_payload(value: Any, *, field_name: str) -> str:
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise AnalyticsDomainError(code=E5A_INVALID_PAYLOAD_JSON, message=f"{field_name} must not be empty")
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError as exc:
            raise AnalyticsDomainError(code=E5A_INVALID_PAYLOAD_JSON, message=f"{field_name} must be valid JSON") from exc
        return json.dumps(parsed, sort_keys=True, separators=(",", ":"))

    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise AnalyticsDomainError(code=E5A_INVALID_PAYLOAD_JSON, message=f"{field_name} must be JSON-serializable") from exc


def supersede_existing_current_snapshot(conn: Any, *, normalized_scope_key: str, superseded_at: float) -> None:
    conn.execute(
        """
        UPDATE analytics_snapshots
        SET is_current = 0,
            snapshot_status = CASE
                WHEN snapshot_status = 'CURRENT' THEN 'SUPERSEDED'
                ELSE snapshot_status
            END,
            updated_at = ?
        WHERE normalized_scope_key = ?
          AND is_current = 1
        """,
        (superseded_at, normalized_scope_key),
    )
