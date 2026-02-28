from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DbViewerPolicyError(RuntimeError):
    """Policy storage/configuration failure for DB Viewer policy endpoints."""


def empty_policy() -> dict[str, Any]:
    return {
        "denylist_tables": [],
        "human_name_overrides": {},
    }


def _require_safe_identifier(value: str, field: str) -> None:
    if not SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(f"{field} contains invalid identifier: {value}")


def validate_denylist_tables(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ValueError("denylist_tables must be a list")

    seen: set[str] = set()
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("denylist_tables items must be strings")
        _require_safe_identifier(item, "denylist_tables")
        if item in seen:
            raise ValueError(f"denylist_tables contains duplicate value: {item}")
        seen.add(item)
        out.append(item)
    return out


def validate_human_name_overrides(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("human_name_overrides must be an object")

    out: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("human_name_overrides keys must be strings")
        _require_safe_identifier(key, "human_name_overrides")
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"human_name_overrides[{key}] must be a non-empty string")
        out[key] = item
    return out


def validate_policy_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("policy payload must be an object")

    denylist_tables = validate_denylist_tables(payload.get("denylist_tables", []))
    human_name_overrides = validate_human_name_overrides(payload.get("human_name_overrides", {}))
    return {
        "denylist_tables": denylist_tables,
        "human_name_overrides": human_name_overrides,
    }


def parse_privileged_users(users_csv: str) -> set[str]:
    if not users_csv:
        return set()
    return {item.strip() for item in users_csv.split(",") if item.strip()}


def is_privileged(username: str, env: Any) -> bool:
    return username in parse_privileged_users(getattr(env, "db_viewer_privileged_users", ""))


def _policy_path(env: Any) -> Path:
    policy_path = getattr(env, "db_viewer_policy_path", "")
    if not policy_path:
        raise DbViewerPolicyError("Policy storage is not configured")
    return Path(policy_path)


def load_policy(env: Any) -> dict[str, Any]:
    policy_path = getattr(env, "db_viewer_policy_path", "")
    if not policy_path:
        return empty_policy()

    path = Path(policy_path)
    if not path.exists():
        return empty_policy()

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DbViewerPolicyError(f"Failed to read policy from {path}: {exc}") from exc

    try:
        return validate_policy_payload(raw)
    except ValueError as exc:
        raise DbViewerPolicyError(f"Invalid policy in {path}: {exc}") from exc


def save_policy(env: Any, payload: Any) -> dict[str, Any]:
    path = _policy_path(env)
    normalized = validate_policy_payload(payload)
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return normalized
