from __future__ import annotations

from typing import Any

from .meta import is_text_declared_type

SECRET_NAME_TOKENS = ("token", "secret", "password", "oauth", "key", "credential")


def is_secret_name(value: str) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in SECRET_NAME_TOKENS)


def filter_allowed_tables(existing_tables: list[str], denylist_tables: list[str]) -> list[str]:
    denied = set(denylist_tables)
    return [table for table in existing_tables if not is_secret_name(table) and table not in denied]


def filter_visible_columns(columns: list[str]) -> list[str]:
    return [column for column in columns if not is_secret_name(column)]


def detect_text_columns(columns: list[dict[str, Any]]) -> set[str]:
    return {
        str(col["name"])
        for col in columns
        if isinstance(col, dict)
        and isinstance(col.get("name"), str)
        and is_text_declared_type(col.get("declared_type") if isinstance(col.get("declared_type"), str) else None)
    }


def make_human_table_name(table_name: str, overrides: dict[str, str] | None = None) -> str:
    if overrides and table_name in overrides:
        return overrides[table_name]
    words = [part for part in table_name.strip().split("_") if part]
    return " ".join(word.capitalize() for word in words) if words else table_name
