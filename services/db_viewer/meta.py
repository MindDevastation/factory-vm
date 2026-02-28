from __future__ import annotations

import re
import sqlite3
from typing import Any

SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def is_safe_identifier(value: str) -> bool:
    return bool(SAFE_IDENTIFIER_RE.fullmatch(value))


def list_existing_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name COLLATE NOCASE"
    ).fetchall()
    out: list[str] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            name = row["name"]
        elif isinstance(row, dict):
            name = row.get("name")
        elif isinstance(row, (tuple, list)) and row:
            name = row[0]
        else:
            name = None
        if isinstance(name, str):
            out.append(name)
    return out


def list_table_columns(conn: sqlite3.Connection, table_name: str) -> list[dict[str, Any]]:
    if not is_safe_identifier(table_name):
        raise ValueError(f"invalid table identifier: {table_name}")

    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            col_name = row["name"]
            declared_type = row["type"]
        elif isinstance(row, dict):
            col_name = row.get("name")
            declared_type = row.get("type")
        elif isinstance(row, (tuple, list)) and len(row) > 2:
            col_name = row[1]
            declared_type = row[2]
        else:
            col_name = None
            declared_type = None

        if isinstance(col_name, str):
            out.append({"name": col_name, "declared_type": declared_type if isinstance(declared_type, str) else ""})

    return out


def is_text_declared_type(declared_type: str | None) -> bool:
    if not declared_type:
        return False
    up = declared_type.upper()
    return any(token in up for token in ("CHAR", "CLOB", "TEXT", "VARCHAR"))
