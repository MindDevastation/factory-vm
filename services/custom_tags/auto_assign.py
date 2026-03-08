from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


_SUPPORTED_OPERATORS = {
    "equals",
    "not_equals",
    "gt",
    "gte",
    "lt",
    "lte",
    "contains",
    "in",
    "between",
}


def _now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_source_path(payload: dict[str, Any], source_path: str) -> Any:
    node: Any = payload
    for part in source_path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _coerce_numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _contains(container: Any, item: Any) -> bool:
    if isinstance(container, str):
        return str(item) in container
    if isinstance(container, list):
        return item in container
    return False


def _rule_matches(actual: Any, operator: str, expected: Any) -> bool:
    if operator == "equals":
        return actual == expected
    if operator == "not_equals":
        return actual != expected
    if operator in {"gt", "gte", "lt", "lte"}:
        lhs = _coerce_numeric(actual)
        rhs = _coerce_numeric(expected)
        if lhs is None or rhs is None:
            return False
        if operator == "gt":
            return lhs > rhs
        if operator == "gte":
            return lhs >= rhs
        if operator == "lt":
            return lhs < rhs
        return lhs <= rhs
    if operator == "contains":
        return _contains(actual, expected)
    if operator == "in":
        if not isinstance(expected, list):
            return False
        return actual in expected
    if operator == "between":
        if not isinstance(expected, list) or len(expected) != 2:
            return False
        lhs = _coerce_numeric(actual)
        lo = _coerce_numeric(expected[0])
        hi = _coerce_numeric(expected[1])
        if lhs is None or lo is None or hi is None:
            return False
        return lo <= lhs <= hi
    return False


def _tag_is_candidate(tag_row: dict[str, Any], rules: list[dict[str, Any]], analyzer_payload: dict[str, Any]) -> bool:
    if str(tag_row["category"]) == "VISUAL":
        if not bool(tag_row["is_channel_bound"]):
            return False

    if not rules:
        return False

    required_results: list[bool] = []
    all_results: list[bool] = []
    any_results: list[bool] = []

    for rule in rules:
        source_path = str(rule["source_path"])
        actual = _resolve_source_path(analyzer_payload, source_path)
        try:
            expected = json.loads(str(rule["value_json"]))
        except json.JSONDecodeError:
            matched = False
        else:
            operator = str(rule["operator"])
            if operator not in _SUPPORTED_OPERATORS:
                matched = False
            else:
                matched = _rule_matches(actual, operator, expected)

        if bool(rule["required"]):
            required_results.append(matched)

        mode = str(rule["match_mode"])
        if mode == "ANY":
            any_results.append(matched)
        else:
            all_results.append(matched)

    required_ok = all(required_results) if required_results else True
    all_ok = all(all_results) if all_results else True
    any_ok = any(any_results) if any_results else True
    return required_ok and all_ok and any_ok


def apply_auto_custom_tags(conn: sqlite3.Connection, track_pk: int, analyzer_payload: dict[str, Any]) -> dict[str, Any]:
    track = conn.execute("SELECT id, channel_slug FROM tracks WHERE id = ?", (track_pk,)).fetchone()
    if track is None:
        return {
            "track_pk": str(track_pk),
            "auto_added": [],
            "auto_removed": [],
            "preserved_manual": [],
            "suppressed_skipped": [],
        }

    tag_rows = conn.execute(
        """
        SELECT t.id, t.category,
               EXISTS(
                   SELECT 1
                   FROM custom_tag_channel_bindings b
                   WHERE b.tag_id = t.id
                     AND b.channel_slug = ?
               ) AS is_channel_bound
        FROM custom_tags t
        WHERE t.is_active = 1
        ORDER BY t.id ASC
        """,
        (str(track["channel_slug"]),),
    ).fetchall()
    if not tag_rows:
        return {
            "track_pk": str(track_pk),
            "auto_added": [],
            "auto_removed": [],
            "preserved_manual": [],
            "suppressed_skipped": [],
        }

    tag_ids = [int(row["id"]) for row in tag_rows]
    placeholders = ",".join("?" for _ in tag_ids)
    rules_rows = conn.execute(
        f"""
        SELECT id, tag_id, source_path, operator, value_json, match_mode, required
        FROM custom_tag_rules
        WHERE is_active = 1 AND tag_id IN ({placeholders})
        ORDER BY priority DESC, id ASC
        """,
        tag_ids,
    ).fetchall()

    rules_by_tag: dict[int, list[dict[str, Any]]] = {tag_id: [] for tag_id in tag_ids}
    for row in rules_rows:
        rules_by_tag[int(row["tag_id"])].append(row)

    candidate_ids: set[int] = set()
    for tag_row in tag_rows:
        tag_id = int(tag_row["id"])
        if _tag_is_candidate(tag_row, rules_by_tag.get(tag_id, []), analyzer_payload):
            candidate_ids.add(tag_id)

    existing_rows = conn.execute(
        """
        SELECT id, tag_id, state
        FROM track_custom_tag_assignments
        WHERE track_pk = ?
        """,
        (track_pk,),
    ).fetchall()
    existing_by_tag = {int(row["tag_id"]): row for row in existing_rows}

    auto_added: list[int] = []
    auto_removed: list[int] = []
    preserved_manual: list[int] = []
    suppressed_skipped: list[int] = []

    now_text = _now_text()

    for tag_id in sorted(candidate_ids):
        existing = existing_by_tag.get(tag_id)
        if existing is None:
            conn.execute(
                """
                INSERT INTO track_custom_tag_assignments(track_pk, tag_id, state, assigned_at, updated_at)
                VALUES(?,?,?,?,?)
                """,
                (track_pk, tag_id, "AUTO", now_text, now_text),
            )
            auto_added.append(tag_id)
            continue

        state = str(existing["state"])
        if state == "MANUAL":
            preserved_manual.append(tag_id)
        elif state == "SUPPRESSED":
            suppressed_skipped.append(tag_id)

    for row in existing_rows:
        tag_id = int(row["tag_id"])
        state = str(row["state"])
        if tag_id in candidate_ids:
            continue
        if state == "AUTO":
            conn.execute("DELETE FROM track_custom_tag_assignments WHERE id = ?", (int(row["id"]),))
            auto_removed.append(tag_id)

    return {
        "track_pk": str(track_pk),
        "auto_added": auto_added,
        "auto_removed": auto_removed,
        "preserved_manual": sorted(set(preserved_manual)),
        "suppressed_skipped": sorted(set(suppressed_skipped)),
    }
