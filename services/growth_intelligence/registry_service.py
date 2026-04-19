from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from services.growth_intelligence.contracts import (
    ensure_boolean_flag_map,
    ensure_impact_confidence,
    ensure_json_text,
    ensure_non_empty_text,
    ensure_source_class,
    ensure_source_hierarchy,
    ensure_source_trust,
)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class GrowthRegistryService:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def get_knowledge_item(self, item_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM growth_knowledge_items WHERE id = ?", (item_id,)).fetchone()
        if row is None:
            raise ValueError(f"knowledge item {item_id} not found")
        return row

    def list_knowledge_items(self, *, source_class: str | None = None, source_trust: str | None = None, status: str | None = None, q: str | None = None) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if source_class:
            where.append("source_class = ?")
            params.append(ensure_source_class(source_class))
        if source_trust:
            where.append("source_trust = ?")
            params.append(ensure_source_trust(source_trust))
        if status:
            where.append("status = ?")
            params.append(str(status).strip())
        if q:
            query = f"%{str(q).strip().lower()}%"
            where.append("(LOWER(code) LIKE ? OR LOWER(title) LIKE ? OR LOWER(description) LIKE ?)")
            params.extend([query, query, query])
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        return list(
            self._conn.execute(
                f"SELECT * FROM growth_knowledge_items {where_sql} ORDER BY updated_at DESC, id DESC",
                tuple(params),
            ).fetchall()
        )

    def create_knowledge_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = self._now_iso()
        code = ensure_non_empty_text(payload.get("code"), field_name="code")
        title = ensure_non_empty_text(payload.get("title"), field_name="title")
        source_type = ensure_non_empty_text(payload.get("source_type"), field_name="source_type")
        source_name = ensure_non_empty_text(payload.get("source_name"), field_name="source_name")
        action_template = ensure_non_empty_text(payload.get("action_template"), field_name="action_template")
        explanation_template = ensure_non_empty_text(payload.get("explanation_template"), field_name="explanation_template")
        status = ensure_non_empty_text(payload.get("status"), field_name="status")
        source_class, source_trust = ensure_source_hierarchy(source_class=str(payload.get("source_class", "")), source_trust=str(payload.get("source_trust", "")))
        impact_confidence = ensure_impact_confidence(str(payload.get("impact_confidence", "")))
        try:
            cur = self._conn.execute(
                """
                INSERT INTO growth_knowledge_items(
                    code,title,description,source_type,source_name,source_trust,impact_confidence,
                    applicable_profiles_json,applicable_metrics_json,trigger_conditions_json,
                    action_template,explanation_template,status,source_url,source_class,evidence_note,
                    reviewed_by,reviewed_at,supersedes_item_id,invalidated_at,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    code,
                    title,
                    str(payload.get("description", "")).strip(),
                    source_type,
                    source_name,
                    source_trust,
                    impact_confidence,
                    ensure_json_text(payload.get("applicable_profiles_json"), field_name="applicable_profiles_json", default=[]),
                    ensure_json_text(payload.get("applicable_metrics_json"), field_name="applicable_metrics_json", default=[]),
                    ensure_json_text(payload.get("trigger_conditions_json"), field_name="trigger_conditions_json", default=[]),
                    action_template,
                    explanation_template,
                    status,
                    str(payload.get("source_url", "")).strip(),
                    source_class,
                    str(payload.get("evidence_note", "")).strip(),
                    _optional_text(payload.get("reviewed_by")),
                    _optional_text(payload.get("reviewed_at")),
                    payload.get("supersedes_item_id"),
                    _optional_text(payload.get("invalidated_at")),
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            if "growth_knowledge_items.code" in str(exc):
                raise ValueError(f"knowledge item with code {code} already exists") from None
            raise
        return self.get_knowledge_item(int(cur.lastrowid))

    def update_knowledge_item(self, item_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        current = self.get_knowledge_item(item_id)
        merged = dict(current)
        merged.update(payload)
        if "source_class" in payload or "source_trust" in payload:
            merged["source_class"], merged["source_trust"] = ensure_source_hierarchy(
                source_class=str(merged.get("source_class", "")), source_trust=str(merged.get("source_trust", ""))
            )
        if "impact_confidence" in payload:
            merged["impact_confidence"] = ensure_impact_confidence(str(merged.get("impact_confidence", "")))
        for field in ("title", "source_type", "source_name", "action_template", "explanation_template", "status"):
            if field in payload:
                merged[field] = ensure_non_empty_text(merged.get(field), field_name=field)
        if not payload:
            return current
        serialized = dict(merged)
        for key in ("applicable_profiles_json", "applicable_metrics_json", "trigger_conditions_json"):
            serialized[key] = ensure_json_text(serialized.get(key), field_name=key, default=[])
        fields = (
            "title", "description", "source_type", "source_name", "source_trust", "impact_confidence",
            "applicable_profiles_json", "applicable_metrics_json", "trigger_conditions_json", "action_template",
            "explanation_template", "status", "source_url", "source_class", "evidence_note", "reviewed_by",
            "reviewed_at", "supersedes_item_id", "invalidated_at",
        )
        updates = {k: serialized[k] for k in fields if k in payload}
        if not updates:
            return current
        updates["updated_at"] = self._now_iso()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        self._conn.execute(f"UPDATE growth_knowledge_items SET {set_clause} WHERE id = ?", tuple(list(updates.values()) + [item_id]))
        return self.get_knowledge_item(item_id)

    def get_playbook(self, playbook_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM growth_playbooks WHERE id = ?", (playbook_id,)).fetchone()
        if row is None:
            raise ValueError(f"playbook {playbook_id} not found")
        return row

    def list_playbooks(self) -> list[dict[str, Any]]:
        return list(self._conn.execute("SELECT * FROM growth_playbooks ORDER BY updated_at DESC, id DESC").fetchall())

    def create_playbook(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = self._now_iso()
        code = ensure_non_empty_text(payload.get("code"), field_name="code")
        goal_type = ensure_non_empty_text(payload.get("goal_type"), field_name="goal_type")
        try:
            cur = self._conn.execute(
                """
                INSERT INTO growth_playbooks(code,goal_type,channel_types_json,release_types_json,activation_rules_json,output_shape_json,trust_policy_json,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    code,
                    goal_type,
                    ensure_json_text(payload.get("channel_types_json"), field_name="channel_types_json", default=[]),
                    ensure_json_text(payload.get("release_types_json"), field_name="release_types_json", default=[]),
                    ensure_json_text(payload.get("activation_rules_json"), field_name="activation_rules_json", default={}),
                    ensure_json_text(payload.get("output_shape_json"), field_name="output_shape_json", default={}),
                    ensure_json_text(payload.get("trust_policy_json"), field_name="trust_policy_json", default={}),
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError as exc:
            if "growth_playbooks.code" in str(exc):
                raise ValueError(f"playbook with code {code} already exists") from None
            raise
        return self.get_playbook(int(cur.lastrowid))

    def update_playbook(self, playbook_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        self.get_playbook(playbook_id)
        allowed = {"goal_type", "channel_types_json", "release_types_json", "activation_rules_json", "output_shape_json", "trust_policy_json"}
        updates = {k: payload[k] for k in payload if k in allowed}
        if "goal_type" in updates:
            updates["goal_type"] = ensure_non_empty_text(updates["goal_type"], field_name="goal_type")
        if not updates:
            return self.get_playbook(playbook_id)
        for key in ("channel_types_json", "release_types_json", "activation_rules_json", "output_shape_json", "trust_policy_json"):
            if key in updates:
                updates[key] = ensure_json_text(updates[key], field_name=key, default=[] if key.endswith("types_json") else {})
        updates["updated_at"] = self._now_iso()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        self._conn.execute(f"UPDATE growth_playbooks SET {set_clause} WHERE id = ?", tuple(list(updates.values()) + [playbook_id]))
        return self.get_playbook(playbook_id)

    def get_channel_feature_flags(self, channel_slug: str) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM growth_channel_feature_flags WHERE channel_slug = ?", (channel_slug,)).fetchone()
        if row:
            return row
        now = self._now_iso()
        return {"channel_slug": channel_slug, "growth_intelligence_enabled": 0, "planning_digest_enabled": 0, "planner_handoff_enabled": 0, "export_enabled": 0, "assisted_planning_enabled": 0, "created_at": now, "updated_at": now}

    def set_channel_feature_flags(self, channel_slug: str, payload: dict[str, Any]) -> dict[str, Any]:
        flags = ensure_boolean_flag_map(payload)
        existing_channel = self._conn.execute("SELECT slug FROM channels WHERE slug = ?", (channel_slug,)).fetchone()
        if existing_channel is None:
            raise ValueError(f"channel {channel_slug} not found")
        now = self._now_iso()
        try:
            self._conn.execute(
                """
                INSERT INTO growth_channel_feature_flags(channel_slug,growth_intelligence_enabled,planning_digest_enabled,planner_handoff_enabled,export_enabled,assisted_planning_enabled,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(channel_slug) DO UPDATE SET
                    growth_intelligence_enabled=excluded.growth_intelligence_enabled,
                    planning_digest_enabled=excluded.planning_digest_enabled,
                    planner_handoff_enabled=excluded.planner_handoff_enabled,
                    export_enabled=excluded.export_enabled,
                    assisted_planning_enabled=excluded.assisted_planning_enabled,
                    updated_at=excluded.updated_at
                """,
                (channel_slug, flags["growth_intelligence_enabled"], flags["planning_digest_enabled"], flags["planner_handoff_enabled"], flags["export_enabled"], flags["assisted_planning_enabled"], now, now),
            )
        except sqlite3.IntegrityError:
            raise ValueError("invalid channel slug") from None
        return self.get_channel_feature_flags(channel_slug)

    def bootstrap_import(self, payload: dict[str, Any]) -> dict[str, Any]:
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("bootstrap payload must contain non-empty items list")
        now = self._now_iso()
        run_id = int(
            self._conn.execute(
                """INSERT INTO growth_bootstrap_import_runs(import_source,import_mode,payload_fingerprint,status,created_count,updated_count,skipped_count,failed_count,created_at,completed_at,actor,notes_json)
                VALUES(?,?,?,'STARTED',0,0,0,0,?,NULL,?,?)""",
                (
                    str(payload.get("import_source", "curated")).strip() or "curated",
                    str(payload.get("import_mode", "upsert")).strip() or "upsert",
                    hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest(),
                    now,
                    _optional_text(payload.get("actor")),
                    ensure_json_text(payload.get("notes_json"), field_name="notes_json", default={}),
                ),
            ).lastrowid
        )
        created = updated = skipped = failed = 0
        item_results: list[dict[str, Any]] = []
        for raw in items:
            code = str((raw or {}).get("code", "")).strip() if isinstance(raw, dict) else ""
            title = str((raw or {}).get("title", "")).strip() if isinstance(raw, dict) else ""
            source_class = str((raw or {}).get("source_class", "")).strip() if isinstance(raw, dict) else ""
            try:
                if not isinstance(raw, dict):
                    raise ValueError("item must be object")
                if not code or not title:
                    raise ValueError("item code/title are required")
                existing = self._conn.execute("SELECT * FROM growth_knowledge_items WHERE code = ?", (code,)).fetchone()
                normalized = {
                    "code": code,
                    "title": title,
                    "description": str(raw.get("description", "")).strip(),
                    "source_type": str(raw.get("source_type", "")).strip(),
                    "source_name": str(raw.get("source_name", "")).strip(),
                    "source_trust": str(raw.get("source_trust", "")),
                    "impact_confidence": str(raw.get("impact_confidence", "")),
                    "applicable_profiles_json": raw.get("applicable_profiles_json"),
                    "applicable_metrics_json": raw.get("applicable_metrics_json"),
                    "trigger_conditions_json": raw.get("trigger_conditions_json"),
                    "action_template": str(raw.get("action_template", "")).strip(),
                    "explanation_template": str(raw.get("explanation_template", "")).strip(),
                    "status": str(raw.get("status", "ACTIVE")).strip() or "ACTIVE",
                    "source_url": str(raw.get("source_url", "")).strip(),
                    "source_class": source_class,
                    "evidence_note": str(raw.get("evidence_note", "")).strip(),
                    "reviewed_by": _optional_text(raw.get("reviewed_by")),
                    "reviewed_at": _optional_text(raw.get("reviewed_at")),
                    "supersedes_item_id": raw.get("supersedes_item_id"),
                    "invalidated_at": _optional_text(raw.get("invalidated_at")),
                }
                if existing is None:
                    self.create_knowledge_item(normalized)
                    created += 1
                    result_status, result_message = "CREATED", "created"
                else:
                    before = {k: existing[k] for k in normalized if k != "code"}
                    preview = dict(normalized)
                    sc, st = ensure_source_hierarchy(source_class=preview["source_class"], source_trust=preview["source_trust"])
                    preview["source_class"] = sc
                    preview["source_trust"] = st
                    preview["impact_confidence"] = ensure_impact_confidence(preview["impact_confidence"])
                    for key in ("applicable_profiles_json", "applicable_metrics_json", "trigger_conditions_json"):
                        preview[key] = ensure_json_text(preview[key], field_name=key, default=[])
                    changed = any(before[k] != preview[k] for k in before)
                    if changed:
                        self.update_knowledge_item(int(existing["id"]), normalized)
                        updated += 1
                        result_status, result_message = "UPDATED", "updated"
                    else:
                        skipped += 1
                        result_status, result_message = "SKIPPED", "no changes"
            except Exception as exc:
                failed += 1
                result_status, result_message = "FAILED", str(exc)
            self._conn.execute(
                "INSERT INTO growth_bootstrap_import_run_items(run_id,item_code,item_title,source_class,result_status,result_message,created_at) VALUES(?,?,?,?,?,?,?)",
                (run_id, code, title, source_class, result_status, result_message, self._now_iso()),
            )
            item_results.append({"item_code": code, "item_title": title, "result_status": result_status, "result_message": result_message})

        status = "FAILED" if failed else "COMPLETED"
        self._conn.execute(
            "UPDATE growth_bootstrap_import_runs SET status=?, created_count=?, updated_count=?, skipped_count=?, failed_count=?, completed_at=? WHERE id=?",
            (status, created, updated, skipped, failed, self._now_iso(), run_id),
        )
        return {"run_id": run_id, "status": status, "created": created, "updated": updated, "skipped": skipped, "failed": failed, "items": item_results}
