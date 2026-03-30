from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.common import db as dbm
from services.common.env import Env
from services.bot.telegram_publish_formatting import format_critical_event_message


def _state_path(env: Env) -> Path:
    return Path(env.storage_root).resolve() / "state" / "telegram_publish_notifications.json"


def _load_state(env: Env) -> dict[str, Any]:
    p = _state_path(env)
    if not p.exists():
        return {"seen": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"seen": {}}


def _save_state(env: Env, state: dict[str, Any]) -> None:
    p = _state_path(env)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def _event_key(family: str, row: Any, occurred_at: float) -> str:
    return f"{family}:{int(row['id'])}:{occurred_at:.3f}"


def _collect_candidates(conn: Any, *, now_ts: float) -> list[tuple[str, Any, float]]:
    rows = conn.execute(
        """
        SELECT id, publish_state, publish_reason_code, publish_last_transition_at, publish_scheduled_at,
               publish_drift_detected_at, publish_last_error_code, publish_last_error_message
        FROM jobs
        WHERE publish_state IN (
            'policy_blocked',
            'published_public',
            'published_unlisted',
            'publish_failed_terminal',
            'manual_handoff_pending',
            'waiting_for_schedule',
            'publish_state_drift_detected'
        )
        """
    ).fetchall()

    out: list[tuple[str, Any, float]] = []
    for row in rows:
        state = str(row["publish_state"])
        transition_ts = float(row["publish_last_transition_at"] or 0.0)
        reason_code = str(row["publish_reason_code"] or "")
        if state == "policy_blocked":
            out.append(("policy block", row, transition_ts))
        elif state in {"published_public", "published_unlisted"}:
            out.append(("publish success", row, transition_ts))
        elif state == "publish_failed_terminal":
            out.append(("publish failed", row, transition_ts))
        elif state == "manual_handoff_pending" and reason_code == "retries_exhausted":
            out.append(("retries exhausted", row, transition_ts))
        elif state == "manual_handoff_pending":
            out.append(("manual handoff required", row, transition_ts))
        elif state == "waiting_for_schedule" and float(row["publish_scheduled_at"] or 0) < now_ts:
            occurred = float(row["publish_scheduled_at"] or transition_ts)
            out.append(("missed schedule", row, occurred))
        elif state == "publish_state_drift_detected" or row["publish_drift_detected_at"] is not None:
            occurred = float(row["publish_drift_detected_at"] or transition_ts)
            out.append(("drift detected", row, occurred))

    control = conn.execute(
        "SELECT auto_publish_paused, reason, updated_at FROM publish_global_controls WHERE singleton_key = 1"
    ).fetchone()
    if control is not None and control["updated_at"] is not None:
        family = "critical global pause" if int(control["auto_publish_paused"] or 0) == 1 else "critical global unblock"
        pseudo = {"id": 0, "publish_state": "policy_blocked", "publish_reason_code": str(control["reason"] or "")}
        out.append((family, pseudo, float(control["updated_at"])))

    return out


async def send_critical_publish_notifications(*, bot: Any, env: Env) -> int:
    state = _load_state(env)
    seen = set(str(item) for item in state.get("seen", {}).keys())
    now_ts = dbm.now_ts()
    conn = dbm.connect(env)
    try:
        candidates = _collect_candidates(conn, now_ts=now_ts)
    finally:
        conn.close()

    sent = 0
    for family, row, occurred_at in candidates:
        key = _event_key(family, row, occurred_at)
        if key in seen:
            continue
        body = format_critical_event_message(family=family, item={
            "job_id": int(row["id"]),
            "publish_state": row.get("publish_state"),
            "publish_reason_code": row.get("publish_reason_code"),
        })
        await bot.send_message(chat_id=env.tg_admin_chat_id, text=body)
        state.setdefault("seen", {})[key] = now_ts
        sent += 1

    _save_state(env, state)
    return sent


__all__ = ["send_critical_publish_notifications"]
