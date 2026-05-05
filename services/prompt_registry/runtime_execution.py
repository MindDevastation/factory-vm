from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone

from .runtime_adapters import RuntimeAdapterRegistry

SYNC_CAPABILITIES = frozenset({
    "CREATE_BULK_JSON_DRAFT",
    "CREATE_METADATA_REQUEST",
    "CREATE_VISUAL_REQUEST",
    "CREATE_ANALYTICS_REQUEST",
})
ASYNC_CAPABILITIES = frozenset({
    "ENQUEUE_INTERNAL_PROMPT_JOB",
    "GENERATE_OPERATOR_HANDOFF_EXPORT",
})
CAPABILITY_EXECUTION_MODE = {**{c: "SYNC" for c in SYNC_CAPABILITIES}, **{c: "ASYNC" for c in ASYNC_CAPABILITIES}}

ACTIVE_STATES = ("PREPARED", "CONFIRMATION_REQUIRED", "ADMITTED", "DISPATCHED", "RUNNING", "RETRY_PENDING")


_SECRET_PATTERNS = ("token", "secret", "password", "api_key", "apikey", "authorization", "bearer", "credential", "private_key")


def _contains_secret_like_text(value: str) -> bool:
    text = str(value or "").lower()
    return any(pattern in text for pattern in _SECRET_PATTERNS)


def _is_secret_safe_data(value) -> bool:
    if isinstance(value, dict):
        for k, v in value.items():
            if _contains_secret_like_text(str(k)):
                return False
            if not _is_secret_safe_data(v):
                return False
        return True
    if isinstance(value, (list, tuple)):
        return all(_is_secret_safe_data(v) for v in value)
    if isinstance(value, (str, bytes)):
        return not _contains_secret_like_text(value.decode("utf-8", "ignore") if isinstance(value, bytes) else value)
    return True


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compute_dedup_key_hash(*, capability_code: str, target_type: str, target_id: str | None, prompt_record_id: int, prompt_version_id: int | None, binding_resolution_fingerprint: str, rendered_payload_hash: str, action_payload_hash: str, reviewed_target_state_hash: str) -> str:
    material = "|".join([
        capability_code,
        target_type,
        target_id or "",
        str(prompt_record_id),
        "" if prompt_version_id is None else str(prompt_version_id),
        binding_resolution_fingerprint,
        rendered_payload_hash,
        action_payload_hash,
        reviewed_target_state_hash,
    ])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _make_confirmation_token(*, execution_attempt_id: int, dedup_key_hash: str, reviewed_target_state_hash: str, operator_id: str, state: str) -> str:
    material = "|".join([str(execution_attempt_id), dedup_key_hash, reviewed_target_state_hash, operator_id, state])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _validate_prompt(conn: sqlite3.Connection, prompt_record_id: int, prompt_version_id: int | None) -> None:
    rec = conn.execute("SELECT id,status,validation_status,active_version_id FROM prompt_records WHERE id=?", (prompt_record_id,)).fetchone()
    if rec is None or rec[1] != "active" or rec[2] != "VALID":
        raise ValueError("PREFLIGHT_REJECTED: prompt record is not executable")
    if prompt_version_id is None:
        prompt_version_id = rec[3]
    v = conn.execute("SELECT id,status,validation_status FROM prompt_versions WHERE id=? AND prompt_id=?", (prompt_version_id, prompt_record_id)).fetchone()
    if v is None or v[1] != "active" or v[2] != "VALID":
        raise ValueError("PREFLIGHT_REJECTED: prompt version is not executable")


def prepare_prompt_execution_preflight(conn: sqlite3.Connection, **kwargs) -> dict:
    capability_code = kwargs["capability_code"]
    if capability_code not in CAPABILITY_EXECUTION_MODE:
        raise ValueError("PREFLIGHT_REJECTED: forbidden capability")
    operator_id = str(kwargs.get("operator_id_or_system_actor") or "").strip()
    if not operator_id:
        raise ValueError("PREFLIGHT_REJECTED: operator identity required")
    if operator_id.startswith("forbidden:"):
        raise ValueError("PREFLIGHT_REJECTED: operator role incompatible")
    required = ["target_type", "binding_resolution_fingerprint", "rendered_payload_hash", "action_payload_hash", "reviewed_target_state_hash"]
    for key in required:
        if not str(kwargs.get(key) or "").strip():
            raise ValueError(f"PREFLIGHT_REJECTED: missing {key}")
    _validate_prompt(conn, int(kwargs["prompt_record_id"]), kwargs.get("prompt_version_id"))
    _write_runtime_audit(conn, prompt_record_id=int(kwargs["prompt_record_id"]), prompt_version_id=kwargs.get("prompt_version_id"), event_type="runtime_preflight_started", actor=operator_id, payload={"capability_code": capability_code, "target_type": kwargs["target_type"], "target_id": kwargs.get("target_id")})

    dedup_key_hash = compute_dedup_key_hash(**{k: kwargs.get(k) for k in ["capability_code", "target_type", "target_id", "prompt_record_id", "prompt_version_id", "binding_resolution_fingerprint", "rendered_payload_hash", "action_payload_hash", "reviewed_target_state_hash"]})
    now = _utc_now()
    existing = conn.execute("SELECT id,execution_group_id,state,operator_id_or_system_actor,reviewed_target_state_hash,dedup_key_hash FROM prompt_execution_attempts WHERE dedup_key_hash=? ORDER BY id DESC LIMIT 1", (dedup_key_hash,)).fetchone()
    if existing:
        token = _make_confirmation_token(execution_attempt_id=existing[0], dedup_key_hash=existing[5], reviewed_target_state_hash=existing[4], operator_id=existing[3], state=existing[2])
        return {"execution_group_id": existing[1], "execution_attempt_id": existing[0], "confirmation_token": token, "dedup_key_hash": existing[5], "state": existing[2], "execution_mode": CAPABILITY_EXECUTION_MODE[capability_code], "capability_code": capability_code, "target_type": kwargs["target_type"], "target_id": kwargs.get("target_id"), "secret_safe_message": "Ready for explicit confirmation."}

    conflict = conn.execute(f"SELECT id FROM prompt_execution_groups WHERE capability_code=? AND target_type=? AND COALESCE(target_id,'')=COALESCE(?, '') AND current_state IN ({','.join('?'*len(ACTIVE_STATES))}) ORDER BY id DESC LIMIT 1", (capability_code, kwargs["target_type"], kwargs.get("target_id"), *ACTIVE_STATES)).fetchone()
    if conflict:
        _write_lifecycle_event(conn, execution_group_id=int(conflict[0]), execution_attempt_id=None, state_before=None, state_after="CONFLICT_BLOCKED", result_code="CONFLICT", actor=operator_id, payload={})
        _write_runtime_audit(conn, prompt_record_id=int(kwargs["prompt_record_id"]), prompt_version_id=kwargs.get("prompt_version_id"), event_type="runtime_conflict_blocked", actor=operator_id, payload={"blocked_by_execution_group_id": int(conflict[0]), "dedup_key_hash": dedup_key_hash})
        conn.commit()
        return {"state": "CONFLICT_BLOCKED", "blocked_by_execution_group_id": int(conflict[0]), "dedup_key_hash": dedup_key_hash}

    mode = CAPABILITY_EXECUTION_MODE[capability_code]
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO prompt_execution_groups(capability_code,target_type,target_id,dedup_lineage_key,current_state,execution_mode,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", (capability_code, kwargs["target_type"], kwargs.get("target_id"), dedup_key_hash, "CONFIRMATION_REQUIRED", mode, now, now))
    except sqlite3.IntegrityError:
        conflict = conn.execute(f"SELECT id FROM prompt_execution_groups WHERE capability_code=? AND target_type=? AND COALESCE(target_id,'')=COALESCE(?, '') AND current_state IN ({','.join('?'*len(ACTIVE_STATES))}) ORDER BY id DESC LIMIT 1", (capability_code, kwargs["target_type"], kwargs.get("target_id"), *ACTIVE_STATES)).fetchone()
        if conflict:
            return {"state": "CONFLICT_BLOCKED", "blocked_by_execution_group_id": int(conflict[0]), "dedup_key_hash": dedup_key_hash}
        raise ValueError("CONFLICT_BLOCKED: active target lock collision")
    gid = int(cur.lastrowid)
    cur.execute("INSERT INTO prompt_execution_attempts(execution_group_id,attempt_number,state,correlation_id,operator_id_or_system_actor,prompt_record_id,prompt_version_id,binding_resolution_fingerprint,rendered_payload_hash,action_payload_hash,reviewed_target_state_hash,dedup_key_hash,retryable_by_operator,cancellable,execution_mode,dispatch_payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (gid, 1, "CONFIRMATION_REQUIRED", f"mf2:{gid}:1", operator_id, kwargs["prompt_record_id"], kwargs.get("prompt_version_id"), kwargs["binding_resolution_fingerprint"], kwargs["rendered_payload_hash"], kwargs["action_payload_hash"], kwargs["reviewed_target_state_hash"], dedup_key_hash, 0, 1, mode, "{}", now, now))
    aid = int(cur.lastrowid)
    conn.commit()
    _write_lifecycle_event(conn, execution_group_id=gid, execution_attempt_id=aid, state_before="PREPARED", state_after="CONFIRMATION_REQUIRED", result_code=None, actor=operator_id, payload={})
    _write_runtime_audit(conn, prompt_record_id=int(kwargs["prompt_record_id"]), prompt_version_id=kwargs.get("prompt_version_id"), event_type="runtime_confirmation_required", actor=operator_id, payload={"execution_group_id": gid, "execution_attempt_id": aid, "dedup_key_hash": dedup_key_hash})
    token = _make_confirmation_token(execution_attempt_id=aid, dedup_key_hash=dedup_key_hash, reviewed_target_state_hash=kwargs["reviewed_target_state_hash"], operator_id=operator_id, state="CONFIRMATION_REQUIRED")
    return {"execution_group_id": gid, "execution_attempt_id": aid, "confirmation_token": token, "dedup_key_hash": dedup_key_hash, "state": "CONFIRMATION_REQUIRED", "execution_mode": mode, "capability_code": capability_code, "target_type": kwargs["target_type"], "target_id": kwargs.get("target_id"), "secret_safe_message": "Ready for explicit confirmation."}


def confirm_prompt_execution(conn: sqlite3.Connection, *, execution_attempt_id: int, confirmation_token: str | None, operator_id_or_system_actor: str, reviewed_target_state_hash: str) -> dict:
    if not confirmation_token:
        raise ValueError("PREFLIGHT_REJECTED: confirmation token required")
    row = conn.execute("SELECT id,execution_group_id,state,dedup_key_hash,reviewed_target_state_hash,operator_id_or_system_actor FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    if row is None:
        raise ValueError("PREFLIGHT_REJECTED: attempt not found")
    if row[4] != reviewed_target_state_hash:
        _write_lifecycle_event(conn, execution_group_id=row[1], execution_attempt_id=row[0], state_before=row[2], state_after="STALE_BLOCKED", result_code="STALE", actor=operator_id_or_system_actor, payload={})
        _write_runtime_audit(conn, prompt_record_id=conn.execute("SELECT prompt_record_id FROM prompt_execution_attempts WHERE id=?", (row[0],)).fetchone()[0], prompt_version_id=conn.execute("SELECT prompt_version_id FROM prompt_execution_attempts WHERE id=?", (row[0],)).fetchone()[0], event_type="runtime_stale_blocked", actor=operator_id_or_system_actor, payload={"execution_group_id": row[1], "execution_attempt_id": row[0]})
        conn.commit()
        return {"state": "STALE_BLOCKED", "execution_group_id": row[1], "execution_attempt_id": row[0]}
    expected_current = _make_confirmation_token(execution_attempt_id=row[0], dedup_key_hash=row[3], reviewed_target_state_hash=row[4], operator_id=operator_id_or_system_actor, state=row[2])
    expected_initial = _make_confirmation_token(execution_attempt_id=row[0], dedup_key_hash=row[3], reviewed_target_state_hash=row[4], operator_id=operator_id_or_system_actor, state="CONFIRMATION_REQUIRED")
    if confirmation_token not in {expected_current, expected_initial}:
        raise ValueError("PREFLIGHT_REJECTED: invalid confirmation token")
    if row[2] == "ADMITTED":
        return {"state": "ADMITTED", "execution_group_id": row[1], "execution_attempt_id": row[0], "dedup_key_hash": row[3]}
    if row[2] != "CONFIRMATION_REQUIRED":
        raise ValueError("PREFLIGHT_REJECTED: confirmation state invalid")
    now = _utc_now()
    conn.execute("UPDATE prompt_execution_attempts SET state='ADMITTED',admitted_at=?,updated_at=? WHERE id=?", (now, now, row[0]))
    conn.execute("UPDATE prompt_execution_groups SET current_state='ADMITTED',updated_at=? WHERE id=?", (now, row[1]))
    _write_lifecycle_event(conn, execution_group_id=row[1], execution_attempt_id=row[0], state_before="CONFIRMATION_REQUIRED", state_after="ADMITTED", result_code=None, actor=operator_id_or_system_actor, payload={})
    pr = conn.execute("SELECT prompt_record_id,prompt_version_id,dedup_key_hash,correlation_id FROM prompt_execution_attempts WHERE id=?", (row[0],)).fetchone()
    g = conn.execute("SELECT capability_code,target_type,target_id FROM prompt_execution_groups WHERE id=?", (row[1],)).fetchone()
    _write_runtime_audit(conn, prompt_record_id=pr[0], prompt_version_id=pr[1], event_type="runtime_confirmed", actor=operator_id_or_system_actor, payload={"execution_group_id": row[1], "execution_attempt_id": row[0], "capability_code": g[0], "target_type": g[1], "target_id": g[2], "dedup_key_hash": pr[2], "correlation_id": pr[3], "state_before": "CONFIRMATION_REQUIRED", "state_after": "ADMITTED"})
    _write_runtime_audit(conn, prompt_record_id=pr[0], prompt_version_id=pr[1], event_type="runtime_admitted", actor=operator_id_or_system_actor, payload={"execution_group_id": row[1], "execution_attempt_id": row[0]})
    _ensure_usage_on_admitted(conn, execution_group_id=row[1], execution_attempt_id=row[0])
    conn.commit()
    return {"state": "ADMITTED", "execution_group_id": row[1], "execution_attempt_id": row[0], "dedup_key_hash": row[3]}


def dispatch_prompt_execution(conn: sqlite3.Connection, *, execution_attempt_id: int, adapter_registry: RuntimeAdapterRegistry, payload: dict | None = None) -> dict:
    row = conn.execute(
        """
        SELECT a.id,a.execution_group_id,a.state,a.execution_mode,a.secret_safe_message,a.dispatch_payload_json,
               g.capability_code,g.current_state
        FROM prompt_execution_attempts a
        JOIN prompt_execution_groups g ON g.id=a.execution_group_id
        WHERE a.id=?
        """,
        (execution_attempt_id,),
    ).fetchone()
    if row is None:
        raise ValueError("DISPATCH_REJECTED: attempt not found")
    if row[2] != "ADMITTED":
        raise ValueError("DISPATCH_REJECTED: non-admitted attempt")
    capability_code = row[6]
    expected_mode = CAPABILITY_EXECUTION_MODE.get(capability_code)
    if expected_mode is None:
        raise ValueError("DISPATCH_REJECTED: forbidden capability")
    if expected_mode != row[3]:
        raise ValueError("DISPATCH_REJECTED: execution mode mismatch")

    now = _utc_now()
    if expected_mode == "SYNC":
        adapter = adapter_registry.get(capability_code)
        if adapter is None:
            raise ValueError("DISPATCH_REJECTED: missing adapter")
        safe_payload = payload if isinstance(payload, dict) or payload is None else None
        if safe_payload is None or not _is_secret_safe_data(dict(safe_payload or {})):
            terminal_at = _utc_now()
            conn.execute("UPDATE prompt_execution_attempts SET state='FAILED_TERMINAL',result_code=?,secret_safe_message=?,terminal_at=?,updated_at=? WHERE id=?", ("SECRET_UNSAFE_PAYLOAD", "Adapter payload failed secret-safety precheck.", terminal_at, terminal_at, execution_attempt_id))
            conn.execute("UPDATE prompt_execution_groups SET current_state='FAILED_TERMINAL',updated_at=?,closed_at=? WHERE id=?", (terminal_at, terminal_at, row[1]))
            conn.commit()
            return {"state": "FAILED_TERMINAL", "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "result_code": "SECRET_UNSAFE_PAYLOAD", "secret_safe_message": "Adapter payload failed secret-safety precheck."}
        conn.execute("UPDATE prompt_execution_attempts SET state='RUNNING',running_at=?,updated_at=? WHERE id=?", (now, now, execution_attempt_id))
        conn.execute("UPDATE prompt_execution_groups SET current_state='RUNNING',updated_at=? WHERE id=?", (now, row[1]))
        _write_lifecycle_event(conn, execution_group_id=row[1], execution_attempt_id=execution_attempt_id, state_before="ADMITTED", state_after="RUNNING", result_code=None, actor="system", payload={})
        pr = conn.execute("SELECT prompt_record_id,prompt_version_id,correlation_id,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
        _write_runtime_audit(conn, prompt_record_id=pr[0], prompt_version_id=pr[1], event_type="runtime_running", actor="system", payload={"execution_group_id": row[1], "execution_attempt_id": execution_attempt_id, "correlation_id": pr[2], "dedup_key_hash": pr[3]})
        try:
            out = adapter(dict(safe_payload or {}))
            result_code = str((out or {}).get("result_code", "OK"))
            message = str((out or {}).get("secret_safe_message", "Sync dispatch completed."))
            if _contains_secret_like_text(result_code) or _contains_secret_like_text(message):
                result_code = "SECRET_UNSAFE_ADAPTER_RESULT"
                message = "Adapter result failed secret-safety precheck."
                terminal_state = "FAILED_TERMINAL"
            else:
                terminal_state = "SUCCEEDED"
        except Exception:
            result_code = "ADAPTER_ERROR"
            message = "Adapter execution failed."
            terminal_state = "FAILED_TERMINAL"
        terminal_at = _utc_now()
        conn.execute("UPDATE prompt_execution_attempts SET state=?,result_code=?,secret_safe_message=?,terminal_at=?,updated_at=? WHERE id=?", (terminal_state, result_code, message, terminal_at, terminal_at, execution_attempt_id))
        conn.execute("UPDATE prompt_execution_groups SET current_state=?,updated_at=?,closed_at=? WHERE id=?", (terminal_state, terminal_at, terminal_at, row[1]))
        _write_lifecycle_event(conn, execution_group_id=row[1], execution_attempt_id=execution_attempt_id, state_before="RUNNING", state_after=terminal_state, result_code=result_code, actor="system", payload={})
        pr = conn.execute("SELECT prompt_record_id,prompt_version_id FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
        evt = "runtime_succeeded" if terminal_state == "SUCCEEDED" else "runtime_failed_terminal"
        _write_runtime_audit(conn, prompt_record_id=pr[0], prompt_version_id=pr[1], event_type=evt, actor="system", payload={"execution_group_id": row[1], "execution_attempt_id": execution_attempt_id, "result_code": result_code, "secret_safe_message": message, "state_after": terminal_state})
        if terminal_state in {"SUCCEEDED", "FAILED_TERMINAL", "CANCELLED", "STALE_BLOCKED", "CONFLICT_BLOCKED"}:
            _update_usage_terminal(conn, execution_group_id=row[1], latest_attempt_id=execution_attempt_id, terminal_outcome=terminal_state)
        conn.commit()
        return {"state": terminal_state, "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "result_code": result_code, "secret_safe_message": message}

    queue_payload = "{}"
    try:
        conn.execute("INSERT INTO prompt_execution_async_queue(execution_group_id,execution_attempt_id,capability_code,queue_state,available_at,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", (row[1], execution_attempt_id, capability_code, "QUEUED", now, queue_payload, now, now))
    except sqlite3.IntegrityError:
        existing = conn.execute("SELECT id FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (execution_attempt_id,)).fetchone()
        if existing:
            return {"state": "DISPATCHED", "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "queue_id": int(existing[0])}
        conn.execute("UPDATE prompt_execution_attempts SET state='FAILED_TERMINAL',result_code=?,secret_safe_message=?,terminal_at=?,updated_at=? WHERE id=?", ("QUEUE_ADMISSION_FAILED", "Async queue admission failed.", now, now, execution_attempt_id))
        conn.execute("UPDATE prompt_execution_groups SET current_state='FAILED_TERMINAL',updated_at=?,closed_at=? WHERE id=?", (now, now, row[1]))
        conn.commit()
        return {"state": "FAILED_TERMINAL", "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "result_code": "QUEUE_ADMISSION_FAILED"}

    conn.execute("UPDATE prompt_execution_attempts SET state='DISPATCHED',updated_at=? WHERE id=?", (now, execution_attempt_id))
    conn.execute("UPDATE prompt_execution_groups SET current_state='DISPATCHED',updated_at=? WHERE id=?", (now, row[1]))
    _write_lifecycle_event(conn, execution_group_id=row[1], execution_attempt_id=execution_attempt_id, state_before="ADMITTED", state_after="DISPATCHED", result_code=None, actor="system", payload={})
    pr = conn.execute("SELECT prompt_record_id,prompt_version_id,correlation_id,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    _write_runtime_audit(conn, prompt_record_id=pr[0], prompt_version_id=pr[1], event_type="runtime_dispatched", actor="system", payload={"execution_group_id": row[1], "execution_attempt_id": execution_attempt_id, "correlation_id": pr[2], "dedup_key_hash": pr[3]})
    qid = int(conn.execute("SELECT id FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (execution_attempt_id,)).fetchone()[0])
    conn.commit()
    return {"state": "DISPATCHED", "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "queue_id": qid}


def _json_dumps(obj: dict) -> str:
    import json
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)


def _write_lifecycle_event(conn: sqlite3.Connection, *, execution_group_id: int, execution_attempt_id: int | None, state_before: str | None, state_after: str, result_code: str | None, actor: str, payload: dict | None = None) -> None:
    event_payload = payload or {}
    if not _is_secret_safe_data(event_payload):
        event_payload = {"note": "redacted"}
    conn.execute(
        "INSERT INTO prompt_execution_lifecycle_events(execution_group_id,execution_attempt_id,state_before,state_after,result_code,actor,timestamp,event_payload_json) VALUES(?,?,?,?,?,?,?,?)",
        (execution_group_id, execution_attempt_id, state_before, state_after, result_code, actor, _utc_now(), _json_dumps(event_payload)),
    )


def _write_runtime_audit(conn: sqlite3.Connection, *, prompt_record_id: int, prompt_version_id: int | None, event_type: str, actor: str, payload: dict) -> None:
    safe_payload = payload if _is_secret_safe_data(payload) else {"note": "redacted"}
    conn.execute(
        "INSERT INTO prompt_audit_events(prompt_id,version_id,event_type,actor,payload_json,created_at) VALUES(?,?,?,?,?,?)",
        (prompt_record_id, prompt_version_id, event_type, actor, _json_dumps(safe_payload), _utc_now()),
    )


def _ensure_usage_on_admitted(conn: sqlite3.Connection, *, execution_group_id: int, execution_attempt_id: int) -> None:
    exists = conn.execute("SELECT id FROM prompt_execution_usage WHERE execution_group_id=?", (execution_group_id,)).fetchone()
    if exists:
        return
    row = conn.execute("SELECT execution_group_id,prompt_record_id,prompt_version_id,rendered_payload_hash,binding_resolution_fingerprint,operator_id_or_system_actor,dedup_key_hash,created_at FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    g = conn.execute("SELECT capability_code,target_type,target_id FROM prompt_execution_groups WHERE id=?", (execution_group_id,)).fetchone()
    now = _utc_now()
    conn.execute(
        "INSERT INTO prompt_execution_usage(execution_group_id,first_admitted_attempt_id,latest_attempt_id,prompt_record_id,prompt_version_id,rendered_payload_hash,binding_resolution_fingerprint,capability_code,target_type,target_id,operator_id,first_admitted_at,usage_payload_json,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (execution_group_id, execution_attempt_id, execution_attempt_id, row[1], row[2], row[3], row[4], g[0], g[1], g[2], row[5], now, _json_dumps({"dedup_key_hash": row[6]}), now),
    )


def _update_usage_terminal(conn: sqlite3.Connection, *, execution_group_id: int, latest_attempt_id: int, terminal_outcome: str) -> None:
    now = _utc_now()
    conn.execute("UPDATE prompt_execution_usage SET latest_attempt_id=?,terminal_outcome=?,terminal_at=?,updated_at=? WHERE execution_group_id=?", (latest_attempt_id, terminal_outcome, now, now, execution_group_id))


def list_prompt_execution_timeline(conn: sqlite3.Connection, *, execution_group_id: int) -> list[dict]:
    rows = conn.execute("SELECT execution_group_id,execution_attempt_id,state_before,state_after,result_code,actor,timestamp,event_payload_json FROM prompt_execution_lifecycle_events WHERE execution_group_id=? ORDER BY timestamp ASC, id ASC", (execution_group_id,)).fetchall()
    return [dict(r) for r in rows]


def get_prompt_execution_status(conn: sqlite3.Connection, *, execution_group_id: int | None = None, execution_attempt_id: int | None = None) -> dict:
    if execution_attempt_id is not None:
        row = conn.execute("SELECT a.id AS execution_attempt_id,a.execution_group_id,a.state,a.result_code,a.secret_safe_message,g.current_state FROM prompt_execution_attempts a JOIN prompt_execution_groups g ON g.id=a.execution_group_id WHERE a.id=?", (execution_attempt_id,)).fetchone()
    elif execution_group_id is not None:
        row = conn.execute("SELECT a.id AS execution_attempt_id,a.execution_group_id,a.state,a.result_code,a.secret_safe_message,g.current_state FROM prompt_execution_attempts a JOIN prompt_execution_groups g ON g.id=a.execution_group_id WHERE a.execution_group_id=? ORDER BY a.id DESC LIMIT 1", (execution_group_id,)).fetchone()
    else:
        raise ValueError("execution_group_id or execution_attempt_id required")
    if row is None:
        raise ValueError("execution status not found")
    usage = conn.execute("SELECT * FROM prompt_execution_usage WHERE execution_group_id=?", (row[1],)).fetchone()
    return {
        "execution_group_id": row[1],
        "execution_attempt_id": row[0],
        "current_state": row[2],
        "result_code": row[3],
        "secret_safe_message": row[4],
        "lifecycle_events": list_prompt_execution_timeline(conn, execution_group_id=row[1]),
        "usage_summary": dict(usage) if usage else None,
    }
