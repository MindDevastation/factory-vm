from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone

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
        conn.commit()
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
            _write_lifecycle_event(conn, execution_group_id=int(conflict[0]), execution_attempt_id=None, state_before=None, state_after="CONFLICT_BLOCKED", result_code="CONFLICT", actor=operator_id, payload={})
            _write_runtime_audit(conn, prompt_record_id=int(kwargs["prompt_record_id"]), prompt_version_id=kwargs.get("prompt_version_id"), event_type="runtime_conflict_blocked", actor=operator_id, payload={"blocked_by_execution_group_id": int(conflict[0]), "dedup_key_hash": dedup_key_hash})
            conn.commit()
            return {"state": "CONFLICT_BLOCKED", "blocked_by_execution_group_id": int(conflict[0]), "dedup_key_hash": dedup_key_hash}
        raise ValueError("CONFLICT_BLOCKED: active target lock collision")
    gid = int(cur.lastrowid)
    cur.execute("INSERT INTO prompt_execution_attempts(execution_group_id,attempt_number,state,correlation_id,operator_id_or_system_actor,prompt_record_id,prompt_version_id,binding_resolution_fingerprint,rendered_payload_hash,action_payload_hash,reviewed_target_state_hash,dedup_key_hash,retryable_by_operator,cancellable,execution_mode,dispatch_payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (gid, 1, "CONFIRMATION_REQUIRED", f"mf2:{gid}:1", operator_id, kwargs["prompt_record_id"], kwargs.get("prompt_version_id"), kwargs["binding_resolution_fingerprint"], kwargs["rendered_payload_hash"], kwargs["action_payload_hash"], kwargs["reviewed_target_state_hash"], dedup_key_hash, 0, 1, mode, "{}", now, now))
    aid = int(cur.lastrowid)
    conn.commit()
    _write_lifecycle_event(conn, execution_group_id=gid, execution_attempt_id=aid, state_before="PREPARED", state_after="CONFIRMATION_REQUIRED", result_code=None, actor=operator_id, payload={})
    _write_runtime_audit(conn, prompt_record_id=int(kwargs["prompt_record_id"]), prompt_version_id=kwargs.get("prompt_version_id"), event_type="runtime_confirmation_required", actor=operator_id, payload={"execution_group_id": gid, "execution_attempt_id": aid, "dedup_key_hash": dedup_key_hash})
    conn.commit()
    token = _make_confirmation_token(execution_attempt_id=aid, dedup_key_hash=dedup_key_hash, reviewed_target_state_hash=kwargs["reviewed_target_state_hash"], operator_id=operator_id, state="CONFIRMATION_REQUIRED")
    return {"execution_group_id": gid, "execution_attempt_id": aid, "confirmation_token": token, "dedup_key_hash": dedup_key_hash, "state": "CONFIRMATION_REQUIRED", "execution_mode": mode, "capability_code": capability_code, "target_type": kwargs["target_type"], "target_id": kwargs.get("target_id"), "secret_safe_message": "Ready for explicit confirmation."}


def confirm_prompt_execution(conn: sqlite3.Connection, *, execution_attempt_id: int, confirmation_token: str | None, operator_id_or_system_actor: str, reviewed_target_state_hash: str) -> dict:
    if not confirmation_token:
        raise ValueError("PREFLIGHT_REJECTED: confirmation token required")
    row = conn.execute("SELECT id,execution_group_id,state,dedup_key_hash,reviewed_target_state_hash,operator_id_or_system_actor FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    if row is None:
        raise ValueError("PREFLIGHT_REJECTED: attempt not found")
    if row[4] != reviewed_target_state_hash:
        _finalize_execution_terminal(
            conn,
            execution_group_id=row[1],
            execution_attempt_id=row[0],
            terminal_state="STALE_BLOCKED",
            result_code="STALE",
            secret_safe_message="Reviewed target state is stale.",
            actor=operator_id_or_system_actor,
        )
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
        safe_payload = {} if payload is None else payload if isinstance(payload, dict) else None
        if safe_payload is None or not _is_secret_safe_data(dict(safe_payload or {})):
            _finalize_execution_terminal(
                conn,
                execution_group_id=row[1],
                execution_attempt_id=execution_attempt_id,
                terminal_state="FAILED_TERMINAL",
                result_code="SECRET_UNSAFE_PAYLOAD",
                secret_safe_message="Adapter payload failed secret-safety precheck.",
                actor="system",
            )
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
        _finalize_execution_terminal(
            conn,
            execution_group_id=row[1],
            execution_attempt_id=execution_attempt_id,
            terminal_state=terminal_state,
            result_code=result_code,
            secret_safe_message=message,
            actor="system",
        )
        conn.commit()
        return {"state": terminal_state, "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "result_code": result_code, "secret_safe_message": message}

    queue_payload = "{}"
    try:
        conn.execute("INSERT INTO prompt_execution_async_queue(execution_group_id,execution_attempt_id,capability_code,queue_state,available_at,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", (row[1], execution_attempt_id, capability_code, "QUEUED", now, queue_payload, now, now))
    except sqlite3.IntegrityError:
        existing = conn.execute("SELECT id FROM prompt_execution_async_queue WHERE execution_attempt_id=?", (execution_attempt_id,)).fetchone()
        if existing:
            return {"state": "DISPATCHED", "execution_attempt_id": execution_attempt_id, "execution_group_id": row[1], "queue_id": int(existing[0])}
        _finalize_execution_terminal(
            conn,
            execution_group_id=row[1],
            execution_attempt_id=execution_attempt_id,
            terminal_state="FAILED_TERMINAL",
            result_code="QUEUE_ADMISSION_FAILED",
            secret_safe_message="Async queue admission failed.",
            actor="system",
        )
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


def _runtime_audit_event_for_terminal_state(terminal_state: str) -> str:
    return {
        "SUCCEEDED": "runtime_succeeded",
        "FAILED_TERMINAL": "runtime_failed_terminal",
        "CANCELLED": "runtime_cancelled",
        "STALE_BLOCKED": "runtime_stale_blocked",
        "CONFLICT_BLOCKED": "runtime_conflict_blocked",
    }.get(terminal_state, "runtime_failed_terminal")


def _secret_safe_text(value: str, fallback: str) -> str:
    text = str(value or "")
    allowed_generic_messages = {
        "Adapter payload failed secret-safety precheck.",
        "Adapter result failed secret-safety precheck.",
    }
    if text in allowed_generic_messages:
        return text
    return fallback if _contains_secret_like_text(text) else text


def _finalize_execution_terminal(
    conn: sqlite3.Connection,
    *,
    execution_group_id: int,
    execution_attempt_id: int,
    terminal_state: str,
    result_code: str,
    secret_safe_message: str,
    actor: str,
) -> None:
    current = conn.execute(
        "SELECT state,prompt_record_id,prompt_version_id,correlation_id,dedup_key_hash FROM prompt_execution_attempts WHERE id=?",
        (execution_attempt_id,),
    ).fetchone()
    if current is None:
        raise ValueError("execution attempt not found")
    state_before = current[0]
    safe_message = _secret_safe_text(secret_safe_message, "Execution terminal message redacted.")
    now = _utc_now()
    conn.execute(
        "UPDATE prompt_execution_attempts SET state=?,result_code=?,secret_safe_message=?,terminal_at=?,updated_at=? WHERE id=?",
        (terminal_state, result_code, safe_message, now, now, execution_attempt_id),
    )
    conn.execute(
        "UPDATE prompt_execution_groups SET current_state=?,updated_at=?,closed_at=? WHERE id=?",
        (terminal_state, now, now, execution_group_id),
    )
    _write_lifecycle_event(
        conn,
        execution_group_id=execution_group_id,
        execution_attempt_id=execution_attempt_id,
        state_before=state_before,
        state_after=terminal_state,
        result_code=result_code,
        actor=actor,
        payload={},
    )
    _write_runtime_audit(
        conn,
        prompt_record_id=current[1],
        prompt_version_id=current[2],
        event_type=_runtime_audit_event_for_terminal_state(terminal_state),
        actor=actor,
        payload={
            "execution_group_id": execution_group_id,
            "execution_attempt_id": execution_attempt_id,
            "correlation_id": current[3],
            "dedup_key_hash": current[4],
            "state_before": state_before,
            "state_after": terminal_state,
            "result_code": result_code,
            "secret_safe_message": safe_message,
        },
    )
    _update_usage_terminal(
        conn,
        execution_group_id=execution_group_id,
        latest_attempt_id=execution_attempt_id,
        terminal_outcome=terminal_state,
    )


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _add_seconds(ts: str, seconds: int) -> str:
    return (_parse_utc(ts) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _latest_attempt_for_group(conn: sqlite3.Connection, execution_group_id: int):
    return conn.execute(
        "SELECT * FROM prompt_execution_attempts WHERE execution_group_id=? ORDER BY attempt_number DESC,id DESC LIMIT 1",
        (execution_group_id,),
    ).fetchone()


def schedule_prompt_execution_retry(
    conn: sqlite3.Connection,
    *,
    execution_attempt_id: int,
    actor: str,
    retry_after: str | None = None,
) -> dict:
    source = conn.execute("SELECT * FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    if source is None:
        raise ValueError("RETRY_REJECTED: attempt not found")
    execution_group_id = int(source["execution_group_id"] if isinstance(source, sqlite3.Row) else source[1])
    state = source["state"] if isinstance(source, sqlite3.Row) else source[3]
    retryable = int(source["retryable_by_operator"] if isinstance(source, sqlite3.Row) else source[12])
    latest = _latest_attempt_for_group(conn, execution_group_id)
    if latest is not None:
        latest_id = int(latest["id"] if isinstance(latest, sqlite3.Row) else latest[0])
        latest_state = latest["state"] if isinstance(latest, sqlite3.Row) else latest[3]
        if latest_id != execution_attempt_id and latest_state in {"ADMITTED", "RETRY_PENDING"}:
            return {"state": latest_state, "execution_group_id": execution_group_id, "execution_attempt_id": latest_id, "idempotent": True}
    if state in {"SUCCEEDED", "CANCELLED", "STALE_BLOCKED", "CONFLICT_BLOCKED", "PREFLIGHT_REJECTED"}:
        raise ValueError("RETRY_REJECTED: state is not retryable")
    if state not in {"FAILED_TERMINAL", "RETRY_PENDING"} or retryable != 1:
        raise ValueError("RETRY_REJECTED: attempt is not retry eligible")
    max_no = int(conn.execute("SELECT COALESCE(MAX(attempt_number),0) FROM prompt_execution_attempts WHERE execution_group_id=?", (execution_group_id,)).fetchone()[0])
    now = _utc_now()
    admitted_at = retry_after or now
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO prompt_execution_attempts(
            execution_group_id,attempt_number,state,correlation_id,operator_id_or_system_actor,
            prompt_record_id,prompt_version_id,binding_resolution_fingerprint,rendered_payload_hash,
            action_payload_hash,reviewed_target_state_hash,dedup_key_hash,retryable_by_operator,
            cancellable,admitted_at,execution_mode,dispatch_payload_json,created_at,updated_at
        )
        SELECT execution_group_id,?, 'ADMITTED', ?, ?, prompt_record_id,prompt_version_id,
               binding_resolution_fingerprint,rendered_payload_hash,action_payload_hash,
               reviewed_target_state_hash,dedup_key_hash,retryable_by_operator,cancellable,?,
               execution_mode,dispatch_payload_json,?,?
        FROM prompt_execution_attempts WHERE id=?
        """,
        (max_no + 1, f"retry:{execution_group_id}:{max_no + 1}", actor, admitted_at, now, now, execution_attempt_id),
    )
    new_attempt_id = int(cur.lastrowid)
    conn.execute("UPDATE prompt_execution_groups SET current_state='ADMITTED',closed_at=NULL,updated_at=? WHERE id=?", (now, execution_group_id))
    _write_lifecycle_event(conn, execution_group_id=execution_group_id, execution_attempt_id=new_attempt_id, state_before=state, state_after="ADMITTED", result_code="RETRY_ADMITTED", actor=actor, payload={"source_execution_attempt_id": execution_attempt_id, "retry_after": retry_after})
    pr = conn.execute("SELECT prompt_record_id,prompt_version_id,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (new_attempt_id,)).fetchone()
    _write_runtime_audit(conn, prompt_record_id=pr[0], prompt_version_id=pr[1], event_type="runtime_retry_admitted", actor=actor, payload={"execution_group_id": execution_group_id, "execution_attempt_id": new_attempt_id, "source_execution_attempt_id": execution_attempt_id, "dedup_key_hash": pr[2], "retry_after": retry_after})
    _ensure_usage_on_admitted(conn, execution_group_id=execution_group_id, execution_attempt_id=new_attempt_id)
    conn.execute("UPDATE prompt_execution_usage SET latest_attempt_id=?,terminal_outcome=NULL,terminal_at=NULL,updated_at=? WHERE execution_group_id=?", (new_attempt_id, now, execution_group_id))
    conn.commit()
    return {"state": "ADMITTED", "execution_group_id": execution_group_id, "execution_attempt_id": new_attempt_id, "attempt_number": max_no + 1, "retry_eligible": True}


def cancel_prompt_execution(conn: sqlite3.Connection, *, execution_attempt_id: int, actor: str) -> dict:
    row = conn.execute("SELECT id,execution_group_id,state FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    if row is None:
        raise ValueError("CANCEL_REJECTED: attempt not found")
    state = row[2]
    execution_group_id = int(row[1])
    if state == "CANCELLED":
        return {"state": "CANCELLED", "execution_group_id": execution_group_id, "execution_attempt_id": execution_attempt_id, "idempotent": True}
    if state not in {"CONFIRMATION_REQUIRED", "ADMITTED", "DISPATCHED", "RUNNING", "RETRY_PENDING"}:
        raise ValueError("CANCEL_REJECTED: state is not cancellable")
    _finalize_execution_terminal(conn, execution_group_id=execution_group_id, execution_attempt_id=execution_attempt_id, terminal_state="CANCELLED", result_code="CANCELLED_BY_OPERATOR", secret_safe_message="Execution cancelled.", actor=actor)
    conn.execute("UPDATE prompt_execution_async_queue SET queue_state='FAILED',lease_owner=NULL,lease_expires_at=NULL,updated_at=? WHERE execution_attempt_id=? AND queue_state IN ('QUEUED','CLAIMED')", (_utc_now(), execution_attempt_id))
    conn.commit()
    return {"state": "CANCELLED", "execution_group_id": execution_group_id, "execution_attempt_id": execution_attempt_id}


def claim_prompt_execution_async_work(conn: sqlite3.Connection, *, lease_owner: str, lease_seconds: int = 60, now: str | None = None) -> dict | None:
    claim_at = now or _utc_now()
    row = conn.execute(
        "SELECT id,execution_group_id,execution_attempt_id,capability_code FROM prompt_execution_async_queue WHERE queue_state='QUEUED' AND available_at<=? ORDER BY available_at ASC,id ASC LIMIT 1",
        (claim_at,),
    ).fetchone()
    if row is None:
        return None
    qid, gid, aid, capability = int(row[0]), int(row[1]), int(row[2]), row[3]
    lease_expires = _add_seconds(claim_at, lease_seconds)
    current = conn.execute("SELECT state,prompt_record_id,prompt_version_id,correlation_id,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (aid,)).fetchone()
    state_before = current[0]
    conn.execute("UPDATE prompt_execution_async_queue SET queue_state='CLAIMED',lease_owner=?,lease_expires_at=?,updated_at=? WHERE id=?", (lease_owner, lease_expires, claim_at, qid))
    conn.execute("UPDATE prompt_execution_attempts SET state='RUNNING',running_at=COALESCE(running_at,?),lease_expires_at=?,updated_at=? WHERE id=?", (claim_at, lease_expires, claim_at, aid))
    conn.execute("UPDATE prompt_execution_groups SET current_state='RUNNING',lease_expires_at=?,updated_at=? WHERE id=?", (lease_expires, claim_at, gid))
    _write_lifecycle_event(conn, execution_group_id=gid, execution_attempt_id=aid, state_before=state_before, state_after="RUNNING", result_code="ASYNC_CLAIMED", actor=lease_owner, payload={"queue_id": qid})
    _write_runtime_audit(conn, prompt_record_id=current[1], prompt_version_id=current[2], event_type="runtime_running", actor=lease_owner, payload={"execution_group_id": gid, "execution_attempt_id": aid, "correlation_id": current[3], "dedup_key_hash": current[4], "queue_id": qid, "capability_code": capability})
    conn.commit()
    return {"queue_id": qid, "execution_group_id": gid, "execution_attempt_id": aid, "queue_state": "CLAIMED", "lease_owner": lease_owner, "lease_expires_at": lease_expires}


def reclaim_expired_prompt_execution_leases(conn: sqlite3.Connection, *, now: str | None = None, max_retries: int = 1) -> list[dict]:
    reclaim_at = now or _utc_now()
    rows = conn.execute("SELECT id,execution_group_id,execution_attempt_id FROM prompt_execution_async_queue WHERE queue_state='CLAIMED' AND lease_expires_at IS NOT NULL AND lease_expires_at<=? ORDER BY id ASC", (reclaim_at,)).fetchall()
    out: list[dict] = []
    for q in rows:
        qid, gid, aid = int(q[0]), int(q[1]), int(q[2])
        attempt = conn.execute("SELECT state,retryable_by_operator,prompt_record_id,prompt_version_id,correlation_id,dedup_key_hash FROM prompt_execution_attempts WHERE id=?", (aid,)).fetchone()
        if attempt is None or attempt[0] in {"SUCCEEDED", "FAILED_TERMINAL", "CANCELLED", "STALE_BLOCKED", "CONFLICT_BLOCKED"}:
            continue
        if int(attempt[1]) == 1 and max_retries > 0:
            conn.execute("UPDATE prompt_execution_async_queue SET queue_state='QUEUED',lease_owner=NULL,lease_expires_at=NULL,available_at=?,updated_at=? WHERE id=?", (reclaim_at, reclaim_at, qid))
            conn.execute("UPDATE prompt_execution_attempts SET state='RETRY_PENDING',lease_expires_at=NULL,updated_at=? WHERE id=?", (reclaim_at, aid))
            conn.execute("UPDATE prompt_execution_groups SET current_state='RETRY_PENDING',lease_expires_at=NULL,updated_at=? WHERE id=?", (reclaim_at, gid))
            _write_lifecycle_event(conn, execution_group_id=gid, execution_attempt_id=aid, state_before=attempt[0], state_after="RETRY_PENDING", result_code="LEASE_RECLAIMED", actor="system", payload={"queue_id": qid})
            _write_runtime_audit(conn, prompt_record_id=attempt[2], prompt_version_id=attempt[3], event_type="runtime_retry_pending", actor="system", payload={"execution_group_id": gid, "execution_attempt_id": aid, "correlation_id": attempt[4], "dedup_key_hash": attempt[5], "queue_id": qid})
            out.append({"queue_id": qid, "execution_attempt_id": aid, "state": "RETRY_PENDING"})
        else:
            conn.execute("UPDATE prompt_execution_async_queue SET queue_state='FAILED',lease_owner=NULL,lease_expires_at=NULL,updated_at=? WHERE id=?", (reclaim_at, qid))
            _finalize_execution_terminal(conn, execution_group_id=gid, execution_attempt_id=aid, terminal_state="FAILED_TERMINAL", result_code="LEASE_EXPIRED", secret_safe_message="Async execution lease expired.", actor="system")
            out.append({"queue_id": qid, "execution_attempt_id": aid, "state": "FAILED_TERMINAL"})
    conn.commit()
    return out


def recover_stale_runtime_executions(conn: sqlite3.Connection, *, now: str | None = None) -> list[dict]:
    recovery_at = now or _utc_now()
    recovered = reclaim_expired_prompt_execution_leases(conn, now=recovery_at)
    rows = conn.execute(
        """
        SELECT a.id,a.execution_group_id
        FROM prompt_execution_attempts a
        WHERE a.state='DISPATCHED'
          AND NOT EXISTS (SELECT 1 FROM prompt_execution_async_queue q WHERE q.execution_attempt_id=a.id AND q.queue_state IN ('QUEUED','CLAIMED'))
        ORDER BY a.id ASC
        """
    ).fetchall()
    for row in rows:
        aid, gid = int(row[0]), int(row[1])
        conn.execute("INSERT OR IGNORE INTO prompt_execution_async_queue(execution_group_id,execution_attempt_id,capability_code,queue_state,available_at,payload_json,created_at,updated_at) SELECT ?,?,g.capability_code,'QUEUED',?,'{}',?,? FROM prompt_execution_groups g WHERE g.id=?", (gid, aid, recovery_at, recovery_at, recovery_at, gid))
        _write_lifecycle_event(conn, execution_group_id=gid, execution_attempt_id=aid, state_before="DISPATCHED", state_after="DISPATCHED", result_code="RECOVERY_REQUEUED", actor="system", payload={})
        recovered.append({"execution_group_id": gid, "execution_attempt_id": aid, "state": "DISPATCHED"})
    conn.commit()
    return recovered


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
