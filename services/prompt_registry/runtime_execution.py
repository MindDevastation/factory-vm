from __future__ import annotations

import hashlib
import sqlite3
from datetime import datetime, timezone

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

    dedup_key_hash = compute_dedup_key_hash(**{k: kwargs.get(k) for k in ["capability_code", "target_type", "target_id", "prompt_record_id", "prompt_version_id", "binding_resolution_fingerprint", "rendered_payload_hash", "action_payload_hash", "reviewed_target_state_hash"]})
    now = _utc_now()
    existing = conn.execute("SELECT id,execution_group_id,state,operator_id_or_system_actor,reviewed_target_state_hash,dedup_key_hash FROM prompt_execution_attempts WHERE dedup_key_hash=? ORDER BY id DESC LIMIT 1", (dedup_key_hash,)).fetchone()
    if existing:
        token = _make_confirmation_token(execution_attempt_id=existing[0], dedup_key_hash=existing[5], reviewed_target_state_hash=existing[4], operator_id=existing[3], state=existing[2])
        return {"execution_group_id": existing[1], "execution_attempt_id": existing[0], "confirmation_token": token, "dedup_key_hash": existing[5], "state": existing[2], "execution_mode": CAPABILITY_EXECUTION_MODE[capability_code], "capability_code": capability_code, "target_type": kwargs["target_type"], "target_id": kwargs.get("target_id"), "secret_safe_message": "Ready for explicit confirmation."}

    conflict = conn.execute(f"SELECT id FROM prompt_execution_groups WHERE capability_code=? AND target_type=? AND COALESCE(target_id,'')=COALESCE(?, '') AND current_state IN ({','.join('?'*len(ACTIVE_STATES))}) ORDER BY id DESC LIMIT 1", (capability_code, kwargs["target_type"], kwargs.get("target_id"), *ACTIVE_STATES)).fetchone()
    if conflict:
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
    token = _make_confirmation_token(execution_attempt_id=aid, dedup_key_hash=dedup_key_hash, reviewed_target_state_hash=kwargs["reviewed_target_state_hash"], operator_id=operator_id, state="CONFIRMATION_REQUIRED")
    return {"execution_group_id": gid, "execution_attempt_id": aid, "confirmation_token": token, "dedup_key_hash": dedup_key_hash, "state": "CONFIRMATION_REQUIRED", "execution_mode": mode, "capability_code": capability_code, "target_type": kwargs["target_type"], "target_id": kwargs.get("target_id"), "secret_safe_message": "Ready for explicit confirmation."}


def confirm_prompt_execution(conn: sqlite3.Connection, *, execution_attempt_id: int, confirmation_token: str | None, operator_id_or_system_actor: str, reviewed_target_state_hash: str) -> dict:
    if not confirmation_token:
        raise ValueError("PREFLIGHT_REJECTED: confirmation token required")
    row = conn.execute("SELECT id,execution_group_id,state,dedup_key_hash,reviewed_target_state_hash,operator_id_or_system_actor FROM prompt_execution_attempts WHERE id=?", (execution_attempt_id,)).fetchone()
    if row is None:
        raise ValueError("PREFLIGHT_REJECTED: attempt not found")
    if row[4] != reviewed_target_state_hash:
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
    conn.commit()
    return {"state": "ADMITTED", "execution_group_id": row[1], "execution_attempt_id": row[0], "dedup_key_hash": row[3]}
