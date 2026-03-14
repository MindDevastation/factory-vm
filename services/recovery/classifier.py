from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from services.common import db as dbm
from services.ops.recovery import CANONICAL_ACTIONS, preview_action


TERMINAL_STATES = {"PUBLISHED", "CANCELLED", "CLEANED"}
FAILED_STATES = {"FAILED", "RENDER_FAILED", "QA_FAILED", "UPLOAD_FAILED"}
ACTIVE_STATES = {"WAITING_INPUTS", "FETCHING_INPUTS", "READY_FOR_RENDER", "RENDERING", "QA_RUNNING", "UPLOADING"}


@dataclass(frozen=True)
class RecoveryRuntimeContext:
    job_lock_ttl_sec: int
    max_render_attempts: int
    retry_backoff_sec: int


class RecoveryClassifier:
    def __init__(self, conn: sqlite3.Connection, *, runtime: RecoveryRuntimeContext):
        self._conn = conn
        self._runtime = runtime

    def list_items(self, *, limit: int = 500) -> list[dict[str, Any]]:
        jobs = dbm.list_jobs(self._conn, limit=limit)
        return self._classify_jobs(jobs)

    def get_item(self, job_id: int) -> dict[str, Any] | None:
        job = dbm.get_job(self._conn, job_id)
        if not job:
            return None
        return self._classify_jobs([job])[0]

    def list_recent_audit(self, job_id: int, *, limit: int = 10) -> list[dict[str, Any]]:
        if not self._has_legacy_audit_schema():
            return []
        rows = self._conn.execute(
            """
            SELECT id, job_id, action, phase, requested_by,
                   request_payload_json, result_payload_json, ok, error_code, created_at
            FROM recovery_action_audit
            WHERE job_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (int(job_id), int(limit)),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            request_payload = self._safe_json(row.get("request_payload_json"))
            result_payload = self._safe_json(row.get("result_payload_json"))
            out.append(
                {
                    "id": int(row["id"]),
                    "job_id": int(row["job_id"]),
                    "action": str(row.get("action") or ""),
                    "phase": str(row.get("phase") or ""),
                    "requested_by": row.get("requested_by"),
                    "ok": bool(int(row.get("ok") or 0)),
                    "error_code": row.get("error_code"),
                    "created_at": row.get("created_at"),
                    "request_payload": request_payload,
                    "result_payload": result_payload,
                }
            )
        return out

    def _has_legacy_audit_schema(self) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            ("recovery_action_audit",),
        ).fetchone()
        if row is None:
            return False
        cols = {str(row.get("name")) for row in self._conn.execute("PRAGMA table_info(recovery_action_audit)").fetchall()}
        expected = {
            "id",
            "job_id",
            "action",
            "phase",
            "requested_by",
            "request_payload_json",
            "result_payload_json",
            "ok",
            "error_code",
            "created_at",
        }
        return expected.issubset(cols)

    @staticmethod
    def _safe_json(value: Any) -> Any:
        if value is None:
            return None
        try:
            return json.loads(str(value))
        except json.JSONDecodeError:
            return value

    def _classify_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now_ts = dbm.now_ts()
        workers = dbm.list_workers(self._conn, limit=500)
        worker_by_id = {str(row.get("worker_id")): row for row in workers}
        retry_child_map = self._retry_children([int(job["id"]) for job in jobs])
        return [
            self._classify_single(job, now_ts=now_ts, worker_by_id=worker_by_id, retry_child_id=retry_child_map.get(int(job["id"])))
            for job in jobs
        ]

    def _retry_children(self, job_ids: list[int]) -> dict[int, int]:
        if not job_ids:
            return {}
        placeholders = ",".join("?" for _ in job_ids)
        rows = self._conn.execute(
            f"SELECT id, retry_of_job_id FROM jobs WHERE retry_of_job_id IN ({placeholders})",
            tuple(job_ids),
        ).fetchall()
        return {
            int(row["retry_of_job_id"]): int(row["id"])
            for row in rows
            if row.get("retry_of_job_id") is not None
        }

    def _classify_single(
        self,
        job: dict[str, Any],
        *,
        now_ts: float,
        worker_by_id: dict[str, dict[str, Any]],
        retry_child_id: int | None,
    ) -> dict[str, Any]:
        state = str(job.get("state") or "")
        progress_updated_at = job.get("progress_updated_at")
        progress_age_sec = None
        if progress_updated_at is not None:
            progress_age_sec = max(0, int(now_ts - float(progress_updated_at)))

        available_actions = []
        has_safe = False
        has_risky = False
        for action in sorted(CANONICAL_ACTIONS):
            preview = preview_action(self._conn, job=job, action=action, runtime=self._runtime)
            risk_level = "safe" if preview.get("safe") else "risky"
            allowed = bool(preview.get("allowed"))
            if allowed and risk_level == "safe":
                has_safe = True
            if allowed and risk_level == "risky":
                has_risky = True
            available_actions.append(
                {
                    "action": action,
                    "allowed": allowed,
                    "risk_level": risk_level,
                    "reason": str(preview.get("reason") or "unknown"),
                }
            )

        categories: list[str] = []
        reasons: list[str] = []

        if state in FAILED_STATES:
            categories.append("failed")
            reasons.append(f"state={state}")

        stale = self._is_stale(job, now_ts=now_ts)
        if stale:
            categories.append("stale")
            reasons.append("job lock exceeded ttl")

        if self._is_stuck(state=state, progress_age_sec=progress_age_sec, stale=stale):
            categories.append("stuck")
            reasons.append("active state without recent progress")

        delete_mp4_at = job.get("delete_mp4_at")
        if state == "PUBLISHED" and delete_mp4_at is not None and float(delete_mp4_at) <= now_ts:
            categories.append("cleanup_pending")
            reasons.append("published artifact past delete_mp4_at")

        if any(item["action"] in {"retry_failed", "reclaim_stale"} and item["allowed"] for item in available_actions):
            categories.append("retryable")
            reasons.append("retry/reclaim action currently allowed")

        if any(item["action"] == "cancel_job" and item["allowed"] for item in available_actions):
            categories.append("cancellable")
            reasons.append("cancel action currently allowed")

        retry_at = job.get("retry_at")
        if retry_at is not None and float(retry_at) > now_ts:
            categories.append("blocked")
            reasons.append("retry_backoff_window_active")

        locked_by = str(job.get("locked_by") or "")
        worker = worker_by_id.get(locked_by) if locked_by else None
        worker_context = {
            "locked_by": locked_by or None,
            "locked_at": job.get("locked_at"),
            "heartbeat_last_seen": worker.get("last_seen") if worker else None,
            "heartbeat_present": bool(worker),
        }
        if locked_by and not worker:
            worker_context["fallback"] = "worker heartbeat unavailable; stale derived from lock ttl only"

        failure_summary = str(job.get("error_reason") or "").strip() or None
        context = {
            "stage": job.get("stage"),
            "attempt": int(job.get("attempt") or 0),
            "attempt_no": int(job.get("attempt_no") or 1),
            "retry_child_job_id": retry_child_id,
            "retry_at": retry_at,
            "progress_text": job.get("progress_text"),
        }

        return {
            "job_id": int(job["id"]),
            "state": state,
            "categories": categories,
            "category_reasons": reasons,
            "channel_slug": job.get("channel_slug"),
            "context": context,
            "updated_at": job.get("updated_at"),
            "last_progress_at": progress_updated_at,
            "progress_age_sec": progress_age_sec,
            "failure_summary": failure_summary,
            "worker_context": worker_context,
            "available_actions": available_actions,
            "actionability": self._actionability(has_safe=has_safe, has_risky=has_risky),
        }

    def _is_stale(self, job: dict[str, Any], *, now_ts: float) -> bool:
        state = str(job.get("state") or "")
        if state not in {"FETCHING_INPUTS", "RENDERING"}:
            return False
        locked_at = job.get("locked_at")
        locked_by = job.get("locked_by")
        if locked_at is None or not locked_by:
            return False
        return float(locked_at) < (now_ts - float(self._runtime.job_lock_ttl_sec))

    def _is_stuck(self, *, state: str, progress_age_sec: int | None, stale: bool) -> bool:
        if stale:
            return False
        if state in TERMINAL_STATES:
            return False
        if state not in ACTIVE_STATES:
            return False
        if progress_age_sec is None:
            return False
        threshold = max(int(self._runtime.job_lock_ttl_sec) * 2, 1800)
        return int(progress_age_sec) >= threshold

    @staticmethod
    def _actionability(*, has_safe: bool, has_risky: bool) -> str:
        if has_safe and has_risky:
            return "risky_present"
        if has_safe:
            return "safe_only"
        if has_risky:
            return "has_actions"
        return "any"
