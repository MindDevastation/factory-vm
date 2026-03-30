from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.security import require_basic_auth


SOURCE_UNAVAILABLE_CODE = "PRC_SOURCE_UNAVAILABLE"


@dataclass(frozen=True)
class ReconcileClassification:
    classification: str
    expected_visibility: str
    observed_visibility: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_visibility(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"public", "unlisted", "private"}:
        return normalized
    return "unknown"


def classify_drift(*, expected_visibility: str, observed_visibility: str) -> ReconcileClassification:
    expected = _normalize_visibility(expected_visibility)
    observed = _normalize_visibility(observed_visibility)
    if expected == "unknown" or observed == "unknown":
        return ReconcileClassification(
            classification="source_unavailable",
            expected_visibility=expected,
            observed_visibility=observed,
        )
    if expected == observed:
        return ReconcileClassification(
            classification="no_drift",
            expected_visibility=expected,
            observed_visibility=observed,
        )
    return ReconcileClassification(
        classification="drift_detected",
        expected_visibility=expected,
        observed_visibility=observed,
    )


def _insert_run(conn: Any, *, status: str, error_code: str | None, error_message: str | None, total_jobs: int, compared_jobs: int, drift_count: int, no_drift_count: int) -> int:
    ts = _now_iso()
    cur = conn.execute(
        """
        INSERT INTO publish_reconcile_runs(
            trigger_mode,
            status,
            error_code,
            error_message,
            total_jobs,
            compared_jobs,
            drift_count,
            no_drift_count,
            created_at,
            finished_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "manual",
            status,
            error_code,
            error_message,
            total_jobs,
            compared_jobs,
            drift_count,
            no_drift_count,
            ts,
            ts,
        ),
    )
    return int(cur.lastrowid)


def _load_candidates(conn: Any) -> list[Any]:
    return conn.execute(
        """
        SELECT j.id, j.release_id, c.slug AS channel_slug, j.publish_state, j.publish_target_visibility, y.privacy AS observed_visibility
        FROM jobs j
        JOIN releases r ON r.id = j.release_id
        JOIN channels c ON c.id = r.channel_id
        LEFT JOIN youtube_uploads y ON y.job_id = j.id
        WHERE j.publish_state IS NOT NULL
          AND j.publish_target_visibility IN ('public', 'unlisted')
        ORDER BY j.id ASC
        """
    ).fetchall()


def _persist_item(conn: Any, *, run_id: int, row: Any, result: ReconcileClassification) -> None:
    conn.execute(
        """
        INSERT INTO publish_reconcile_items(
            run_id,
            job_id,
            release_id,
            channel_slug,
            publish_state_snapshot,
            expected_visibility,
            observed_visibility,
            drift_classification,
            created_at
        ) VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            int(row["id"]),
            int(row["release_id"]),
            str(row["channel_slug"]),
            str(row["publish_state"]),
            result.expected_visibility,
            result.observed_visibility,
            result.classification,
            _now_iso(),
        ),
    )


def _run_reconcile(conn: Any) -> tuple[int, int]:
    candidates = _load_candidates(conn)
    total_jobs = len(candidates)
    classified: list[tuple[Any, ReconcileClassification]] = []
    for row in candidates:
        result = classify_drift(
            expected_visibility=str(row["publish_target_visibility"] or ""),
            observed_visibility=str(row["observed_visibility"] or ""),
        )
        classified.append((row, result))

    if any(result.classification == "source_unavailable" for _, result in classified):
        run_id = _insert_run(
            conn,
            status="source_unavailable",
            error_code=SOURCE_UNAVAILABLE_CODE,
            error_message="publish source unavailable for reconciliation",
            total_jobs=total_jobs,
            compared_jobs=0,
            drift_count=0,
            no_drift_count=0,
        )
        conn.commit()
        return run_id, 503

    drift_count = 0
    no_drift_count = 0
    for _, result in classified:
        if result.classification == "drift_detected":
            drift_count += 1
        elif result.classification == "no_drift":
            no_drift_count += 1

    run_id = _insert_run(
        conn,
        status="completed",
        error_code=None,
        error_message=None,
        total_jobs=total_jobs,
        compared_jobs=len(classified),
        drift_count=drift_count,
        no_drift_count=no_drift_count,
    )
    for row, result in classified:
        _persist_item(conn, run_id=run_id, row=row, result=result)
    conn.commit()
    return run_id, 200


def _serialize_item(row: Any) -> dict[str, Any]:
    return {
        "item_id": int(row["id"]),
        "job_id": int(row["job_id"]),
        "release_id": int(row["release_id"]),
        "channel_slug": str(row["channel_slug"]),
        "publish_state_snapshot": str(row["publish_state_snapshot"]),
        "expected_visibility": str(row["expected_visibility"]),
        "observed_visibility": str(row["observed_visibility"]),
        "drift_classification": str(row["drift_classification"]),
        "created_at": str(row["created_at"]),
    }


def _serialize_run(row: Any) -> dict[str, Any]:
    return {
        "run_id": int(row["id"]),
        "trigger_mode": str(row["trigger_mode"]),
        "status": str(row["status"]),
        "error_code": row["error_code"],
        "error_message": row["error_message"],
        "summary": {
            "total_jobs": int(row["total_jobs"]),
            "compared_jobs": int(row["compared_jobs"]),
            "drift_count": int(row["drift_count"]),
            "no_drift_count": int(row["no_drift_count"]),
        },
        "created_at": str(row["created_at"]),
        "finished_at": str(row["finished_at"]),
    }


def create_publish_reconcile_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/publish/reconcile", tags=["publish-reconcile"])

    @router.post("/run")
    def run_reconcile(_: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            run_id, status_code = _run_reconcile(conn)
            run_row = conn.execute("SELECT * FROM publish_reconcile_runs WHERE id = ?", (run_id,)).fetchone()
        finally:
            conn.close()

        if status_code == 503:
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "code": SOURCE_UNAVAILABLE_CODE,
                        "message": "publish source unavailable for reconciliation",
                        "run_id": run_id,
                    }
                },
            )

        assert run_row is not None
        return _serialize_run(run_row)

    @router.get("/runs/{run_id}")
    def get_run(run_id: int, _: bool = Depends(require_basic_auth(env))):
        conn = dbm.connect(env)
        try:
            run_row = conn.execute("SELECT * FROM publish_reconcile_runs WHERE id = ?", (run_id,)).fetchone()
            if run_row is None:
                return JSONResponse(status_code=404, content={"error": {"code": "PRC_RUN_NOT_FOUND", "message": "reconcile run not found"}})
            item_rows = conn.execute(
                "SELECT * FROM publish_reconcile_items WHERE run_id = ? ORDER BY id ASC",
                (run_id,),
            ).fetchall()
        finally:
            conn.close()

        payload = _serialize_run(run_row)
        payload["items"] = [_serialize_item(row) for row in item_rows]
        return payload

    return router


__all__ = [
    "SOURCE_UNAVAILABLE_CODE",
    "ReconcileClassification",
    "classify_drift",
    "create_publish_reconcile_router",
]
