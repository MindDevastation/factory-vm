from __future__ import annotations

import sqlite3
import json
from dataclasses import dataclass
from typing import Any

from services.analytics_center.errors import (
    AnalyticsDomainError,
    E5A_EXTERNAL_CHANNEL_IDENTITY_MISSING,
    E5A_EXTERNAL_VIDEO_LINK_CONFLICT,
    E5A_INVALID_COVERAGE_PAYLOAD,
    E5A_INVALID_EXTERNAL_SCOPE,
    E5A_INVALID_REFRESH_MODE,
    E5A_SYNC_RUN_CONFLICT,
)
from services.analytics_center.helpers import validate_json_payload
from services.analytics_center.literals import (
    ANALYTICS_EXTERNAL_RUN_MODES,
    ANALYTICS_EXTERNAL_SYNC_STATES,
    ANALYTICS_EXTERNAL_TARGET_SCOPE_TYPES,
    ANALYTICS_YT_LINKAGE_CONFIDENCE,
    ANALYTICS_YT_LINKAGE_SOURCE,
)
from services.common.db import now_ts

METRIC_FAMILY_ALIASES: dict[str, str] = {
    "views": "views",
    "impressions": "impressions",
    "ctr": "ctr",
    "click_through_rate": "ctr",
    "watch_time": "watch_time",
    "average_view_duration": "average_view_duration",
    "retention": "retention",
    "subscribers": "subscribers",
    "subscribers_gained_lost": "subscribers",
    "monetization": "monetization",
}


@dataclass(frozen=True)
class SyncTarget:
    target_scope_type: str
    target_scope_ref: str
    run_mode: str
    observed_from: float | None
    observed_to: float | None
    metric_families: tuple[str, ...]


def normalize_metric_families(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    normalized: list[str] = []
    for value in values:
        key = str(value).strip().lower()
        if not key:
            continue
        canonical = METRIC_FAMILY_ALIASES.get(key)
        if canonical is None:
            raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message=f"unknown metric family: {value}")
        if canonical not in normalized:
            normalized.append(canonical)
    return tuple(normalized)


def build_coverage_payload(
    *,
    metric_families_requested: list[str] | tuple[str, ...],
    metric_families_returned: list[str] | tuple[str, ...],
    metric_families_unavailable: list[str] | tuple[str, ...],
    covered_window: dict[str, float | None],
    incomplete_backfill: bool,
    freshness_basis: str,
) -> str:
    requested = normalize_metric_families(metric_families_requested)
    returned = normalize_metric_families(metric_families_returned)
    unavailable = normalize_metric_families(metric_families_unavailable)
    if set(returned) - set(requested):
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message="returned metrics must be subset of requested")
    if set(unavailable) - set(requested):
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message="unavailable metrics must be subset of requested")
    if covered_window.get("from") is None and covered_window.get("to") is None:
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message="covered window required")
    payload: dict[str, Any] = {
        "metric_families_requested": requested,
        "metric_families_returned": returned,
        "metric_families_unavailable": unavailable,
        "covered_window": {"from": covered_window.get("from"), "to": covered_window.get("to")},
        "incomplete_backfill": bool(incomplete_backfill),
        "freshness_basis": freshness_basis,
    }
    return validate_json_payload(payload, field_name="coverage_payload_json")


def classify_external_availability(*, has_sync_history: bool, source_unavailable: bool, permission_limited: bool, stale: bool, partial: bool) -> str:
    if not has_sync_history:
        return "NOT_YET_SYNCED"
    if source_unavailable:
        return "SOURCE_UNAVAILABLE"
    if permission_limited:
        return "PERMISSION_LIMITED"
    if partial:
        return "PARTIAL"
    if stale:
        return "STALE"
    return "FRESH"


def _validate_scope(scope_type: str) -> str:
    normalized = scope_type.strip().upper()
    if normalized not in ANALYTICS_EXTERNAL_TARGET_SCOPE_TYPES:
        raise AnalyticsDomainError(code=E5A_INVALID_EXTERNAL_SCOPE, message=f"invalid target scope: {scope_type}")
    return normalized


def _validate_run_mode(run_mode: str) -> str:
    normalized = run_mode.strip().upper()
    if normalized not in ANALYTICS_EXTERNAL_RUN_MODES:
        raise AnalyticsDomainError(code=E5A_INVALID_REFRESH_MODE, message=f"invalid run mode: {run_mode}")
    return normalized


def _validate_sync_state(sync_state: str) -> str:
    normalized = sync_state.strip().upper()
    if normalized not in ANALYTICS_EXTERNAL_SYNC_STATES:
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message=f"invalid sync state: {sync_state}")
    return normalized


def create_sync_run(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    target_scope_type: str,
    target_scope_ref: str,
    run_mode: str,
    metric_families_requested: list[str] | tuple[str, ...],
    observed_from: float | None,
    observed_to: float | None,
    freshness_basis: str,
) -> int:
    scope = _validate_scope(target_scope_type)
    mode = _validate_run_mode(run_mode)
    metrics = normalize_metric_families(metric_families_requested)
    coverage_payload = build_coverage_payload(
        metric_families_requested=metrics,
        metric_families_returned=(),
        metric_families_unavailable=(),
        covered_window={"from": observed_from, "to": observed_to},
        incomplete_backfill=False,
        freshness_basis=freshness_basis,
    )
    running = conn.execute(
        """
        SELECT id FROM analytics_external_sync_runs
        WHERE provider_name = ? AND target_scope_type = ? AND target_scope_ref = ? AND run_mode = ? AND sync_state = 'RUNNING'
        LIMIT 1
        """,
        (provider_name, scope, target_scope_ref, mode),
    ).fetchone()
    if running is not None:
        raise AnalyticsDomainError(code=E5A_SYNC_RUN_CONFLICT, message="sync run already active for scope+mode")

    started_at = now_ts()
    row = conn.execute(
        """
        INSERT INTO analytics_external_sync_runs(
            provider_name, target_scope_type, target_scope_ref, run_mode, sync_state,
            requested_metric_families_json, coverage_payload_json,
            observed_from, observed_to, started_at
        ) VALUES(?, ?, ?, ?, 'RUNNING', ?, ?, ?, ?, ?)
        """,
        (
            provider_name,
            scope,
            target_scope_ref,
            mode,
            validate_json_payload(list(metrics), field_name="requested_metric_families_json"),
            coverage_payload,
            observed_from,
            observed_to,
            started_at,
        ),
    )
    _upsert_scope_status(
        conn,
        provider_name=provider_name,
        target_scope_type=scope,
        target_scope_ref=target_scope_ref,
        sync_state="RUNNING",
        freshness_status="UNKNOWN",
        coverage_payload_json=coverage_payload,
        has_sync_history=True,
        source_unavailable=False,
        permission_limited=False,
        stale=False,
        partial=False,
    )
    return int(row.lastrowid)


def transition_sync_run(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    to_sync_state: str,
    metric_families_returned: list[str] | tuple[str, ...],
    metric_families_unavailable: list[str] | tuple[str, ...],
    incomplete_backfill: bool,
    freshness_status: str,
    freshness_basis: str,
    source_unavailable: bool = False,
    permission_limited: bool = False,
) -> None:
    target_state = _validate_sync_state(to_sync_state)
    if target_state == "RUNNING":
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message="terminal state required")

    run = conn.execute("SELECT * FROM analytics_external_sync_runs WHERE id = ?", (int(run_id),)).fetchone()
    if run is None:
        raise AnalyticsDomainError(code=E5A_INVALID_EXTERNAL_SCOPE, message="sync run not found")
    if str(run["sync_state"]).upper() != "RUNNING":
        raise AnalyticsDomainError(code=E5A_SYNC_RUN_CONFLICT, message="sync run already finalized")

    requested = tuple(
        json.loads(
            validate_json_payload(
                run["requested_metric_families_json"],
                field_name="requested_metric_families_json",
            )
        )
    )
    coverage_payload = build_coverage_payload(
        metric_families_requested=requested,
        metric_families_returned=metric_families_returned,
        metric_families_unavailable=metric_families_unavailable,
        covered_window={"from": run["observed_from"], "to": run["observed_to"]},
        incomplete_backfill=incomplete_backfill,
        freshness_basis=freshness_basis,
    )
    completed = now_ts()
    conn.execute(
        """
        UPDATE analytics_external_sync_runs
        SET sync_state = ?, coverage_payload_json = ?, completed_at = ?, error_code = ?
        WHERE id = ?
        """,
        (
            target_state,
            coverage_payload,
            completed,
            "E5A_INCOMPLETE_BACKFILL" if incomplete_backfill else None,
            int(run_id),
        ),
    )
    _upsert_scope_status(
        conn,
        provider_name=str(run["provider_name"]),
        target_scope_type=str(run["target_scope_type"]),
        target_scope_ref=str(run["target_scope_ref"]),
        sync_state=target_state,
        freshness_status=freshness_status,
        coverage_payload_json=coverage_payload,
        has_sync_history=True,
        source_unavailable=source_unavailable,
        permission_limited=permission_limited,
        stale=freshness_status.upper() == "STALE",
        partial=target_state == "PARTIAL",
    )


def _upsert_scope_status(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    target_scope_type: str,
    target_scope_ref: str,
    sync_state: str,
    freshness_status: str,
    coverage_payload_json: str,
    has_sync_history: bool,
    source_unavailable: bool,
    permission_limited: bool,
    stale: bool,
    partial: bool,
) -> None:
    now = now_ts()
    availability_status = classify_external_availability(
        has_sync_history=has_sync_history,
        source_unavailable=source_unavailable,
        permission_limited=permission_limited,
        stale=stale,
        partial=partial,
    )
    conn.execute(
        """
        INSERT INTO analytics_external_scope_status(
            provider_name, target_scope_type, target_scope_ref,
            last_successful_sync_at, last_attempted_sync_at,
            sync_state, freshness_status, coverage_payload_json, availability_status, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider_name, target_scope_type, target_scope_ref)
        DO UPDATE SET
            last_successful_sync_at = excluded.last_successful_sync_at,
            last_attempted_sync_at = excluded.last_attempted_sync_at,
            sync_state = excluded.sync_state,
            freshness_status = excluded.freshness_status,
            coverage_payload_json = excluded.coverage_payload_json,
            availability_status = excluded.availability_status,
            updated_at = excluded.updated_at
        """,
        (
            provider_name,
            target_scope_type,
            target_scope_ref,
            now if sync_state in {"SUCCEEDED", "PARTIAL"} else None,
            now,
            sync_state,
            freshness_status,
            coverage_payload_json,
            availability_status,
            now,
        ),
    )


def link_channel_identity(conn: sqlite3.Connection, *, channel_slug: str) -> str:
    row = conn.execute(
        """
        SELECT external_id
        FROM analytics_external_identities
        WHERE entity_type = 'CHANNEL' AND entity_ref = (SELECT CAST(id AS TEXT) FROM channels WHERE slug = ? LIMIT 1)
          AND source_family = 'EXTERNAL_YOUTUBE'
        ORDER BY id DESC LIMIT 1
        """,
        (channel_slug,),
    ).fetchone()
    if row is None:
        raise AnalyticsDomainError(code=E5A_EXTERNAL_CHANNEL_IDENTITY_MISSING, message="missing external channel identity")
    return str(row["external_id"])


def link_release_video_context(conn: sqlite3.Connection, *, release_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT yl.youtube_video_id, yl.channel_slug, yl.job_id, yu.video_id AS uploaded_video_id
        FROM analytics_youtube_video_links yl
        LEFT JOIN youtube_uploads yu ON yu.job_id = yl.job_id
        WHERE yl.release_id = ?
        ORDER BY yl.id DESC
        LIMIT 1
        """,
        (int(release_id),),
    ).fetchone()
    if row is None:
        return None
    return dict(row)


def create_or_update_youtube_video_link(
    conn: sqlite3.Connection,
    *,
    channel_slug: str,
    youtube_video_id: str,
    release_id: int | None,
    job_id: int | None,
    youtube_channel_id: str | None,
    linkage_confidence: str,
    linkage_source: str,
    payload_json: Any,
) -> int:
    confidence = linkage_confidence.strip().upper()
    source = linkage_source.strip().upper()
    if confidence not in ANALYTICS_YT_LINKAGE_CONFIDENCE:
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message="invalid linkage confidence")
    if source not in ANALYTICS_YT_LINKAGE_SOURCE:
        raise AnalyticsDomainError(code=E5A_INVALID_COVERAGE_PAYLOAD, message="invalid linkage source")
    payload = validate_json_payload(payload_json, field_name="payload_json")
    existing = conn.execute(
        "SELECT id FROM analytics_youtube_video_links WHERE channel_slug = ? AND youtube_video_id = ? LIMIT 1",
        (channel_slug, youtube_video_id),
    ).fetchone()
    now = now_ts()
    if existing is None:
        try:
            row = conn.execute(
                """
                INSERT INTO analytics_youtube_video_links(
                    channel_slug, youtube_video_id, release_id, job_id, youtube_channel_id,
                    linkage_confidence, linkage_source, payload_json, created_at, updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (channel_slug, youtube_video_id, release_id, job_id, youtube_channel_id, confidence, source, payload, now, now),
            )
        except sqlite3.IntegrityError as exc:
            raise AnalyticsDomainError(code=E5A_EXTERNAL_VIDEO_LINK_CONFLICT, message="youtube video link conflict") from exc
        return int(row.lastrowid)

    conn.execute(
        """
        UPDATE analytics_youtube_video_links
        SET release_id = ?, job_id = ?, youtube_channel_id = ?, linkage_confidence = ?, linkage_source = ?, payload_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (release_id, job_id, youtube_channel_id, confidence, source, payload, now, int(existing["id"])),
    )
    return int(existing["id"])


def plan_fetch_targets(
    *,
    run_mode: str,
    channel_slug: str,
    release_video_refs: list[str] | None,
    now_ts_value: float,
    stale_before_ts: float | None,
    backfill_days: int,
    metric_families: list[str] | tuple[str, ...],
) -> list[SyncTarget]:
    mode = _validate_run_mode(run_mode)
    metrics = normalize_metric_families(metric_families)
    release_refs = [r for r in (release_video_refs or []) if str(r).strip()]

    if mode == "INITIAL_BACKFILL":
        from_ts = now_ts_value - float(backfill_days) * 86400.0
        targets = [SyncTarget("CHANNEL", channel_slug, mode, from_ts, now_ts_value, metrics)]
        targets.extend(SyncTarget("RELEASE_VIDEO", r, mode, from_ts, now_ts_value, metrics) for r in release_refs)
        return targets

    if mode in {"SCHEDULED_SYNC", "MANUAL_REFRESH", "PARTIAL_REFRESH"}:
        from_ts = now_ts_value - 86400.0
        targets = [SyncTarget("CHANNEL", channel_slug, mode, from_ts, now_ts_value, metrics)]
        targets.extend(SyncTarget("RELEASE_VIDEO", r, mode, from_ts, now_ts_value, metrics) for r in release_refs)
        return targets

    # STALE_RESYNC
    if stale_before_ts is None:
        stale_before_ts = now_ts_value - 172800.0
    targets = [SyncTarget("CHANNEL", channel_slug, mode, stale_before_ts, now_ts_value, metrics)]
    targets.extend(SyncTarget("RELEASE_VIDEO", r, mode, stale_before_ts, now_ts_value, metrics) for r in release_refs)
    return targets
