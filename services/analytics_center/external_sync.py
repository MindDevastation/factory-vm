from __future__ import annotations

import sqlite3
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol

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
from services.analytics_center.write_service import SnapshotWriteInput, write_snapshot

logger = logging.getLogger(__name__)

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


class YouTubeMetricsProvider(Protocol):
    def fetch_channel_metrics(
        self,
        *,
        channel_slug: str,
        metric_families: tuple[str, ...],
        observed_from: float | None,
        observed_to: float | None,
    ) -> dict[str, Any]: ...

    def fetch_video_metrics(
        self,
        *,
        channel_slug: str,
        youtube_video_id: str,
        metric_families: tuple[str, ...],
        observed_from: float | None,
        observed_to: float | None,
    ) -> dict[str, Any]: ...


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
    _record_external_event(
        conn,
        event_type=f"{mode}_STARTED",
        provider_name=provider_name,
        target_scope_type=scope,
        target_scope_ref=target_scope_ref,
        run_mode=mode,
        sync_state="RUNNING",
        observed_from=observed_from,
        observed_to=observed_to,
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
    _record_external_event(
        conn,
        event_type=f"{str(run['run_mode']).upper()}_COMPLETED",
        provider_name=str(run["provider_name"]),
        target_scope_type=str(run["target_scope_type"]),
        target_scope_ref=str(run["target_scope_ref"]),
        run_mode=str(run["run_mode"]),
        sync_state=target_state,
        observed_from=run["observed_from"],
        observed_to=run["observed_to"],
        missing_metric_families=tuple(metric_families_unavailable),
        incomplete_backfill=incomplete_backfill,
        freshness_status=freshness_status,
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


def run_external_youtube_ingestion(
    conn: sqlite3.Connection,
    *,
    run_id: int,
    provider: YouTubeMetricsProvider,
    channel_slug: str,
    target_scope_type: str,
    target_scope_ref: str,
) -> int | None:
    run = conn.execute("SELECT * FROM analytics_external_sync_runs WHERE id = ?", (int(run_id),)).fetchone()
    if run is None:
        raise AnalyticsDomainError(code=E5A_INVALID_EXTERNAL_SCOPE, message="sync run not found")
    if str(run["provider_name"]).upper() != "YOUTUBE":
        raise AnalyticsDomainError(code=E5A_INVALID_EXTERNAL_SCOPE, message="provider must be YOUTUBE")
    if str(run["sync_state"]).upper() != "RUNNING":
        raise AnalyticsDomainError(code=E5A_SYNC_RUN_CONFLICT, message="sync run is not RUNNING")

    requested = tuple(json.loads(str(run["requested_metric_families_json"])))
    scope = _validate_scope(target_scope_type)
    snapshot_id: int | None = None
    try:
        if scope == "CHANNEL":
            provider_payload = provider.fetch_channel_metrics(
                channel_slug=channel_slug,
                metric_families=requested,
                observed_from=run["observed_from"],
                observed_to=run["observed_to"],
            )
            channel = conn.execute("SELECT id FROM channels WHERE slug = ?", (channel_slug,)).fetchone()
            if channel is None:
                raise AnalyticsDomainError(code=E5A_INVALID_EXTERNAL_SCOPE, message="channel not found")
            snapshot_id = _persist_external_snapshot(
                conn,
                entity_type="CHANNEL",
                entity_ref=str(channel["id"]),
                run_mode=str(run["run_mode"]),
                provider_payload=provider_payload,
            )
        else:
            link = conn.execute(
                """
                SELECT * FROM analytics_youtube_video_links
                WHERE channel_slug = ? AND youtube_video_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (channel_slug, target_scope_ref),
            ).fetchone()
            if link is None or link.get("release_id") is None:
                raise AnalyticsDomainError(code=E5A_INVALID_EXTERNAL_SCOPE, message="release-linked video context missing")
            provider_payload = provider.fetch_video_metrics(
                channel_slug=channel_slug,
                youtube_video_id=target_scope_ref,
                metric_families=requested,
                observed_from=run["observed_from"],
                observed_to=run["observed_to"],
            )
            snapshot_id = _persist_external_snapshot(
                conn,
                entity_type="RELEASE",
                entity_ref=str(link["release_id"]),
                run_mode=str(run["run_mode"]),
                provider_payload=provider_payload,
            )

        returned = provider_payload.get("metric_families_returned", [])
        unavailable = provider_payload.get("metric_families_unavailable", [])
        incomplete_backfill = bool(provider_payload.get("incomplete_backfill", False))
        source_unavailable = bool(provider_payload.get("source_unavailable", False))
        permission_limited = bool(provider_payload.get("permission_limited", False))
        to_state = "FAILED" if source_unavailable else ("PARTIAL" if unavailable or incomplete_backfill else "SUCCEEDED")
        freshness = str(provider_payload.get("freshness_status", "FRESH")).upper()
        transition_sync_run(
            conn,
            run_id=run_id,
            to_sync_state=to_state,
            metric_families_returned=returned,
            metric_families_unavailable=unavailable,
            incomplete_backfill=incomplete_backfill,
            freshness_status=freshness,
            freshness_basis=str(provider_payload.get("freshness_basis", "window_end")),
            source_unavailable=source_unavailable,
            permission_limited=permission_limited,
        )
        conn.execute(
            "UPDATE analytics_external_sync_runs SET created_snapshots_count = ?, partial_snapshots_count = ?, failed_snapshots_count = ? WHERE id = ?",
            (
                0 if snapshot_id is None else 1,
                1 if to_state == "PARTIAL" else 0,
                1 if to_state == "FAILED" else 0,
                int(run_id),
            ),
        )
        _record_external_event(
            conn,
            event_type="SNAPSHOTS_WRITTEN",
            provider_name=str(run["provider_name"]),
            target_scope_type=scope,
            target_scope_ref=target_scope_ref,
            run_mode=str(run["run_mode"]),
            sync_state=to_state,
            observed_from=run["observed_from"],
            observed_to=run["observed_to"],
            created_snapshots_count=0 if snapshot_id is None else 1,
            partial_snapshots_count=1 if to_state == "PARTIAL" else 0,
            failed_snapshots_count=1 if to_state == "FAILED" else 0,
            missing_metric_families=tuple(unavailable),
            incomplete_backfill=incomplete_backfill,
            freshness_status=freshness,
        )
        if source_unavailable:
            _record_external_event(
                conn,
                event_type="EXTERNAL_SOURCE_UNAVAILABLE",
                provider_name=str(run["provider_name"]),
                target_scope_type=scope,
                target_scope_ref=target_scope_ref,
                run_mode=str(run["run_mode"]),
                sync_state=to_state,
                observed_from=run["observed_from"],
                observed_to=run["observed_to"],
                missing_metric_families=tuple(unavailable),
                incomplete_backfill=incomplete_backfill,
                freshness_status=freshness,
            )
        if permission_limited and unavailable:
            _record_external_event(
                conn,
                event_type="PERMISSION_LIMITED_METRICS_SKIPPED",
                provider_name=str(run["provider_name"]),
                target_scope_type=scope,
                target_scope_ref=target_scope_ref,
                run_mode=str(run["run_mode"]),
                sync_state=to_state,
                observed_from=run["observed_from"],
                observed_to=run["observed_to"],
                missing_metric_families=tuple(unavailable),
                incomplete_backfill=incomplete_backfill,
                freshness_status=freshness,
            )
        if incomplete_backfill:
            _record_external_event(
                conn,
                event_type="INCOMPLETE_BACKFILL_RECORDED",
                provider_name=str(run["provider_name"]),
                target_scope_type=scope,
                target_scope_ref=target_scope_ref,
                run_mode=str(run["run_mode"]),
                sync_state=to_state,
                observed_from=run["observed_from"],
                observed_to=run["observed_to"],
                missing_metric_families=tuple(unavailable),
                incomplete_backfill=True,
                freshness_status=freshness,
            )
        return snapshot_id
    except AnalyticsDomainError:
        raise
    except Exception as exc:
        transition_sync_run(
            conn,
            run_id=run_id,
            to_sync_state="FAILED",
            metric_families_returned=(),
            metric_families_unavailable=requested,
            incomplete_backfill=False,
            freshness_status="UNKNOWN",
            freshness_basis="runner_exception",
            source_unavailable=True,
            permission_limited=False,
        )
        conn.execute(
            "UPDATE analytics_external_sync_runs SET failed_snapshots_count = 1, error_detail = ? WHERE id = ?",
            (str(exc), int(run_id)),
        )
        _record_external_event(
            conn,
            event_type="SYNC_RUN_FAILED",
            provider_name=str(run["provider_name"]),
            target_scope_type=scope,
            target_scope_ref=target_scope_ref,
            run_mode=str(run["run_mode"]),
            sync_state="FAILED",
            observed_from=run["observed_from"],
            observed_to=run["observed_to"],
            created_snapshots_count=0,
            partial_snapshots_count=0,
            failed_snapshots_count=1,
            missing_metric_families=requested,
            incomplete_backfill=False,
            freshness_status="UNKNOWN",
        )
        return None


def _persist_external_snapshot(
    conn: sqlite3.Connection,
    *,
    entity_type: str,
    entity_ref: str,
    run_mode: str,
    provider_payload: dict[str, Any],
) -> int:
    unavailable = normalize_metric_families(tuple(provider_payload.get("metric_families_unavailable", ())))
    source_unavailable = bool(provider_payload.get("source_unavailable", False))
    incomplete_backfill = bool(provider_payload.get("incomplete_backfill", False))
    status = "FAILED" if source_unavailable else ("PARTIAL" if unavailable or incomplete_backfill else "CURRENT")
    freshness_status = str(provider_payload.get("freshness_status", "FRESH")).upper()
    snapshot = SnapshotWriteInput(
        entity_type=entity_type,
        entity_ref=entity_ref,
        source_family="EXTERNAL_YOUTUBE",
        window_type="BOUNDED_WINDOW",
        snapshot_status=status,
        freshness_status=freshness_status if freshness_status in {"FRESH", "STALE", "PARTIAL", "UNKNOWN"} else "UNKNOWN",
        payload_json=provider_payload.get("metrics", {}),
        explainability_json={"provider": "YOUTUBE", "run_mode": run_mode},
        lineage_json={"source": "youtube_analytics_provider", "channel_slug": provider_payload.get("channel_slug")},
        anomaly_markers_json=provider_payload.get("metric_families_unavailable", []),
        captured_at=now_ts(),
        is_current=not source_unavailable,
    )
    return write_snapshot(conn, snapshot)


def request_manual_refresh(
    conn: sqlite3.Connection,
    *,
    target_scope_type: str,
    target_scope_ref: str,
    refresh_mode: str,
    metrics_subset: list[str] | None = None,
    observed_from: float | None = None,
    observed_to: float | None = None,
    force: bool = False,
) -> int:
    mode = _validate_run_mode(refresh_mode)
    if mode not in {"MANUAL_REFRESH", "PARTIAL_REFRESH", "STALE_RESYNC", "INITIAL_BACKFILL"}:
        raise AnalyticsDomainError(code=E5A_INVALID_REFRESH_MODE, message="unsupported manual refresh mode")
    return create_sync_run(
        conn,
        provider_name="YOUTUBE",
        target_scope_type=target_scope_type,
        target_scope_ref=target_scope_ref,
        run_mode=mode,
        metric_families_requested=metrics_subset or ["views", "impressions", "ctr", "watch_time", "average_view_duration", "retention", "subscribers", "monetization"],
        observed_from=observed_from if observed_from is not None else now_ts() - (86400.0 if not force else 259200.0),
        observed_to=observed_to if observed_to is not None else now_ts(),
        freshness_basis="manual_refresh_force" if force else "manual_refresh",
    )


def get_sync_status(conn: sqlite3.Connection, *, target_scope_type: str, target_scope_ref: str) -> dict[str, Any]:
    scope = _validate_scope(target_scope_type)
    row = conn.execute(
        """
        SELECT provider_name, target_scope_type, target_scope_ref, last_successful_sync_at, last_attempted_sync_at,
               freshness_status, sync_state, coverage_payload_json, availability_status
        FROM analytics_external_scope_status
        WHERE provider_name = 'YOUTUBE' AND target_scope_type = ? AND target_scope_ref = ?
        """,
        (scope, target_scope_ref),
    ).fetchone()
    if row is None:
        return {
            "provider_name": "YOUTUBE",
            "target_scope_type": scope,
            "target_scope_ref": target_scope_ref,
            "last_successful_sync_at": None,
            "last_attempted_sync_at": None,
            "freshness_status": "UNKNOWN",
            "sync_state": "FAILED",
            "covered_windows": None,
            "incomplete_backfill": False,
            "missing_metric_families": [],
            "source_availability_status": "NOT_YET_SYNCED",
        }
    coverage = json.loads(str(row["coverage_payload_json"]))
    return {
        "provider_name": row["provider_name"],
        "target_scope_type": row["target_scope_type"],
        "target_scope_ref": row["target_scope_ref"],
        "last_successful_sync_at": row["last_successful_sync_at"],
        "last_attempted_sync_at": row["last_attempted_sync_at"],
        "freshness_status": row["freshness_status"],
        "sync_state": row["sync_state"],
        "covered_windows": coverage.get("covered_window"),
        "incomplete_backfill": bool(coverage.get("incomplete_backfill", False)),
        "missing_metric_families": list(coverage.get("metric_families_unavailable", [])),
        "source_availability_status": row["availability_status"],
    }


def get_coverage_report(conn: sqlite3.Connection, *, target_scope_type: str, target_scope_ref: str) -> dict[str, Any]:
    status = get_sync_status(conn, target_scope_type=target_scope_type, target_scope_ref=target_scope_ref)
    if status["source_availability_status"] == "NOT_YET_SYNCED":
        return {
            "scope": {"target_scope_type": target_scope_type, "target_scope_ref": target_scope_ref},
            "metric_family_coverage": {},
            "historical_range_coverage": None,
            "incomplete_windows": [],
            "unavailable_by_permission": [],
            "not_yet_synced": True,
        }
    row = conn.execute(
        """
        SELECT coverage_payload_json
        FROM analytics_external_scope_status
        WHERE provider_name='YOUTUBE' AND target_scope_type = ? AND target_scope_ref = ?
        """,
        (_validate_scope(target_scope_type), target_scope_ref),
    ).fetchone()
    assert row is not None
    coverage = json.loads(str(row["coverage_payload_json"]))
    requested = list(coverage.get("metric_families_requested", []))
    returned = set(coverage.get("metric_families_returned", []))
    unavailable = list(coverage.get("metric_families_unavailable", []))
    return {
        "scope": {"target_scope_type": target_scope_type, "target_scope_ref": target_scope_ref},
        "metric_family_coverage": {k: (k in returned) for k in requested},
        "historical_range_coverage": coverage.get("covered_window"),
        "incomplete_windows": [coverage.get("covered_window")] if coverage.get("incomplete_backfill", False) else [],
        "unavailable_by_permission": unavailable if status["source_availability_status"] == "PERMISSION_LIMITED" else [],
        "not_yet_synced": False,
    }


def list_sync_runs(conn: sqlite3.Connection, *, target_scope_type: str | None = None, target_scope_ref: str | None = None) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if target_scope_type:
        clauses.append("target_scope_type = ?")
        params.append(_validate_scope(target_scope_type))
    if target_scope_ref:
        clauses.append("target_scope_ref = ?")
        params.append(target_scope_ref)
    query = "SELECT * FROM analytics_external_sync_runs"
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY started_at DESC, id DESC"
    return [dict(r) for r in conn.execute(query, tuple(params)).fetchall()]


def get_sync_run_detail(conn: sqlite3.Connection, *, run_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM analytics_external_sync_runs WHERE id = ?", (int(run_id),)).fetchone()
    if row is None:
        return None
    payload = json.loads(str(row["coverage_payload_json"]))
    detail = dict(row)
    detail["missing_metric_families"] = list(payload.get("metric_families_unavailable", []))
    detail["incomplete_backfill"] = bool(payload.get("incomplete_backfill", False))
    return detail


def _record_external_event(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    provider_name: str,
    target_scope_type: str,
    target_scope_ref: str,
    run_mode: str,
    sync_state: str,
    observed_from: float | None,
    observed_to: float | None,
    created_snapshots_count: int = 0,
    partial_snapshots_count: int = 0,
    failed_snapshots_count: int = 0,
    missing_metric_families: tuple[str, ...] | list[str] = (),
    incomplete_backfill: bool = False,
    freshness_status: str = "UNKNOWN",
) -> None:
    conn.execute(
        """
        INSERT INTO analytics_external_audit_events(
            event_type, provider_name, target_scope_type, target_scope_ref, run_mode, sync_state,
            observed_from, observed_to, created_snapshots_count, partial_snapshots_count,
            failed_snapshots_count, missing_metric_families_json, incomplete_backfill, freshness_status, created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_type,
            provider_name,
            target_scope_type,
            target_scope_ref,
            run_mode,
            sync_state,
            observed_from,
            observed_to,
            created_snapshots_count,
            partial_snapshots_count,
            failed_snapshots_count,
            validate_json_payload(list(missing_metric_families), field_name="missing_metric_families_json"),
            1 if incomplete_backfill else 0,
            freshness_status,
            now_ts(),
        ),
    )
    logger.info(
        "external_sync_event=%s provider_name=%s target_scope_type=%s target_scope_ref=%s run_mode=%s sync_state=%s observed_from=%s observed_to=%s created_snapshots_count=%s partial_snapshots_count=%s failed_snapshots_count=%s missing_metric_families=%s incomplete_backfill=%s freshness_status=%s",
        event_type,
        provider_name,
        target_scope_type,
        target_scope_ref,
        run_mode,
        sync_state,
        observed_from,
        observed_to,
        created_snapshots_count,
        partial_snapshots_count,
        failed_snapshots_count,
        ",".join(missing_metric_families),
        incomplete_backfill,
        freshness_status,
    )
