from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.analytics_center.freshness_state_model import normalize_coverage_state
from services.analytics_center.literals import ANALYZER_DEFAULT_MUTATION_POLICY
from services.analytics_center.profile_registry import CORE_ANALYZER_MODE, profile_hook_fingerprint, resolve_profile_bundle
from services.analytics_center.read_service import SnapshotReadFilters, read_snapshots
from services.analytics_center.write_service import SnapshotWriteInput, write_snapshot

ANALYZER_SYNC_STATES: tuple[str, ...] = (
    "NOT_YET_SYNCED",
    "RUNNING",
    "SUCCEEDED",
    "PARTIAL",
    "FAILED",
)


@dataclass(frozen=True)
class AnalyzerSnapshotWriteRequest:
    entity_type: str
    entity_ref: str
    source_family: str
    window_type: str
    snapshot_status: str
    freshness_status: str
    payload_json: dict[str, Any]
    captured_at: float
    channel_strategy_profile: str
    format_profile: str
    sync_state: str
    is_current: bool = False
    comparison_baseline_snapshot_id: int | None = None
    window_start_ts: float | None = None
    window_end_ts: float | None = None
    explainability_json: dict[str, Any] | None = None
    lineage_json: dict[str, Any] | None = None
    anomaly_markers_json: list[dict[str, Any]] | None = None


@dataclass(frozen=True)
class AnalyzerSnapshotReadRequest:
    entity_type: str | None = None
    entity_ref: str | None = None
    source_family: str | None = None
    window_type: str | None = None
    current_only: bool = False


def _normalize_sync_state(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in ANALYZER_SYNC_STATES:
        raise ValueError(f"invalid analyzer sync_state: {value}")
    return normalized


def _assert_foundation_invariants() -> None:
    if CORE_ANALYZER_MODE != "ONE_ANALYZER_MANY_PROFILES":
        raise ValueError("analyzer foundation invariant broken: core mode")
    if ANALYZER_DEFAULT_MUTATION_POLICY != "NO_AUTO_APPLY":
        raise ValueError("analyzer foundation invariant broken: mutation policy")


def write_analyzer_snapshot(conn: Any, req: AnalyzerSnapshotWriteRequest) -> int:
    _assert_foundation_invariants()
    sync_state = _normalize_sync_state(req.sync_state)
    profile = resolve_profile_bundle(
        channel_strategy_profile=req.channel_strategy_profile,
        format_profile=req.format_profile,
    )

    coverage_state = normalize_coverage_state(req.freshness_status)
    lineage = {
        **(req.lineage_json or {}),
        "analyzer_foundation": {
            "core_mode": CORE_ANALYZER_MODE,
            "default_mutation_policy": ANALYZER_DEFAULT_MUTATION_POLICY,
            "sync_state": sync_state,
            "coverage_state": coverage_state,
            "profile": {
                "channel_strategy_profile": profile.channel_strategy_profile,
                "format_profile": profile.format_profile,
                "hook_fingerprint": profile_hook_fingerprint(profile),
            },
        },
    }

    snapshot = SnapshotWriteInput(
        entity_type=req.entity_type,
        entity_ref=req.entity_ref,
        source_family=req.source_family,
        window_type=req.window_type,
        snapshot_status=req.snapshot_status,
        freshness_status=req.freshness_status,
        payload_json=req.payload_json,
        explainability_json=req.explainability_json or {},
        lineage_json=lineage,
        anomaly_markers_json=req.anomaly_markers_json or [],
        captured_at=float(req.captured_at),
        is_current=bool(req.is_current),
        comparison_baseline_snapshot_id=req.comparison_baseline_snapshot_id,
        window_start_ts=req.window_start_ts,
        window_end_ts=req.window_end_ts,
    )
    return write_snapshot(conn, snapshot)


def read_analyzer_snapshots(conn: Any, req: AnalyzerSnapshotReadRequest) -> list[dict[str, Any]]:
    _assert_foundation_invariants()
    rows = read_snapshots(
        conn,
        SnapshotReadFilters(
            entity_type=req.entity_type,
            entity_ref=req.entity_ref,
            source_family=req.source_family,
            window_type=req.window_type,
            current_only=req.current_only,
        ),
    )
    for row in rows:
        row["coverage_state"] = normalize_coverage_state(row.get("freshness_status"))
    return rows
