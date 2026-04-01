from __future__ import annotations

from services.analytics_center.write_service import SnapshotWriteInput
from services.common import db as dbm


def make_snapshot_input(
    *,
    entity_type: str,
    entity_ref: str,
    source_family: str = "INTERNAL_OPERATIONAL",
    window_type: str = "LAST_KNOWN_CURRENT",
    snapshot_status: str = "CURRENT",
    freshness_status: str = "FRESH",
    payload_json: object | None = None,
    is_current: bool = True,
    comparison_baseline_snapshot_id: int | None = None,
) -> SnapshotWriteInput:
    return SnapshotWriteInput(
        entity_type=entity_type,
        entity_ref=entity_ref,
        source_family=source_family,
        window_type=window_type,
        snapshot_status=snapshot_status,
        freshness_status=freshness_status,
        payload_json=payload_json if payload_json is not None else {"metric": 1},
        explainability_json={"primary_reason": "r", "supporting_signals": ["s"], "remediation_hint": "h", "baseline_context": {}},
        lineage_json={"sources": ["internal"]},
        anomaly_markers_json=["none"],
        captured_at=dbm.now_ts(),
        is_current=is_current,
        comparison_baseline_snapshot_id=comparison_baseline_snapshot_id,
    )


def make_sync_run_payload(
    *,
    target_scope_type: str = "CHANNEL",
    target_scope_ref: str = "darkwood-reverie",
    run_mode: str = "MANUAL_REFRESH",
) -> dict:
    return {
        "provider_name": "YOUTUBE",
        "target_scope_type": target_scope_type,
        "target_scope_ref": target_scope_ref,
        "run_mode": run_mode,
        "metric_families_requested": ["views", "impressions", "ctr"],
        "observed_from": dbm.now_ts() - 86400.0,
        "observed_to": dbm.now_ts(),
        "freshness_basis": "window_end",
    }


def make_coverage_payload_inputs() -> dict:
    return {
        "metric_families_requested": ["views", "impressions", "ctr", "watch_time"],
        "metric_families_returned": ["views", "impressions"],
        "metric_families_unavailable": ["ctr"],
        "covered_window": {"from": dbm.now_ts() - 86400.0, "to": dbm.now_ts()},
        "incomplete_backfill": True,
        "freshness_basis": "window_end",
    }
