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
