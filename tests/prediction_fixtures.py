from __future__ import annotations

from services.analytics_center.helpers import canonicalize_scope_ref
from services.common import db as dbm


def seed_mf4_mixed_input_snapshots(conn, *, scope_type: str = "CHANNEL", scope_ref: str = "darkwood-reverie") -> None:
    now = dbm.now_ts()
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=scope_type, scope_ref=scope_ref)
    entity_type = scope_type.replace("BATCH_MONTH", "BATCH")
    payload_external = {"views": 1200.0, "impressions": 3500.0, "ctr": 0.08}
    payload_internal = {"queue_depth": 4, "publish_latency": 1800.0, "retry_ratio": 0.1}
    conn.execute(
        """
        INSERT INTO analytics_snapshots(
            entity_type, entity_ref, source_family, window_type, snapshot_status, freshness_status,
            payload_json, explainability_json, lineage_json, anomaly_markers_json, normalized_scope_key,
            captured_at, is_current, created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            entity_type,
            canonical_scope_ref,
            "EXTERNAL_YOUTUBE",
            "BOUNDED_WINDOW",
            "CURRENT",
            "FRESH",
            __import__("json").dumps(payload_external, sort_keys=True),
            "{}",
            "{}",
            "[]",
            f"{entity_type}::{canonical_scope_ref}::EXTERNAL_YOUTUBE::BOUNDED_WINDOW",
            now - 1000.0,
            1,
            now - 1000.0,
            now - 1000.0,
        ),
    )
    conn.execute(
        """
        INSERT INTO analytics_snapshots(
            entity_type, entity_ref, source_family, window_type, snapshot_status, freshness_status,
            payload_json, explainability_json, lineage_json, anomaly_markers_json, normalized_scope_key,
            captured_at, is_current, created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            entity_type,
            canonical_scope_ref,
            "INTERNAL_OPERATIONAL",
            "ROLLING_BASELINE",
            "CURRENT",
            "FRESH",
            __import__("json").dumps(payload_internal, sort_keys=True),
            "{}",
            "{}",
            "[]",
            f"{entity_type}::{canonical_scope_ref}::INTERNAL_OPERATIONAL::ROLLING_BASELINE",
            now - 500.0,
            1,
            now - 500.0,
            now - 500.0,
        ),
    )


def seed_mf4_operational_kpi_snapshot(conn, *, scope_type: str = "CHANNEL", scope_ref: str = "darkwood-reverie") -> None:
    now = dbm.now_ts()
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=scope_type, scope_ref=scope_ref)
    run_id = int(
        conn.execute(
            """
            INSERT INTO analytics_operational_kpi_runs(
                target_scope_type, target_scope_ref, recompute_mode, run_state, started_at, completed_at,
                computed_kpi_count, anomaly_count, risk_count
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (scope_type, canonical_scope_ref, "FULL_RECOMPUTE", "SUCCEEDED", now - 100.0, now - 50.0, 1, 0, 0),
        ).lastrowid
    )
    conn.execute(
        """
        INSERT INTO analytics_operational_kpi_snapshots(
            run_id, scope_type, scope_ref, kpi_family, kpi_code, status_class,
            observed_from, observed_to, is_current, value_payload_json, explainability_payload_json, source_snapshot_refs_json,
            created_at, updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            scope_type,
            canonical_scope_ref,
            "PIPELINE_TIMING",
            "pipeline_latency",
            "NORMAL",
            now - 3600.0,
            now,
            1,
            __import__("json").dumps({"median_pipeline_latency": 1400.0}, sort_keys=True),
            __import__("json").dumps({"primary_reason": "stable"}, sort_keys=True),
            "[]",
            now - 40.0,
            now - 40.0,
        ),
    )
