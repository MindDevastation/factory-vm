from __future__ import annotations

import json
from typing import Any


def _rows(conn: Any, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(query, params).fetchall()]


def aggregate_overview(conn: Any) -> dict[str, Any]:
    channel_rows = _rows(
        conn,
        """
        SELECT entity_ref AS channel_ref, COUNT(*) AS snapshot_count
        FROM analytics_snapshots
        WHERE entity_type = 'CHANNEL' AND is_current = 1
        GROUP BY entity_ref
        ORDER BY snapshot_count DESC
        """,
    )
    anomaly_rows = _rows(
        conn,
        "SELECT scope_ref, comparison_family AS family, variance_class FROM analytics_comparison_snapshots WHERE is_current = 1 AND variance_class IN ('ANOMALY','RISK') ORDER BY created_at DESC LIMIT 20"
    )
    rec_rows = _rows(
        conn,
        "SELECT recommendation_scope_ref, recommendation_family, severity_class, confidence_class FROM analytics_recommendation_snapshots WHERE is_current = 1 AND lifecycle_status = 'OPEN' ORDER BY created_at DESC LIMIT 20",
    )
    summary_cards = [
        {"card": "channels_with_snapshots", "value": len(channel_rows)},
        {"card": "open_recommendations", "value": len(rec_rows)},
        {"card": "active_anomalies", "value": len(anomaly_rows)},
    ]
    detail_blocks = [
        {"table": "channel_summary", "rows": channel_rows},
        {"table": "risk_highlights", "rows": anomaly_rows},
    ]
    return {"summary_cards": summary_cards, "detail_blocks": detail_blocks, "anomaly_risk_markers": anomaly_rows, "recommendation_summary": rec_rows}


def aggregate_scope(conn: Any, *, scope_type: str, scope_ref: str) -> dict[str, Any]:
    predictions = _rows(
        conn,
        "SELECT prediction_family, variance_class, confidence_class, predicted_label FROM analytics_prediction_snapshots WHERE scope_type=? AND scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, scope_ref),
    )
    comparisons = _rows(
        conn,
        "SELECT comparison_family, variance_class, delta_payload_json FROM analytics_comparison_snapshots WHERE scope_type=? AND scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, scope_ref),
    )
    kpis = _rows(
        conn,
        "SELECT kpi_family, kpi_code, status_class FROM analytics_operational_kpi_snapshots WHERE scope_type=? AND scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, scope_ref),
    )
    recs = _rows(
        conn,
        "SELECT id, recommendation_family, severity_class, confidence_class, target_domain, explainability_payload_json FROM analytics_recommendation_snapshots WHERE recommendation_scope_type=? AND recommendation_scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, scope_ref),
    )
    anomalies = [r for r in comparisons if str(r.get("variance_class")) in {"ANOMALY", "RISK"}] + [r for r in predictions if str(r.get("variance_class")) in {"ANOMALY", "RISK"}] + [r for r in kpis if str(r.get("status_class")) in {"ANOMALY", "RISK"}]
    return {
        "summary_cards": [
            {"card": "predictions", "value": len(predictions)},
            {"card": "comparisons", "value": len(comparisons)},
            {"card": "kpis", "value": len(kpis)},
            {"card": "recommendations", "value": len(recs)},
        ],
        "detail_blocks": [
            {"table": "predictions", "rows": predictions},
            {"table": "comparisons", "rows": comparisons},
            {"table": "kpis", "rows": kpis},
        ],
        "anomaly_risk_markers": anomalies,
        "recommendation_summary": recs,
    }


def aggregate_anomalies(conn: Any) -> dict[str, Any]:
    kpi = _rows(conn, "SELECT scope_type, scope_ref, kpi_family, kpi_code, status_class FROM analytics_operational_kpi_snapshots WHERE is_current=1 AND status_class IN ('ANOMALY','RISK')")
    cmp = _rows(conn, "SELECT scope_type, scope_ref, comparison_family, variance_class FROM analytics_comparison_snapshots WHERE is_current=1 AND variance_class IN ('ANOMALY','RISK')")
    pred = _rows(conn, "SELECT scope_type, scope_ref, prediction_family, variance_class, confidence_class FROM analytics_prediction_snapshots WHERE is_current=1 AND variance_class IN ('ANOMALY','RISK')")
    rows = [{**r, "source": "KPI"} for r in kpi] + [{**r, "source": "COMPARISON"} for r in cmp] + [{**r, "source": "PREDICTION"} for r in pred]
    return {"summary_cards": [{"card": "anomaly_items", "value": len(rows)}], "detail_blocks": [{"table": "problematic_units", "rows": rows}], "anomaly_risk_markers": rows, "recommendation_summary": []}


def aggregate_recommendations(conn: Any) -> dict[str, Any]:
    recs = _rows(conn, "SELECT id, recommendation_scope_type, recommendation_scope_ref, recommendation_family, severity_class, confidence_class, lifecycle_status, target_domain, explainability_payload_json FROM analytics_recommendation_snapshots ORDER BY created_at DESC")
    for r in recs:
        try:
            r["explainability_payload_json"] = json.loads(str(r["explainability_payload_json"]))
        except Exception:
            pass
    return {
        "summary_cards": [{"card": "recommendation_queue", "count": len(recs)}],
        "detail_blocks": [{"table": "recommendation_list", "rows": recs}],
        "anomaly_risk_markers": [],
        "recommendation_summary": recs,
    }
