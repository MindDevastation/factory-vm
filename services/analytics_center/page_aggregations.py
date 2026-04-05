from __future__ import annotations

import json
import time
from typing import Any

from services.analytics_center.helpers import canonicalize_scope_ref


def _rows(conn: Any, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(query, params).fetchall()]


_FRESHNESS_STALE_AFTER_SECONDS = 7 * 86400


def _state_from_timestamp(*, count: int, latest_ts: float | None) -> str:
    if int(count) <= 0 or latest_ts is None:
        return "MISSING"
    age_seconds = max(0.0, time.time() - float(latest_ts))
    return "STALE" if age_seconds > _FRESHNESS_STALE_AFTER_SECONDS else "FRESH"


def _overall_freshness(source_states: dict[str, str]) -> dict[str, Any]:
    states = list(source_states.values())
    if states and all(state == "MISSING" for state in states):
        return {"status": "MISSING", "warning": "no canonical analytics source data"}
    if "STALE" in states:
        stale_sources = sorted([k for k, v in source_states.items() if v == "STALE"])
        return {"status": "STALE", "warning": f"stale upstream sources: {', '.join(stale_sources)}"}
    if "PARTIAL" in states or "MISSING" in states:
        degraded_sources = sorted([k for k, v in source_states.items() if v in {"PARTIAL", "MISSING"}])
        return {"status": "PARTIAL", "warning": f"degraded upstream sources: {', '.join(degraded_sources)}"}
    return {"status": "FRESH", "warning": None}


def _coverage_summary(source_states: dict[str, str]) -> dict[str, Any]:
    missing_sources = sorted([k for k, v in source_states.items() if v == "MISSING"])
    stale_sources = sorted([k for k, v in source_states.items() if v == "STALE"])
    if source_states and all(v == "MISSING" for v in source_states.values()):
        status = "NO_DATA"
    elif not missing_sources and not stale_sources:
        status = "FULL"
    else:
        status = "PARTIAL"
    return {"status": status, "missing_sources": missing_sources, "stale_sources": stale_sources, "source_states": dict(source_states)}


def _time_window_cutoff(time_window: str | None) -> float | None:
    value = str(time_window or "").strip().lower()
    if value in {"", "all", "latest"}:
        return None
    mapping = {"24h": 1, "7d": 7, "30d": 30, "90d": 90}
    days = mapping.get(value)
    if days is None:
        return None
    return time.time() - float(days * 86400)


def _apply_time_window(rows: list[dict[str, Any]], *, time_window: str | None) -> list[dict[str, Any]]:
    cutoff = _time_window_cutoff(time_window)
    if cutoff is None:
        return rows
    filtered: list[dict[str, Any]] = []
    for row in rows:
        ts = row.get("created_at")
        if ts is None:
            filtered.append(row)
            continue
        try:
            if float(ts) >= cutoff:
                filtered.append(row)
        except Exception:
            filtered.append(row)
    return filtered


def _row_freshness_state(row: dict[str, Any]) -> str:
    created_at = row.get("created_at")
    if created_at is None:
        return "MISSING"
    try:
        age_seconds = max(0.0, time.time() - float(created_at))
    except Exception:
        return "MISSING"
    return "STALE" if age_seconds > _FRESHNESS_STALE_AFTER_SECONDS else "FRESH"


def compute_page_freshness(conn: Any, *, page_scope: str, scope_ref: str | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=page_scope, scope_ref=str(scope_ref or ""))
    entity_scope = {
        "OVERVIEW": None,
        "CHANNEL": ("CHANNEL", canonical_scope_ref),
        "RELEASE": ("RELEASE", str(scope_ref or "")),
        "BATCH_MONTH": ("BATCH", str(scope_ref or "")),
    }.get(page_scope)

    if entity_scope is None:
        snap = conn.execute(
            """
            SELECT COUNT(*) AS c, MAX(captured_at) AS latest_ts,
                   SUM(CASE WHEN freshness_status='STALE' THEN 1 ELSE 0 END) AS stale_count,
                   SUM(CASE WHEN freshness_status IN ('PARTIAL','UNKNOWN') THEN 1 ELSE 0 END) AS degraded_count
            FROM analytics_snapshots
            WHERE is_current = 1
            """
        ).fetchone()
    else:
        snap = conn.execute(
            """
            SELECT COUNT(*) AS c, MAX(captured_at) AS latest_ts,
                   SUM(CASE WHEN freshness_status='STALE' THEN 1 ELSE 0 END) AS stale_count,
                   SUM(CASE WHEN freshness_status IN ('PARTIAL','UNKNOWN') THEN 1 ELSE 0 END) AS degraded_count
            FROM analytics_snapshots
            WHERE is_current = 1 AND entity_type = ? AND entity_ref = ?
            """,
            (entity_scope[0], entity_scope[1]),
        ).fetchone()
    snapshot_state = _state_from_timestamp(count=int(snap["c"]), latest_ts=snap["latest_ts"])
    if int(snap["c"]) > 0 and int(snap["stale_count"] or 0) > 0:
        snapshot_state = "STALE"
    elif int(snap["c"]) > 0 and int(snap["degraded_count"] or 0) > 0 and snapshot_state != "STALE":
        snapshot_state = "PARTIAL"

    def _scope_count_latest(table: str, scope_col: str, ref_col: str) -> tuple[int, float | None]:
        if page_scope in {"OVERVIEW", "ANOMALIES", "RECOMMENDATIONS", "REPORTS_EXPORTS"}:
            row = conn.execute(f"SELECT COUNT(*) AS c, MAX(created_at) AS latest_ts FROM {table} WHERE is_current = 1").fetchone()
        else:
            row = conn.execute(
                f"SELECT COUNT(*) AS c, MAX(created_at) AS latest_ts FROM {table} WHERE is_current = 1 AND {scope_col} = ? AND {ref_col} = ?",
                (page_scope, canonical_scope_ref if page_scope == "CHANNEL" else str(scope_ref or "")),
            ).fetchone()
        return int(row["c"]), row["latest_ts"]

    kpi_count, kpi_latest = _scope_count_latest("analytics_operational_kpi_snapshots", "scope_type", "scope_ref")
    cmp_count, cmp_latest = _scope_count_latest("analytics_comparison_snapshots", "scope_type", "scope_ref")
    pred_count, pred_latest = _scope_count_latest("analytics_prediction_snapshots", "scope_type", "scope_ref")
    if page_scope in {"OVERVIEW", "ANOMALIES", "RECOMMENDATIONS", "REPORTS_EXPORTS"}:
        rec = conn.execute("SELECT COUNT(*) AS c, MAX(created_at) AS latest_ts FROM analytics_recommendation_snapshots WHERE is_current = 1").fetchone()
    else:
        rec = conn.execute(
            "SELECT COUNT(*) AS c, MAX(created_at) AS latest_ts FROM analytics_recommendation_snapshots WHERE is_current = 1 AND recommendation_scope_type = ? AND recommendation_scope_ref = ?",
            (page_scope, canonical_scope_ref if page_scope == "CHANNEL" else str(scope_ref or "")),
        ).fetchone()

    source_states = {
        "analytics_snapshots": snapshot_state,
        "analytics_operational_kpi_snapshots": _state_from_timestamp(count=kpi_count, latest_ts=kpi_latest),
        "analytics_comparison_snapshots": _state_from_timestamp(count=cmp_count, latest_ts=cmp_latest),
        "analytics_prediction_snapshots": _state_from_timestamp(count=pred_count, latest_ts=pred_latest),
        "analytics_recommendation_snapshots": _state_from_timestamp(count=int(rec["c"]), latest_ts=rec["latest_ts"]),
    }
    return _overall_freshness(source_states), _coverage_summary(source_states)


def aggregate_overview(conn: Any, *, time_window: str | None = None, freshness: str | None = None) -> dict[str, Any]:
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
        "SELECT scope_ref, comparison_family AS family, variance_class, created_at FROM analytics_comparison_snapshots WHERE is_current = 1 AND variance_class IN ('ANOMALY','RISK') ORDER BY created_at DESC LIMIT 100"
    )
    rec_rows = _rows(
        conn,
        "SELECT recommendation_scope_ref, recommendation_family, severity_class, confidence_class, lifecycle_status, created_at FROM analytics_recommendation_snapshots WHERE is_current = 1 AND lifecycle_status = 'OPEN' ORDER BY created_at DESC LIMIT 100",
    )
    anomaly_rows = _apply_time_window(anomaly_rows, time_window=time_window)
    rec_rows = _apply_time_window(rec_rows, time_window=time_window)
    if freshness:
        fresh = str(freshness).upper()
        if fresh in {"FRESH", "STALE", "PARTIAL", "MISSING"}:
            anomaly_rows = [r for r in anomaly_rows if _row_freshness_state(r) == fresh]
            rec_rows = [r for r in rec_rows if _row_freshness_state(r) == fresh]

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


def aggregate_scope(conn: Any, *, scope_type: str, scope_ref: str, filters: dict[str, Any] | None = None) -> dict[str, Any]:
    filters = dict(filters or {})
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=scope_type, scope_ref=scope_ref)
    predictions = _rows(
        conn,
        "SELECT prediction_family, variance_class, confidence_class, predicted_label, created_at FROM analytics_prediction_snapshots WHERE scope_type=? AND scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, canonical_scope_ref),
    )
    comparisons = _rows(
        conn,
        "SELECT comparison_family, variance_class, delta_payload_json, created_at FROM analytics_comparison_snapshots WHERE scope_type=? AND scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, canonical_scope_ref),
    )
    kpis = _rows(
        conn,
        "SELECT kpi_family, kpi_code, status_class, created_at FROM analytics_operational_kpi_snapshots WHERE scope_type=? AND scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, canonical_scope_ref),
    )
    recs = _rows(
        conn,
        "SELECT id, recommendation_family, severity_class, confidence_class, target_domain, lifecycle_status, explainability_payload_json, created_at FROM analytics_recommendation_snapshots WHERE recommendation_scope_type=? AND recommendation_scope_ref=? AND is_current=1 ORDER BY created_at DESC",
        (scope_type, canonical_scope_ref),
    )

    severity = str(filters.get("severity") or "").upper()
    confidence = str(filters.get("confidence") or "").upper()
    rec_family = str(filters.get("recommendation_family") or "")
    anomaly_status = str(filters.get("anomaly_risk_status") or "").upper()
    time_window = filters.get("time_window")

    predictions = _apply_time_window(predictions, time_window=time_window)
    comparisons = _apply_time_window(comparisons, time_window=time_window)
    kpis = _apply_time_window(kpis, time_window=time_window)
    recs = _apply_time_window(recs, time_window=time_window)

    if confidence:
        predictions = [r for r in predictions if str(r.get("confidence_class") or "").upper() == confidence]
        recs = [r for r in recs if str(r.get("confidence_class") or "").upper() == confidence]
    if severity:
        recs = [r for r in recs if str(r.get("severity_class") or "").upper() == severity]
    if rec_family:
        recs = [r for r in recs if str(r.get("recommendation_family") or "") == rec_family]
    if anomaly_status in {"ANOMALY", "RISK", "NORMAL"}:
        predictions = [r for r in predictions if str(r.get("variance_class") or "").upper() == anomaly_status]
        comparisons = [r for r in comparisons if str(r.get("variance_class") or "").upper() == anomaly_status]
        kpis = [r for r in kpis if str(r.get("status_class") or "").upper() == anomaly_status]

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


def aggregate_anomalies(conn: Any, *, filters: dict[str, Any] | None = None) -> dict[str, Any]:
    filters = dict(filters or {})
    scope_type = str(filters.get("scope_type") or "").upper()
    severity = str(filters.get("severity") or "").upper()
    confidence = str(filters.get("confidence") or "").upper()
    recommendation_family = str(filters.get("recommendation_family") or "")
    target_domain = str(filters.get("target_domain") or "")

    kpi = _rows(conn, "SELECT scope_type, scope_ref, kpi_family, kpi_code, status_class, created_at FROM analytics_operational_kpi_snapshots WHERE is_current=1 AND status_class IN ('ANOMALY','RISK')")
    cmp = _rows(conn, "SELECT scope_type, scope_ref, comparison_family, variance_class, created_at FROM analytics_comparison_snapshots WHERE is_current=1 AND variance_class IN ('ANOMALY','RISK')")
    pred = _rows(conn, "SELECT scope_type, scope_ref, prediction_family, variance_class, confidence_class, created_at FROM analytics_prediction_snapshots WHERE is_current=1 AND variance_class IN ('ANOMALY','RISK')")
    recs = _rows(conn, "SELECT recommendation_scope_type, recommendation_scope_ref, recommendation_family, target_domain, severity_class FROM analytics_recommendation_snapshots WHERE is_current = 1")

    allow_refs: set[tuple[str, str]] | None = None
    if recommendation_family or target_domain or severity:
        allow = [r for r in recs if (not recommendation_family or str(r.get("recommendation_family") or "") == recommendation_family) and (not target_domain or str(r.get("target_domain") or "") == target_domain) and (not severity or str(r.get("severity_class") or "").upper() == severity)]
        allow_refs = {(str(r["recommendation_scope_type"]), str(r["recommendation_scope_ref"])) for r in allow}

    rows = [{**r, "source": "KPI"} for r in kpi] + [{**r, "source": "COMPARISON"} for r in cmp] + [{**r, "source": "PREDICTION"} for r in pred]
    if scope_type:
        rows = [r for r in rows if str(r.get("scope_type") or "").upper() == scope_type]
    if confidence:
        rows = [r for r in rows if r.get("source") != "PREDICTION" or str(r.get("confidence_class") or "").upper() == confidence]
    if allow_refs is not None:
        rows = [r for r in rows if (str(r.get("scope_type") or ""), str(r.get("scope_ref") or "")) in allow_refs]

    return {"summary_cards": [{"card": "anomaly_items", "value": len(rows)}], "detail_blocks": [{"table": "problematic_units", "rows": rows}], "anomaly_risk_markers": rows, "recommendation_summary": []}


def aggregate_recommendations(conn: Any, *, filters: dict[str, Any] | None = None) -> dict[str, Any]:
    filters = dict(filters or {})
    scope_type = str(filters.get("scope_type") or "").upper()
    recommendation_family = str(filters.get("recommendation_family") or "")
    target_domain = str(filters.get("target_domain") or "")
    severity = str(filters.get("severity") or "").upper()
    confidence = str(filters.get("confidence") or "").upper()
    lifecycle_status = str(filters.get("lifecycle_status") or "").upper()

    clauses = ["1=1"]
    params: list[Any] = []
    if scope_type:
        clauses.append("recommendation_scope_type = ?")
        params.append(scope_type)
    if recommendation_family:
        clauses.append("recommendation_family = ?")
        params.append(recommendation_family)
    if target_domain:
        clauses.append("target_domain = ?")
        params.append(target_domain)
    if severity:
        clauses.append("severity_class = ?")
        params.append(severity)
    if confidence:
        clauses.append("confidence_class = ?")
        params.append(confidence)
    if lifecycle_status:
        clauses.append("lifecycle_status = ?")
        params.append(lifecycle_status)

    recs = _rows(
        conn,
        "SELECT id, recommendation_scope_type, recommendation_scope_ref, recommendation_family, severity_class, confidence_class, lifecycle_status, target_domain, explainability_payload_json, created_at FROM analytics_recommendation_snapshots WHERE "
        + " AND ".join(clauses)
        + " ORDER BY created_at DESC",
        tuple(params),
    )
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
