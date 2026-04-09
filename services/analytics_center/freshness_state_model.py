from __future__ import annotations

from typing import Any

ANALYZER_COVERAGE_STATES: tuple[str, ...] = (
    "MISSING",
    "PARTIAL",
    "PERMISSION_LIMITED",
    "STALE",
    "REFRESHED",
)


def normalize_coverage_state(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    aliases = {
        "NOT_YET_SYNCED": "MISSING",
        "SOURCE_UNAVAILABLE": "MISSING",
        "UNKNOWN": "MISSING",
        "PARTIAL": "PARTIAL",
        "PERMISSION_LIMITED": "PERMISSION_LIMITED",
        "STALE": "STALE",
        "FRESH": "REFRESHED",
        "REFRESHED": "REFRESHED",
    }
    mapped = aliases.get(normalized, "MISSING")
    if mapped not in ANALYZER_COVERAGE_STATES:
        return "MISSING"
    return mapped


def summarize_coverage_states(*, source_states: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized = {str(k): normalize_coverage_state(v) for k, v in source_states.items()}

    if normalized and all(v == "MISSING" for v in normalized.values()):
        freshness_status = "MISSING"
        warning = "no refreshed analytics source data"
    elif any(v == "PERMISSION_LIMITED" for v in normalized.values()):
        freshness_status = "PERMISSION_LIMITED"
        warning = (
            "permission-limited sources: "
            + ", ".join(sorted([k for k, v in normalized.items() if v == "PERMISSION_LIMITED"]))
        )
    elif any(v == "STALE" for v in normalized.values()):
        freshness_status = "STALE"
        warning = "stale sources: " + ", ".join(sorted([k for k, v in normalized.items() if v == "STALE"]))
    elif any(v == "PARTIAL" for v in normalized.values()) or any(v == "MISSING" for v in normalized.values()):
        freshness_status = "PARTIAL"
        warning_sources = sorted([k for k, v in normalized.items() if v in {"PARTIAL", "MISSING"}])
        warning = "partial/missing sources: " + ", ".join(warning_sources)
    else:
        freshness_status = "REFRESHED"
        warning = None

    coverage = {
        "status": freshness_status,
        "missing_sources": sorted([k for k, v in normalized.items() if v == "MISSING"]),
        "partial_sources": sorted([k for k, v in normalized.items() if v == "PARTIAL"]),
        "permission_limited_sources": sorted([k for k, v in normalized.items() if v == "PERMISSION_LIMITED"]),
        "stale_sources": sorted([k for k, v in normalized.items() if v == "STALE"]),
        "refreshed_sources": sorted([k for k, v in normalized.items() if v == "REFRESHED"]),
        "source_states": normalized,
    }
    freshness = {"status": freshness_status, "warning": warning}
    return freshness, coverage
