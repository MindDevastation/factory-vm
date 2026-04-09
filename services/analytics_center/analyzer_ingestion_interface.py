from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from services.analytics_center.freshness_state_model import ANALYZER_COVERAGE_STATES, normalize_coverage_state
from services.analytics_center.literals import ANALYZER_REQUIRED_METRIC_DIMENSIONS
from services.analytics_center.profile_registry import CORE_ANALYZER_MODE, resolve_profile_bundle

INGESTION_SCOPE_TYPES: tuple[str, ...] = ("CHANNEL", "RELEASE_VIDEO")


@dataclass(frozen=True)
class AnalyzerIngestionRequest:
    scope_type: str
    scope_ref: str
    metric_dimensions: tuple[str, ...]
    channel_strategy_profile: str
    format_profile: str
    observed_from: float | None
    observed_to: float | None


@dataclass(frozen=True)
class AnalyzerIngestionResponse:
    scope_type: str
    scope_ref: str
    metric_dimensions_requested: tuple[str, ...]
    metric_dimensions_returned: tuple[str, ...]
    metric_dimensions_unavailable: tuple[str, ...]
    coverage_state: str
    freshness_basis: str
    payload: dict[str, Any]


class AnalyzerExternalMetricsProvider(Protocol):
    def fetch_metrics(self, request: AnalyzerIngestionRequest) -> AnalyzerIngestionResponse: ...


def _normalize_scope_type(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in INGESTION_SCOPE_TYPES:
        raise ValueError(f"invalid ingestion scope_type: {value}")
    return normalized


def _normalize_metric_dimensions(values: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    out: list[str] = []
    required = set(ANALYZER_REQUIRED_METRIC_DIMENSIONS)
    for value in values:
        key = str(value or "").strip().lower()
        if not key:
            continue
        if key not in required:
            raise ValueError(f"unsupported metric dimension for analyzer contract: {value}")
        if key not in out:
            out.append(key)
    if not out:
        raise ValueError("metric_dimensions is required")
    return tuple(out)


def normalize_ingestion_request(request: AnalyzerIngestionRequest) -> AnalyzerIngestionRequest:
    if CORE_ANALYZER_MODE != "ONE_ANALYZER_MANY_PROFILES":
        raise ValueError("analyzer core mode invariant broken")
    resolve_profile_bundle(
        channel_strategy_profile=request.channel_strategy_profile,
        format_profile=request.format_profile,
    )
    return AnalyzerIngestionRequest(
        scope_type=_normalize_scope_type(request.scope_type),
        scope_ref=str(request.scope_ref or "").strip(),
        metric_dimensions=_normalize_metric_dimensions(request.metric_dimensions),
        channel_strategy_profile=str(request.channel_strategy_profile),
        format_profile=str(request.format_profile),
        observed_from=request.observed_from,
        observed_to=request.observed_to,
    )


def normalize_ingestion_response(response: AnalyzerIngestionResponse) -> AnalyzerIngestionResponse:
    scope_type = _normalize_scope_type(response.scope_type)
    requested = _normalize_metric_dimensions(response.metric_dimensions_requested)
    returned = _normalize_metric_dimensions(response.metric_dimensions_returned) if response.metric_dimensions_returned else tuple()
    unavailable = _normalize_metric_dimensions(response.metric_dimensions_unavailable) if response.metric_dimensions_unavailable else tuple()

    if set(returned) - set(requested):
        raise ValueError("returned metrics must be subset of requested metrics")
    if set(unavailable) - set(requested):
        raise ValueError("unavailable metrics must be subset of requested metrics")

    coverage_state = normalize_coverage_state(response.coverage_state)
    if coverage_state not in ANALYZER_COVERAGE_STATES:
        raise ValueError("invalid coverage_state")

    return AnalyzerIngestionResponse(
        scope_type=scope_type,
        scope_ref=str(response.scope_ref or "").strip(),
        metric_dimensions_requested=requested,
        metric_dimensions_returned=returned,
        metric_dimensions_unavailable=unavailable,
        coverage_state=coverage_state,
        freshness_basis=str(response.freshness_basis or ""),
        payload=dict(response.payload or {}),
    )


def build_analyzer_ingestion_contract() -> dict[str, Any]:
    return {
        "core_analyzer_mode": CORE_ANALYZER_MODE,
        "supported_scope_types": list(INGESTION_SCOPE_TYPES),
        "required_metric_dimensions": list(ANALYZER_REQUIRED_METRIC_DIMENSIONS),
        "coverage_states": list(ANALYZER_COVERAGE_STATES),
        "invariants": [
            "requested_metrics_must_be_explicit",
            "returned_metrics_subset_of_requested",
            "unavailable_metrics_subset_of_requested",
            "coverage_state_explicit",
            "profile_context_required",
            "no_auto_apply",
        ],
        "execution_scope": "INTERFACE_FOUNDATION_ONLY",
    }
