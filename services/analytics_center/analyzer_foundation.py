from __future__ import annotations

from typing import Any

from services.analytics_center.external_sync import METRIC_FAMILY_ALIASES
from services.analytics_center.analyzer_ingestion_interface import build_analyzer_ingestion_contract
from services.analytics_center.analyzer_service_foundation import ANALYZER_SYNC_STATES
from services.analytics_center.freshness_state_model import ANALYZER_COVERAGE_STATES
from services.analytics_center.literals import (
    ANALYZER_DEFAULT_MUTATION_POLICY,
    ANALYZER_PROFILE_AXES,
    ANALYZER_REFRESH_SELECTOR_VALUES,
    ANALYZER_REQUIRED_METRIC_DIMENSIONS,
)
from services.analytics_center.profile_registry import (
    CORE_ANALYZER_MODE,
    build_profile_registry_contract,
    profile_hook_fingerprint,
    resolve_profile_bundle,
)


def _status(ok: bool, *, note: str) -> dict[str, Any]:
    return {"status": "READY" if ok else "GAP", "note": note}


def build_analyzer_foundation_contract() -> dict[str, Any]:
    """MF1-S1 contract: explicit repo-vs-spec foundation coverage.

    This contract intentionally reports foundation readiness and known mandatory gaps
    without claiming feature completeness.
    """

    implemented_metric_dimensions = tuple(sorted(set(METRIC_FAMILY_ALIASES.values())))
    missing_required_metrics = tuple(
        metric for metric in ANALYZER_REQUIRED_METRIC_DIMENSIONS if metric not in implemented_metric_dimensions
    )

    sample_profile_bundle = resolve_profile_bundle(
        channel_strategy_profile="LONG_FORM_BACKGROUND_MUSIC",
        format_profile="LONG_FORM",
    )

    scope_coverage = {
        "analytics_domain_snapshot_foundation": _status(
            True,
            note="analytics snapshots/external sync/prediction/recommendation/report foundations exist",
        ),
        "one_analyzer_many_profiles_foundation_hooks": _status(
            len(ANALYZER_PROFILE_AXES) >= 2,
            note="profile axes are defined at analyzer contract level",
        ),
        "required_metrics_breadth": _status(
            len(missing_required_metrics) == 0,
            note=(
                "all required metric dimensions are normalized"
                if len(missing_required_metrics) == 0
                else "missing required metric dimensions in current normalization"
            ),
        ),
        "refresh_selector_exactness": _status(
            False,
            note="exact operator selector runtime wiring for hourly/12h/daily remains pending",
        ),
        "planning_assistant_v1_surface": _status(
            False,
            note="analyzer planning assistant scope contract remains pending in dedicated slice",
        ),
        "telegram_analyzer_surface": _status(
            False,
            note="analyzer Telegram summaries/alerts/snapshots contract remains pending",
        ),
    }

    return {
        "contract_version": "MF1-S1",
        "analyzer_model": {
            "core_mode": CORE_ANALYZER_MODE,
            "profile_axes": list(ANALYZER_PROFILE_AXES),
            "default_mutation_policy": ANALYZER_DEFAULT_MUTATION_POLICY,
            "refresh_selector_values": list(ANALYZER_REFRESH_SELECTOR_VALUES),
        },
        "required_metric_dimensions": list(ANALYZER_REQUIRED_METRIC_DIMENSIONS),
        "implemented_metric_dimensions": list(implemented_metric_dimensions),
        "missing_required_metric_dimensions": list(missing_required_metrics),
        "mandatory_scope_coverage": scope_coverage,
        "ingestion_interface_contract": build_analyzer_ingestion_contract(),
        "service_boundary_contract": {
            "write_service": "write_analyzer_snapshot",
            "read_service": "read_analyzer_snapshots",
            "sync_states": list(ANALYZER_SYNC_STATES),
            "invariants": [
                "one_analyzer_many_profiles",
                "default_no_auto_apply",
                "explicit_coverage_state_visibility",
            ],
        },
        "state_model_contract": {
            "coverage_states": list(ANALYZER_COVERAGE_STATES),
            "visibility_guarantees": [
                "missing",
                "partial",
                "permission-limited",
                "stale",
                "refreshed",
            ],
        },
        "profile_registry_contract": build_profile_registry_contract(),
        "sample_profile_effects": {
            "channel_strategy_profile": sample_profile_bundle.channel_strategy_profile,
            "format_profile": sample_profile_bundle.format_profile,
            "weighting_hooks": sample_profile_bundle.weighting_hooks,
            "baseline_hooks": sample_profile_bundle.baseline_hooks,
            "prediction_hooks": sample_profile_bundle.prediction_hooks,
            "recommendation_hooks": sample_profile_bundle.recommendation_hooks,
            "planning_hooks": sample_profile_bundle.planning_hooks,
            "hook_fingerprint": profile_hook_fingerprint(sample_profile_bundle),
        },
        "completeness": "FOUNDATION_ONLY",
        "non_goals_in_this_slice": [
            "telegram implementation",
            "planning assistant implementation",
            "chart/export polish",
            "cross-domain auto-apply",
        ],
    }
