from __future__ import annotations

from typing import Any

from services.analytics_center.external_sync import METRIC_FAMILY_ALIASES
from services.analytics_center.literals import (
    ANALYZER_DEFAULT_MUTATION_POLICY,
    ANALYZER_PROFILE_AXES,
    ANALYZER_REFRESH_SELECTOR_VALUES,
    ANALYZER_REQUIRED_METRIC_DIMENSIONS,
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
            "core_mode": "ONE_ANALYZER_MANY_PROFILES",
            "profile_axes": list(ANALYZER_PROFILE_AXES),
            "default_mutation_policy": ANALYZER_DEFAULT_MUTATION_POLICY,
            "refresh_selector_values": list(ANALYZER_REFRESH_SELECTOR_VALUES),
        },
        "required_metric_dimensions": list(ANALYZER_REQUIRED_METRIC_DIMENSIONS),
        "implemented_metric_dimensions": list(implemented_metric_dimensions),
        "missing_required_metric_dimensions": list(missing_required_metrics),
        "mandatory_scope_coverage": scope_coverage,
        "completeness": "FOUNDATION_ONLY",
        "non_goals_in_this_slice": [
            "telegram implementation",
            "planning assistant implementation",
            "chart/export polish",
            "cross-domain auto-apply",
        ],
    }
