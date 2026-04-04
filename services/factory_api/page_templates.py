from __future__ import annotations

from typing import Final

from services.factory_api.ux_registry import route_metadata_for_path

OVERVIEW_PAGE: Final[str] = "OVERVIEW_PAGE"
WORKSPACE_PAGE: Final[str] = "WORKSPACE_PAGE"
DETAIL_PAGE: Final[str] = "DETAIL_PAGE"
PROBLEM_LIST_PAGE: Final[str] = "PROBLEM_LIST_PAGE"
SUMMARY_REPORT_PAGE: Final[str] = "SUMMARY_REPORT_PAGE"


_ROUTE_TEMPLATE_BY_KEY: dict[str, str] = {
    "CONTROL_CENTER": OVERVIEW_PAGE,
    "PLANNER": WORKSPACE_PAGE,
    "PUBLISH_QUEUE": WORKSPACE_PAGE,
    "PUBLISH_BLOCKED": PROBLEM_LIST_PAGE,
    "PUBLISH_FAILED": PROBLEM_LIST_PAGE,
    "PUBLISH_HEALTH": PROBLEM_LIST_PAGE,
    "PUBLISH_JOB_DETAIL": DETAIL_PAGE,
    "LEGACY_JOB_DETAIL": DETAIL_PAGE,
    "UI_JOB_CREATE": WORKSPACE_PAGE,
    "UI_JOB_EDIT": DETAIL_PAGE,
    "TRACK_ANALYSIS_REPORT": SUMMARY_REPORT_PAGE,
    "CUSTOM_TAGS_DASHBOARD": SUMMARY_REPORT_PAGE,
}


def classify_page_template(*, current_path: str) -> str:
    route = route_metadata_for_path(current_path=current_path)
    if route is None:
        return OVERVIEW_PAGE
    route_key = str(route["route_key"])
    if route_key in _ROUTE_TEMPLATE_BY_KEY:
        return _ROUTE_TEMPLATE_BY_KEY[route_key]
    route_family = str(route.get("route_family") or "")
    if route_family == "entity_drilldown":
        return DETAIL_PAGE
    if route_family == "problems":
        return PROBLEM_LIST_PAGE
    if route_family == "workspaces":
        return WORKSPACE_PAGE
    return OVERVIEW_PAGE


def page_template_contract(*, current_path: str) -> dict[str, str]:
    template = classify_page_template(current_path=current_path)
    return {
        "template": template,
        "state_contract": "MF2_S1_BASELINE",
    }
