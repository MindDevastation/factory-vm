from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RouteMetadata:
    route_key: str
    path: str
    label: str
    owner_group: str
    route_family: str
    parent_route_key: str | None = None
    in_primary_nav: bool = False
    migrated_shell: bool = True


ROUTE_METADATA_REGISTRY: tuple[RouteMetadata, ...] = (
    RouteMetadata("CONTROL_CENTER", "/", "Control Center", "control_center", "control_center", in_primary_nav=True),
    RouteMetadata("PLANNER", "/ui/planner", "Planner", "workspaces", "workspaces", parent_route_key="CONTROL_CENTER", in_primary_nav=True),
    RouteMetadata("PUBLISH_QUEUE", "/ui/publish/queue", "Publish Queue", "workspaces", "workspaces", parent_route_key="CONTROL_CENTER", in_primary_nav=True),
    RouteMetadata("PUBLISH_BLOCKED", "/ui/publish/blocked", "Publish Blocked", "problems_attention", "problems", parent_route_key="PUBLISH_QUEUE"),
    RouteMetadata("PUBLISH_FAILED", "/ui/publish/failed", "Problems", "problems_attention", "problems", parent_route_key="PUBLISH_QUEUE", in_primary_nav=True),
    RouteMetadata("PUBLISH_MANUAL", "/ui/publish/manual", "Publish Manual", "workspaces", "workspaces", parent_route_key="PUBLISH_QUEUE"),
    RouteMetadata("PUBLISH_HEALTH", "/ui/publish/health", "Publish Health", "problems_attention", "problems", parent_route_key="PUBLISH_QUEUE"),
    RouteMetadata("PUBLISH_JOB_DETAIL", "/ui/publish/jobs/", "Publish Job", "entities", "entity_drilldown", parent_route_key="PUBLISH_QUEUE"),
    RouteMetadata("UI_JOB_CREATE", "/ui/jobs/create", "Create Job", "workspaces", "legacy_bridge", parent_route_key="CONTROL_CENTER"),
    RouteMetadata("UI_JOB_EDIT", "/ui/jobs/", "Edit Job", "entities", "legacy_bridge", parent_route_key="PUBLISH_QUEUE"),
    RouteMetadata("LEGACY_JOB_DETAIL", "/jobs/", "Job Detail (Legacy)", "entities", "legacy_bridge", parent_route_key="PUBLISH_QUEUE"),
    RouteMetadata("RECOVERY", "/ui/ops/recovery", "Recovery", "ops", "legacy_bridge", parent_route_key="CONTROL_CENTER", in_primary_nav=True),
    RouteMetadata("DB_VIEWER", "/ui/db-viewer", "Database", "ops", "legacy_bridge", parent_route_key="CONTROL_CENTER"),
    RouteMetadata("METADATA_TITLE_TEMPLATES", "/ui/metadata/title-templates", "Title Templates", "workspaces", "workspaces", parent_route_key="CONTROL_CENTER"),
    RouteMetadata("TRACK_ANALYSIS_REPORT", "/ui/track-catalog/analysis-report", "Track Analysis", "workspaces", "workspaces", parent_route_key="CONTROL_CENTER"),
    RouteMetadata("CUSTOM_TAGS", "/ui/track-catalog/custom-tags", "Custom Tags", "workspaces", "workspaces", parent_route_key="CONTROL_CENTER"),
    RouteMetadata("CUSTOM_TAGS_DASHBOARD", "/ui/track-catalog/custom-tags/dashboard", "Tag Dashboard", "entities", "entity_drilldown", parent_route_key="CUSTOM_TAGS"),
)

_ROUTE_BY_KEY: dict[str, RouteMetadata] = {item.route_key: item for item in ROUTE_METADATA_REGISTRY}
_ROUTE_BY_EXACT_PATH: dict[str, RouteMetadata] = {item.path: item for item in ROUTE_METADATA_REGISTRY}


def _path_matches_prefix(*, current_path: str, prefix: str) -> bool:
    if not current_path.startswith(prefix):
        return False
    if current_path == prefix:
        return True
    if prefix.endswith("/"):
        return True
    return current_path[len(prefix)] == "/"


def _find_route_by_path(current_path: str) -> RouteMetadata | None:
    exact = _ROUTE_BY_EXACT_PATH.get(current_path)
    if exact is not None:
        return exact
    candidates = [
        item
        for item in ROUTE_METADATA_REGISTRY
        if item.path != "/" and _path_matches_prefix(current_path=current_path, prefix=item.path)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda item: len(item.path))


def _find_route_by_key(route_key: str) -> RouteMetadata | None:
    return _ROUTE_BY_KEY.get(route_key)


def route_ownership_map() -> dict[str, dict[str, str | bool | None]]:
    return {
        item.path: {
            "route_key": item.route_key,
            "owner_group": item.owner_group,
            "route_family": item.route_family,
            "parent_route_key": item.parent_route_key,
            "migrated_shell": item.migrated_shell,
        }
        for item in ROUTE_METADATA_REGISTRY
    }


def _nearest_primary_parent(route: RouteMetadata | None) -> str | None:
    parent_key = route.parent_route_key if route else None
    while parent_key:
        parent = _find_route_by_key(parent_key)
        if parent is None:
            return None
        if parent.in_primary_nav:
            return parent.route_key
        parent_key = parent.parent_route_key
    return None


def primary_nav_items(*, current_path: str) -> list[dict[str, str | bool]]:
    active_route = _find_route_by_path(current_path)
    active_key = active_route.route_key if active_route else None
    inherited_active_key = None if (active_route and active_route.in_primary_nav) else _nearest_primary_parent(active_route)
    items: list[dict[str, str | bool]] = []
    for item in ROUTE_METADATA_REGISTRY:
        if not item.in_primary_nav:
            continue
        is_active = bool(active_key == item.route_key or inherited_active_key == item.route_key)
        items.append({"key": item.route_key, "label": item.label, "path": item.path, "active": is_active})
    return items


def breadcrumb_context(*, current_path: str) -> list[dict[str, str]]:
    route = _find_route_by_path(current_path)
    if route is None:
        return [control_center_entry()]
    chain: list[dict[str, str]] = [{"label": route.label, "path": route.path}]
    parent_key = route.parent_route_key
    while parent_key:
        parent = _find_route_by_key(parent_key)
        if parent is None:
            break
        chain.append({"label": parent.label, "path": parent.path})
        parent_key = parent.parent_route_key
    chain.reverse()
    if chain[0]["path"] != "/":
        chain.insert(0, control_center_entry())
    return chain


def control_center_entry() -> dict[str, str]:
    return {"label": "Control Center", "path": "/"}
