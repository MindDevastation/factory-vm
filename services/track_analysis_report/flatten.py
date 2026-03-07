from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Mapping


def resolve_source_path(sources: Mapping[str, Any], source: str, path: str) -> Any:
    """Resolve a dotted path in one source mapping. Missing path returns None."""
    current = sources.get(source)
    if current is None:
        return None

    if not path:
        return current

    for segment in path.split("."):
        if isinstance(current, Mapping):
            current = current.get(segment)
            if current is None:
                return None
            continue
        return None
    return current


def flatten_value(value: Any, rule: str) -> Any:
    if value is None:
        return None

    if rule == "direct":
        return value

    if rule == "join_csv":
        if not isinstance(value, list):
            return str(value)
        return ", ".join(str(item) for item in value)

    if rule == "json_string":
        return json.dumps(value, sort_keys=True, ensure_ascii=False)

    if rule == "unix_ts_iso":
        if isinstance(value, bool):
            return None
        try:
            ts = float(value)
        except (TypeError, ValueError):
            return None
        return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")

    raise ValueError(f"Unknown flatten rule: {rule}")
