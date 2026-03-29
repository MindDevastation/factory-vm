from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def publish_lifecycle_events_path(storage_root: str) -> Path:
    return Path(storage_root).resolve() / "logs" / "publish_lifecycle_events.jsonl"


def append_publish_lifecycle_event(*, storage_root: str, event: Mapping[str, Any]) -> None:
    """Append lifecycle event in recovery-style JSONL format.

    This helper is additive-only and does not mutate publish_state.
    """

    path = publish_lifecycle_events_path(storage_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(event), ensure_ascii=False, sort_keys=True) + "\n")


def read_publish_lifecycle_events(*, storage_root: str, limit: int = 50) -> list[dict[str, Any]]:
    path = publish_lifecycle_events_path(storage_root)
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    rows: list[dict[str, Any]] = []
    for line in reversed(lines):
        if len(rows) >= max(0, int(limit)):
            break
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


__all__ = [
    "publish_lifecycle_events_path",
    "append_publish_lifecycle_event",
    "read_publish_lifecycle_events",
]
