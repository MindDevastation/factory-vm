from __future__ import annotations

from collections import defaultdict
from typing import Any


def assemble_digest(items: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        key = f"{item.get('category','SYSTEM')}::{item.get('severity','INFO')}"
        grouped[key].append(item)
    blocks = []
    for key in sorted(grouped):
        category, severity = key.split("::", 1)
        bucket = grouped[key]
        blocks.append(
            {
                "category": category,
                "severity": severity,
                "count": len(bucket),
                "message_ids": [int(it["id"]) for it in bucket if it.get("id") is not None],
            }
        )
    return {"block_count": len(blocks), "blocks": blocks, "total_items": len(items)}
