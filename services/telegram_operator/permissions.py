from __future__ import annotations

from .literals import PERMISSION_ACCESS_CLASSES, ensure_permission_access_class

_PERMISSION_RANK = {name: i for i, name in enumerate(PERMISSION_ACCESS_CLASSES)}


def permission_rank(permission_class: str) -> int:
    return int(_PERMISSION_RANK[ensure_permission_access_class(permission_class)])


def permission_allows(*, granted: str, requested: str) -> bool:
    return permission_rank(granted) >= permission_rank(requested)
