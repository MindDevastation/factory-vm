from __future__ import annotations

from collections import defaultdict, deque
import time
from collections.abc import Callable
from typing import DefaultDict

REQUESTS_PER_MINUTE = 50
WINDOW_SECONDS = 60.0
GROUP_READ = "read"
GROUP_POLICY = "policy"


class InMemoryRateLimiter:
    """Simple in-memory sliding-window limiter keyed by (username, group)."""

    def __init__(
        self,
        *,
        max_requests: int = REQUESTS_PER_MINUTE,
        window_seconds: float = WINDOW_SECONDS,
        now_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._now_fn = now_fn
        self._requests: DefaultDict[tuple[str, str], deque[float]] = defaultdict(deque)

    def is_limited(self, username: str, group: str) -> bool:
        now = self._now_fn()
        key = (username, group)
        timestamps = self._requests[key]
        cutoff = now - self._window_seconds

        while timestamps and timestamps[0] <= cutoff:
            timestamps.popleft()

        if len(timestamps) >= self._max_requests:
            return True

        timestamps.append(now)
        return False


def endpoint_group(path: str) -> str | None:
    if path.startswith("/policy"):
        return GROUP_POLICY
    if path == "/tables" or path == "/rows":
        return GROUP_READ
    return None
