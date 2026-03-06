from __future__ import annotations

from collections.abc import Iterable, Mapping


def parse_statuses_from_query(raw_statuses: str | None, source_statuses: Iterable[str]) -> set[str]:
    """Parse query-param statuses into a validated selection set.

    Mirrors the UI behavior in templates/index.html:
    - empty/None input yields an empty set (caller treats this as "all")
    - unknown statuses are ignored
    """

    valid_statuses = set(source_statuses)
    selected: set[str] = set()
    if not raw_statuses:
        return selected

    for value in raw_statuses.split(","):
        status = value.strip()
        if status and status in valid_statuses:
            selected.add(status)
    return selected


def serialize_statuses_to_query(selected_statuses: set[str] | None, source_statuses: list[str]) -> str:
    """Serialize selected statuses in source/backend order for query params."""

    if (
        not selected_statuses
        or len(selected_statuses) == 0
        or len(selected_statuses) == len(source_statuses)
    ):
        return ""
    return ",".join(status for status in source_statuses if status in selected_statuses)


def filter_jobs(jobs: list[Mapping[str, str]], selected_statuses: set[str] | None) -> list[Mapping[str, str]]:
    """Filter job rows by selected statuses.

    Empty/None selection returns all jobs.
    """

    if not selected_statuses:
        return jobs
    return [job for job in jobs if job.get("status", "") in selected_statuses]
