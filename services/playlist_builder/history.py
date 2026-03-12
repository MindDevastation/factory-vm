from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from services.playlist_builder.models import PlaylistHistoryEntry


def track_set_overlap(current: Iterable[int], previous: Iterable[int]) -> float:
    a = set(current)
    b = set(previous)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def novelty_against_previous(current: Iterable[int], previous: Iterable[int]) -> float:
    a = set(current)
    if not a:
        return 0.0
    return 1.0 - (len(a & set(previous)) / len(a))


def ordered_sequence_overlap(current: Iterable[int], previous: Iterable[int]) -> float:
    a = tuple(current)
    b = tuple(previous)
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    same = sum(1 for i in range(n) if a[i] == b[i])
    return same / max(len(a), len(b))


def position_overlap(current: Iterable[int], previous: Iterable[int]) -> float:
    return ordered_sequence_overlap(current, previous)


def prefix_overlap(current: Iterable[int], previous: Iterable[int], n: int) -> float:
    if n <= 0:
        return 0.0
    a = tuple(current)[:n]
    b = tuple(previous)[:n]
    if len(a) < n or len(b) < n:
        return 0.0
    same = sum(1 for i in range(n) if a[i] == b[i])
    return same / n


def batch_distribution_overlap(current_batches: Iterable[str | None], previous_batches: Iterable[str | None]) -> float:
    a_counts: dict[str, int] = defaultdict(int)
    b_counts: dict[str, int] = defaultdict(int)
    for value in current_batches:
        if value:
            a_counts[value] += 1
    for value in previous_batches:
        if value:
            b_counts[value] += 1
    if not a_counts and not b_counts:
        return 1.0
    all_keys = set(a_counts) | set(b_counts)
    if not all_keys:
        return 1.0
    total_a = sum(a_counts.values()) or 1
    total_b = sum(b_counts.values()) or 1
    return sum(min(a_counts[k] / total_a, b_counts[k] / total_b) for k in all_keys)


def list_effective_history(conn: object, *, channel_slug: str, window: int) -> list[PlaylistHistoryEntry]:
    rows = conn.execute(
        """
        SELECT id, job_id, history_stage, created_at
        FROM playlist_history
        WHERE channel_slug = ? AND is_active = 1
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        (channel_slug, max(window * 3, window)),
    ).fetchall()
    by_job: dict[int, dict] = {}
    passthrough: list[dict] = []
    for row in rows:
        job_id = row["job_id"]
        if job_id is None:
            passthrough.append(row)
            continue
        existing = by_job.get(int(job_id))
        if existing is None:
            by_job[int(job_id)] = row
            continue
        if str(row["history_stage"]).upper() == "COMMITTED":
            by_job[int(job_id)] = row

    effective_rows = sorted(list(by_job.values()) + passthrough, key=lambda r: (r["created_at"], r["id"]), reverse=True)[:window]
    history_ids = [int(r["id"]) for r in effective_rows]
    if not history_ids:
        return []

    placeholders = ",".join("?" for _ in history_ids)
    item_rows = conn.execute(
        f"""
        SELECT history_id, track_pk, position_index, month_batch
        FROM playlist_history_items
        WHERE history_id IN ({placeholders})
        ORDER BY history_id ASC, position_index ASC
        """,
        tuple(history_ids),
    ).fetchall()

    grouped: dict[int, list[dict]] = defaultdict(list)
    for item in item_rows:
        grouped[int(item["history_id"])].append(item)

    result: list[PlaylistHistoryEntry] = []
    by_id = {int(r["id"]): r for r in effective_rows}
    for hid in history_ids:
        row = by_id[hid]
        tracks = tuple(int(i["track_pk"]) for i in grouped.get(hid, []))
        batches = tuple(i["month_batch"] for i in grouped.get(hid, []))
        result.append(
            PlaylistHistoryEntry(
                history_id=hid,
                job_id=int(row["job_id"]) if row["job_id"] is not None else None,
                history_stage=str(row["history_stage"]),
                tracks=tracks,
                month_batches=batches,
            )
        )
    return result


def position_memory_risk(track_pk: int, slot: int, history: list[PlaylistHistoryEntry]) -> float:
    if not history:
        return 0.0
    seen = 0
    for entry in history:
        if slot < len(entry.tracks) and int(entry.tracks[slot]) == int(track_pk):
            seen += 1
    return seen / len(history)
