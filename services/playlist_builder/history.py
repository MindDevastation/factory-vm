from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from services.playlist_builder.models import PlaylistHistoryEntry


def track_set_overlap(current: Iterable[int], previous: Iterable[int]) -> float:
    a = set(current)
    b = set(previous)
    if not a:
        return 0.0
    return len(a & b) / len(a)


def novelty_against_previous(current: Iterable[int], previous: Iterable[int]) -> float:
    a = set(current)
    if not a:
        return 0.0
    return 1.0 - (len(a & set(previous)) / len(a))


def ordered_sequence_overlap(current: Iterable[int], previous: Iterable[int]) -> float:
    a = tuple(current)
    b = tuple(previous)
    if len(a) < 2 or len(b) < 2:
        return 0.0
    a_pairs = {(a[i], a[i + 1]) for i in range(len(a) - 1)}
    b_pairs = {(b[i], b[i + 1]) for i in range(len(b) - 1)}
    return len(a_pairs & b_pairs) / max(1, len(a_pairs))


def position_overlap(current: Iterable[int], previous: Iterable[int]) -> float:
    a = tuple(current)
    b = tuple(previous)
    if not a:
        return 0.0
    n = min(len(a), len(b))
    same = sum(1 for i in range(n) if a[i] == b[i])
    return same / len(a)


def prefix_overlap(current: Iterable[int], previous: Iterable[int], n: int) -> float:
    if n <= 0:
        return 0.0
    a = tuple(current)
    b = tuple(previous)
    common = 0
    for idx in range(n):
        if idx >= len(a) or idx >= len(b) or a[idx] != b[idx]:
            break
        common += 1
    return common / n


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
    total_a = sum(a_counts.values()) or 1
    total_b = sum(b_counts.values()) or 1
    l1 = sum(abs((a_counts[k] / total_a) - (b_counts[k] / total_b)) for k in all_keys)
    return 1.0 - (l1 / 2.0)


def list_effective_history(conn: object, *, channel_slug: str, window: int) -> list[PlaylistHistoryEntry]:
    rows = conn.execute(
        """
        SELECT id, job_id, history_stage, created_at
        FROM playlist_history
        WHERE channel_slug = ? AND is_active = 1
        ORDER BY datetime(created_at) DESC, id DESC
        """,
        (channel_slug,),
    ).fetchall()
    by_job: dict[int, dict] = {}
    passthrough: list[dict] = []
    for row in rows:
        job_id = row["job_id"]
        if job_id is None:
            passthrough.append(row)
            continue
        jid = int(job_id)
        existing = by_job.get(jid)
        stage = str(row["history_stage"]).upper()
        if existing is None:
            by_job[jid] = row
            continue
        existing_stage = str(existing["history_stage"]).upper()
        if existing_stage == "COMMITTED":
            continue
        if stage == "COMMITTED":
            by_job[jid] = row

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
