from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

from services.track_analyzer.canon import (
    canonicalize_track_filename,
    deterministic_hash_suffix,
    sanitize_title,
)

log = logging.getLogger(__name__)

_FOLDER_MIME = "application/vnd.google-apps.folder"
_CANON_WAV_RE = re.compile(r"^(\d{3})_(.+)\.wav$", re.IGNORECASE)


class DiscoverError(RuntimeError):
    pass


@dataclass(frozen=True)
class DiscoverStats:
    seen_wav: int = 0
    renamed: int = 0
    inserted: int = 0
    updated: int = 0


def discover_channel_tracks(conn: Any, drive: Any, *, gdrive_library_root_id: str, channel_slug: str) -> DiscoverStats:
    channel = _require_channel_and_canon(conn, channel_slug)
    channel_display_name = str(channel.get("display_name") or "").strip()
    if not channel_display_name:
        raise DiscoverError(f"channel display_name is empty: {channel_slug}")

    channel_folder = _find_child_folder(drive, gdrive_library_root_id, channel_display_name)
    if channel_folder is None:
        raise DiscoverError(f"channel folder not found: {channel_display_name}")

    audio_folder = _find_child_folder(drive, channel_folder.id, "Audio")
    if audio_folder is None:
        raise DiscoverError(f"audio folder not found for channel: {channel_slug}")

    stats = DiscoverStats()

    month_folders = [
        item for item in drive.list_children(audio_folder.id)
        if str(getattr(item, "mime_type", "")) == _FOLDER_MIME
    ]
    for month in sorted(month_folders, key=lambda i: str(i.name).lower()):
        stats = _process_month(conn, drive, channel_slug=channel_slug, month_folder=month, stats=stats)

    return stats


def _process_month(conn: Any, drive: Any, *, channel_slug: str, month_folder: Any, stats: DiscoverStats) -> DiscoverStats:
    children = list(drive.list_children(month_folder.id))
    by_name = {str(item.name): item for item in children}

    seen_wav = stats.seen_wav
    renamed = stats.renamed
    inserted = stats.inserted
    updated = stats.updated

    for item in sorted(children, key=lambda i: str(i.name).lower()):
        if str(getattr(item, "mime_type", "")) == _FOLDER_MIME:
            continue

        original_name = str(item.name)
        if not original_name.lower().endswith(".wav"):
            continue

        seen_wav += 1

        target_name = _build_target_name(conn, channel_slug=channel_slug, original_name=original_name)
        target_name = _resolve_collision(
            month_name=str(month_folder.name),
            channel_slug=channel_slug,
            file_id=str(item.id),
            original_name=original_name,
            target_name=target_name,
            by_name=by_name,
        )

        final_name = original_name
        if target_name != original_name:
            drive.update_name(str(item.id), target_name)
            renamed += 1
            by_name.pop(original_name, None)
            by_name[target_name] = item
            final_name = target_name

        track_id, title = _parse_canon_wav(final_name)
        ts = time.time()
        row = conn.execute(
            "SELECT id FROM tracks WHERE gdrive_file_id = ? LIMIT 1",
            (str(item.id),),
        ).fetchone()
        if row is not None:
            conn.execute(
                """
                UPDATE tracks
                SET channel_slug = ?, track_id = ?, filename = ?, title = ?, source = COALESCE(source, 'GDRIVE'), discovered_at = ?
                WHERE gdrive_file_id = ?
                """,
                (channel_slug, track_id, final_name, title, ts, str(item.id)),
            )
            updated += 1
            continue

        row_by_track = conn.execute(
            "SELECT id FROM tracks WHERE channel_slug = ? AND track_id = ? LIMIT 1",
            (channel_slug, track_id),
        ).fetchone()
        if row_by_track is None:
            conn.execute(
                """
                INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (channel_slug, track_id, str(item.id), "GDRIVE", final_name, title, None, None, ts, None),
            )
            inserted += 1
        else:
            log.warning(
                "track discover skip insert: channel=%s track_id=%s file_id=%s reason=track_id_exists",
                channel_slug,
                track_id,
                str(item.id),
            )

    return DiscoverStats(seen_wav=seen_wav, renamed=renamed, inserted=inserted, updated=updated)


def _build_target_name(conn: Any, *, channel_slug: str, original_name: str) -> str:
    canonical = canonicalize_track_filename(original_name)
    parsed = _parse_canon_wav_opt(canonical)
    if parsed is not None:
        track_id, title = parsed
        return f"{track_id}_{title}.wav"

    next_id = _first_free_track_id(conn, channel_slug)
    stem, _ext = os.path.splitext(original_name)
    title = sanitize_title(stem, track_id=next_id) or "Track"
    return f"{next_id}_{title}.wav"


def _resolve_collision(
    *,
    month_name: str,
    channel_slug: str,
    file_id: str,
    original_name: str,
    target_name: str,
    by_name: dict[str, Any],
) -> str:
    existing = by_name.get(target_name)
    if existing is None or str(existing.id) == file_id:
        return target_name

    parsed = _parse_canon_wav_opt(target_name)
    if parsed is None:
        return target_name

    track_id, title = parsed
    suffix = deterministic_hash_suffix(channel_slug, month_name, file_id, original_name, target_name)
    collided_name = f"{track_id}_{title}_{suffix}.wav"
    log.warning(
        "track discover collision: channel=%s month=%s file_id=%s target=%s resolved=%s",
        channel_slug,
        month_name,
        file_id,
        target_name,
        collided_name,
    )
    return collided_name


def _first_free_track_id(conn: Any, channel_slug: str) -> str:
    rows = conn.execute(
        "SELECT track_id FROM tracks WHERE channel_slug = ?",
        (channel_slug,),
    ).fetchall()
    used = {int(r["track_id"]) for r in rows if str(r.get("track_id") or "").isdigit()}
    idx = 1
    while idx in used:
        idx += 1
    return f"{idx:03d}"


def _parse_canon_wav(name: str) -> tuple[str, str]:
    parsed = _parse_canon_wav_opt(name)
    if parsed is None:
        raise DiscoverError(f"cannot parse canonical wav name: {name}")
    return parsed


def _parse_canon_wav_opt(name: str) -> tuple[str, str] | None:
    m = _CANON_WAV_RE.match(name)
    if not m:
        return None
    return m.group(1), sanitize_title(m.group(2), track_id=m.group(1)) or "Track"


def _require_channel_and_canon(conn: Any, channel_slug: str) -> dict[str, Any]:
    channel = conn.execute(
        "SELECT slug, display_name FROM channels WHERE slug = ? LIMIT 1",
        (channel_slug,),
    ).fetchone()
    if channel is None:
        raise DiscoverError("channel not found")

    in_canon_channels = conn.execute(
        "SELECT 1 FROM canon_channels WHERE value = ? LIMIT 1", (channel_slug,)
    ).fetchone()
    in_canon_thresholds = conn.execute(
        "SELECT 1 FROM canon_thresholds WHERE value = ? LIMIT 1", (channel_slug,)
    ).fetchone()
    if in_canon_channels is None or in_canon_thresholds is None:
        raise DiscoverError("CHANNEL_NOT_IN_CANON")

    return dict(channel)


def _find_child_folder(drive: Any, parent_id: str, name: str) -> Any | None:
    for item in drive.list_children(parent_id):
        if str(getattr(item, "mime_type", "")) == _FOLDER_MIME and str(item.name) == name:
            return item
    return None
