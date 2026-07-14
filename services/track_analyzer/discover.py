from __future__ import annotations

import logging
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

from services.track_analyzer.canon import sanitize_title

log = logging.getLogger(__name__)

_FOLDER_MIME = "application/vnd.google-apps.folder"
_CANON_WAV_RE = re.compile(r"^(\d{4})_(.+)\.wav$", re.IGNORECASE)
_PREFIX_RE = re.compile(r"^(\d{3,4})(?:[_ .-]+)(.+)$")
_STACKED_PREFIX_RE = re.compile(r"^\d{3,4}(?:[_ .-]+)\d{3,4}(?:[_ .-]+)")
_DOWNLOAD_SUFFIX_RE = re.compile(r"^(?P<base>.+?)\s*\((?P<num>\d+)\)\s*$")
_NON_ALNUM_RE = re.compile(r"[^\w]+", re.UNICODE)

_MONTHS = {
    "jan": 1, "january": 1, "январь": 1, "января": 1, "січень": 1, "січня": 1,
    "feb": 2, "february": 2, "февраль": 2, "февраля": 2, "лютий": 2, "лютого": 2,
    "mar": 3, "march": 3, "март": 3, "марта": 3, "березень": 3, "березня": 3,
    "apr": 4, "april": 4, "апрель": 4, "апреля": 4, "квітень": 4, "квітня": 4,
    "may": 5, "май": 5, "мая": 5, "травень": 5, "травня": 5,
    "jun": 6, "june": 6, "июнь": 6, "июня": 6, "червень": 6, "червня": 6,
    "jul": 7, "july": 7, "июль": 7, "июля": 7, "липень": 7, "липня": 7,
    "aug": 8, "august": 8, "август": 8, "августа": 8, "серпень": 8, "серпня": 8,
    "sep": 9, "sept": 9, "september": 9, "сентябрь": 9, "сентября": 9, "вересень": 9, "вересня": 9,
    "oct": 10, "october": 10, "октябрь": 10, "октября": 10, "жовтень": 10, "жовтня": 10,
    "nov": 11, "november": 11, "ноябрь": 11, "ноября": 11, "листопад": 11, "листопада": 11,
    "dec": 12, "december": 12, "декабрь": 12, "декабря": 12, "грудень": 12, "грудня": 12,
}


class DiscoverError(RuntimeError):
    pass


@dataclass(frozen=True)
class DiscoverStats:
    seen_wav: int = 0
    renamed: int = 0
    inserted: int = 0
    updated: int = 0
    ids_repaired: int = 0
    legacy_three_digit_migrated: int = 0
    duplicate_ids_repaired: int = 0
    duplicate_titles_repaired: int = 0
    stacked_prefixes_repaired: int = 0
    stale_db_rows: int = 0
    unparseable_month_folders: int = 0


@dataclass
class InventoryItem:
    month_id: str
    month_name: str
    month_key: tuple[int, int, int, str]
    file_id: str
    original_name: str
    parsed_id: int | None
    id_width: int | None
    legacy_prefixes: list[str]
    raw_title: str
    title: str
    normalized_base: str
    existing_db: Any | None = None
    desired_id: str = ""
    desired_title: str = ""
    desired_name: str = ""


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

    inventory, unparseable = _build_inventory(conn, drive, channel_slug=channel_slug, audio_folder_id=str(audio_folder.id))
    _assign_desired_mapping(inventory)
    stats = _apply_plan(conn, drive, channel_slug=channel_slug, inventory=inventory, unparseable_month_folders=unparseable)
    problems = _verify_integrity(conn, inventory)
    if problems:
        raise DiscoverError("TRACK_DISCOVER_INTEGRITY_FAILED: " + "; ".join(problems[:10]))
    return stats


def _build_inventory(conn: Any, drive: Any, *, channel_slug: str, audio_folder_id: str) -> tuple[list[InventoryItem], int]:
    rows = conn.execute("SELECT * FROM tracks WHERE channel_slug = ?", (channel_slug,)).fetchall()
    db_by_gid = {str(r["gdrive_file_id"]): r for r in rows if r.get("gdrive_file_id")}
    month_folders = [i for i in drive.list_children(audio_folder_id) if str(getattr(i, "mime_type", "")) == _FOLDER_MIME]
    month_keys = {str(m.id): _parse_month_key(str(m.name)) for m in month_folders}
    unparseable = sum(1 for k in month_keys.values() if k[0])
    for m in month_folders:
        key = month_keys[str(m.id)]
        if key[0]:
            log.warning("track discover unparseable month folder: name=%s", str(m.name))
    inventory: list[InventoryItem] = []
    for month in sorted(month_folders, key=lambda m: month_keys[str(m.id)]):
        month_key = month_keys[str(month.id)]
        for item in drive.list_children(str(month.id)):
            if str(getattr(item, "mime_type", "")) == _FOLDER_MIME or not str(item.name).lower().endswith(".wav"):
                continue
            parsed_id, width, prefixes, raw_title = _parse_track_name(str(item.name))
            title = sanitize_title(raw_title) or "Track"
            inventory.append(InventoryItem(
                month_id=str(month.id), month_name=str(month.name), month_key=month_key, file_id=str(item.id),
                original_name=str(item.name), parsed_id=parsed_id, id_width=width, legacy_prefixes=prefixes,
                raw_title=raw_title, title=title, normalized_base=_normalized_duplicate_base(title),
                existing_db=db_by_gid.get(str(item.id)),
            ))
    return inventory, unparseable


def _parse_month_key(name: str) -> tuple[int, int, int, str]:
    s = name.strip().lower().replace("_", "-")
    m = re.search(r"(20\d{2}|19\d{2})[- ]?(0[1-9]|1[0-2])", s)
    if m:
        return (0, int(m.group(1)), int(m.group(2)), s)
    year = None
    ym = re.search(r"(20\d{2}|19\d{2})", s)
    if ym:
        year = int(ym.group(1))
    else:
        y2 = re.search(r"(?<!\d)(\d{2})(?!\d)", s)
        if y2:
            year = 2000 + int(y2.group(1))
    for word, month in _MONTHS.items():
        if re.search(rf"(?<![\w]){re.escape(word)}(?![\w])", s, re.UNICODE):
            return (0, year or 0, month, s)
    return (1, 9999, 99, s)


def _parse_track_name(filename: str) -> tuple[int | None, int | None, list[str], str]:
    stem, _ = os.path.splitext(filename)
    prefixes: list[str] = []
    rest = stem
    width = None
    while True:
        m = _PREFIX_RE.match(rest)
        if not m:
            break
        prefixes.append(m.group(1))
        width = len(m.group(1))
        rest = m.group(2)
    parsed = int(prefixes[-1]) if prefixes else None
    return parsed, width, prefixes, rest


def _assign_desired_mapping(inventory: list[InventoryItem]) -> None:
    dup_id_counts = Counter(i.parsed_id for i in inventory if i.parsed_id is not None)
    for idx, item in enumerate(sorted(inventory, key=_inventory_sort_key), start=1):
        item.desired_id = f"{idx:04d}"
    grouped: dict[str, list[InventoryItem]] = defaultdict(list)
    for item in inventory:
        grouped[item.normalized_base].append(item)
    used_titles: set[str] = set()
    for _base, items in sorted(grouped.items(), key=lambda kv: kv[0]):
        for ordinal, item in enumerate(sorted(items, key=_inventory_sort_key), start=1):
            candidate = _title_with_download_suffix_preserved(item.title)
            if candidate.lower() in used_titles:
                base = sanitize_title(_download_base(item.title)) or "Track"
                candidate = f"{base} Variant {ordinal}"
                n = 2
                while candidate.lower() in used_titles:
                    candidate = f"{base} Variant {ordinal}-{n}"
                    n += 1
            used_titles.add(candidate.lower())
            item.desired_title = candidate
            item.desired_name = f"{item.desired_id}_{candidate}.wav"


def _inventory_sort_key(item: InventoryItem) -> tuple[Any, ...]:
    usable = item.parsed_id is not None
    return (item.month_key, 0 if usable else 1, item.parsed_id or 0, _natural_key(item.title), item.file_id)


def _title_with_download_suffix_preserved(title: str) -> str:
    m = _DOWNLOAD_SUFFIX_RE.match(title)
    if not m:
        return title
    return sanitize_title(f"{m.group('base')} {m.group('num')}") or title


def _download_base(title: str) -> str:
    m = _DOWNLOAD_SUFFIX_RE.match(title)
    return m.group("base") if m else title


def _normalized_duplicate_base(title: str) -> str:
    return _normalized(_download_base(title)) or "track"


def _natural_key(s: str) -> tuple[Any, ...]:
    parts = re.split(r"(\d+)", s.casefold())
    return tuple(int(p) if p.isdigit() else p for p in parts)


def _normalized(s: str) -> str:
    return _NON_ALNUM_RE.sub(" ", s.casefold()).strip()


def _apply_plan(conn: Any, drive: Any, *, channel_slug: str, inventory: list[InventoryItem], unparseable_month_folders: int) -> DiscoverStats:
    seen = len(inventory)
    old_ids = [i.parsed_id for i in inventory if i.parsed_id is not None]
    duplicate_ids = {x for x, c in Counter(old_ids).items() if c > 1}
    renamed = inserted = updated = ids_repaired = legacy = dup_ids = dup_titles = stacked = 0
    final_by_parent: dict[str, set[str]] = defaultdict(set)
    for item in inventory:
        if item.desired_name in final_by_parent[item.month_id]:
            raise DiscoverError(f"TRACK_DISCOVER_INTEGRITY_FAILED: duplicate final name {item.desired_name}")
        final_by_parent[item.month_id].add(item.desired_name)
    rename_items = [i for i in inventory if i.original_name != i.desired_name]
    temp_names: dict[str, str] = {}
    nonce = int(time.time() * 1000)
    try:
        for n, item in enumerate(rename_items, start=1):
            temp = f".__discover_tmp__{nonce}_{n}_{item.file_id}.wav"
            temp_names[item.file_id] = temp
            drive.update_name(item.file_id, temp)
        for item in rename_items:
            drive.update_name(item.file_id, item.desired_name)
            renamed += 1
            if item.parsed_id is None or f"{item.parsed_id:04d}" != item.desired_id or item.id_width != 4:
                ids_repaired += 1
            if item.id_width == 3:
                legacy += 1
            if item.parsed_id in duplicate_ids:
                dup_ids += 1
            if len(item.legacy_prefixes) > 1 or _STACKED_PREFIX_RE.match(os.path.splitext(item.original_name)[0]):
                stacked += 1
    except Exception as e:
        raise DiscoverError(f"TRACK_DISCOVER_RENAME_FAILED: {e}") from e

    title_counts = Counter(i.normalized_base for i in inventory)
    dup_titles = sum(1 for i in inventory if title_counts[i.normalized_base] > 1 and i.desired_title != i.title)
    ts = time.time()
    active_ids = {i.file_id for i in inventory}
    stale = conn.execute(
        "SELECT COUNT(1) AS n FROM tracks WHERE channel_slug = ? AND gdrive_file_id NOT IN (%s)" % ",".join("?" for _ in active_ids)
        if active_ids else "SELECT COUNT(1) AS n FROM tracks WHERE channel_slug = ?",
        (channel_slug, *active_ids) if active_ids else (channel_slug,),
    ).fetchone()["n"]
    with conn:
        for item in inventory:
            row = conn.execute("SELECT id FROM tracks WHERE gdrive_file_id = ? LIMIT 1", (item.file_id,)).fetchone()
            if row is not None:
                conn.execute("UPDATE tracks SET track_id = ? WHERE id = ?", (f"__tmp__{row['id']}", row["id"]))
        for item in inventory:
            row = conn.execute("SELECT id FROM tracks WHERE gdrive_file_id = ? LIMIT 1", (item.file_id,)).fetchone()
            if row is not None:
                conn.execute(
                    """
                    UPDATE tracks
                    SET channel_slug=?, track_id=?, filename=?, title=?, source=COALESCE(source,'GDRIVE'), month_batch=?, discovered_at=?
                    WHERE id=?
                    """,
                    (channel_slug, item.desired_id, item.desired_name, item.desired_title, item.month_name, ts, row["id"]),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, month_batch, discovered_at, analyzed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (channel_slug, item.desired_id, item.file_id, "GDRIVE", item.desired_name, item.desired_title, None, None, item.month_name, ts, None),
                )
                inserted += 1
    return DiscoverStats(seen, renamed, inserted, updated, ids_repaired, legacy, dup_ids, dup_titles, stacked, int(stale or 0), unparseable_month_folders)


def _verify_integrity(conn: Any, inventory: list[InventoryItem]) -> list[str]:
    problems: list[str] = []
    ids = []
    titles = []
    for item in inventory:
        row = conn.execute("SELECT * FROM tracks WHERE gdrive_file_id = ?", (item.file_id,)).fetchall()
        if len(row) != 1:
            problems.append(f"file {item.file_id} has {len(row)} db rows")
            continue
        r = row[0]
        if not _CANON_WAV_RE.match(str(r["filename"] or "")):
            problems.append(f"bad filename {item.file_id}")
        if str(r["track_id"] or "") != item.desired_id or not re.match(r"^\d{4}$", str(r["track_id"] or "")):
            problems.append(f"bad track_id {item.file_id}")
        if str(r["filename"] or "") != item.desired_name:
            problems.append(f"filename mismatch {item.file_id}")
        if str(r["title"] or "") != item.desired_title:
            problems.append(f"title mismatch {item.file_id}")
        if _STACKED_PREFIX_RE.match(os.path.splitext(str(r["filename"] or ""))[0]):
            problems.append(f"stacked prefix {item.file_id}")
        ids.append(str(r["track_id"] or "")); titles.append(_normalized(str(r["title"] or "")))
    if len(ids) != len(set(ids)):
        problems.append("duplicate active track_id")
    if len(titles) != len(set(titles)):
        problems.append("duplicate active title")
    return problems


def _parse_canon_wav(name: str) -> tuple[str, str]:
    m = _CANON_WAV_RE.match(name)
    if not m:
        raise DiscoverError(f"cannot parse canonical wav name: {name}")
    return m.group(1), sanitize_title(m.group(2), track_id=m.group(1)) or "Track"


def _require_channel_and_canon(conn: Any, channel_slug: str) -> dict[str, Any]:
    channel = conn.execute("SELECT slug, display_name FROM channels WHERE slug = ? LIMIT 1", (channel_slug,)).fetchone()
    if channel is None:
        raise DiscoverError("channel not found")
    if conn.execute("SELECT 1 FROM canon_channels WHERE value = ? LIMIT 1", (channel_slug,)).fetchone() is None or conn.execute("SELECT 1 FROM canon_thresholds WHERE value = ? LIMIT 1", (channel_slug,)).fetchone() is None:
        raise DiscoverError("CHANNEL_NOT_IN_CANON")
    return dict(channel)


def _find_child_folder(drive: Any, parent_id: str, name: str) -> Any | None:
    for item in drive.list_children(parent_id):
        if str(getattr(item, "mime_type", "")) == _FOLDER_MIME and str(item.name) == name:
            return item
    return None
