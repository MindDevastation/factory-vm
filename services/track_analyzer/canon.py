"""Canonicalization helpers for track analyzer naming rules.

All helpers in this module are pure functions and do not perform DB or API calls.
"""

from __future__ import annotations

import hashlib
import os
import re
from typing import Optional

_FORBIDDEN_TITLE_CHARS_RE = re.compile(r'[<>:"/\\|?*]')
_WHITESPACE_RE = re.compile(r"\s+")


def sanitize_title(title: str, track_id: Optional[str] = None, max_len: int = 90) -> str:
    """Normalize title to naming-safe text.

    Rules:
    - forbidden chars ``<>:\"/\\|?*`` are replaced with spaces,
    - repeated whitespace is collapsed to one space,
    - output is trimmed,
    - output length is capped (default 90),
    - if ``track_id`` is provided, only leading ID prefixes are removed from the title.
    """

    cleaned = _FORBIDDEN_TITLE_CHARS_RE.sub(" ", title)
    if track_id:
        cleaned = re.sub(rf"^(?:{re.escape(str(track_id))})(?:[_ .-]+)", "", cleaned)

    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned[:max_len].rstrip()


def canonicalize_track_filename(filename: str) -> str:
    """Repair supported non-canonical track filename patterns to four-digit IDs."""

    stem, ext = os.path.splitext(filename)
    prefixes: list[str] = []
    rest = stem
    while True:
        match = re.match(r"^(\d{3,4})[_ .-]+(.+)$", rest)
        if not match:
            break
        prefixes.append(match.group(1))
        rest = match.group(2)
    if not prefixes:
        return filename

    track_id = f"{int(prefixes[-1]):04d}"
    safe_title = sanitize_title(rest, track_id=track_id) or "Track"
    return f"{track_id}_{safe_title}{ext}"


def deterministic_hash_suffix(*parts: str, length: int = 6) -> str:
    """Return a deterministic collision suffix for naming.

    The hash input is the UTF-8 encoding of ``"|".join(parts)``.
    ``length`` must be between 4 and 6 characters (inclusive).
    """

    if length < 4 or length > 6:
        raise ValueError("length must be between 4 and 6")

    digest = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
    return digest[:length]
