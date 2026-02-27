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
    - if ``track_id`` is provided, it is removed from the title.
    """

    cleaned = _FORBIDDEN_TITLE_CHARS_RE.sub(" ", title)
    if track_id:
        cleaned = cleaned.replace(str(track_id), " ")

    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned[:max_len].rstrip()


def canonicalize_track_filename(filename: str) -> str:
    """Repair supported non-canonical track filename patterns.

    Behavior:
    - ``081_001_Title.ext`` -> keep second id (``001_Title.ext``)
    - ``001 Title.ext`` / ``001-Title.ext`` / ``001.Title.ext`` -> ``001_Title.ext``
    - canonical ``XXX_Title.ext`` is preserved (no extra prefixing)
    """

    stem, ext = os.path.splitext(filename)

    match = re.match(r"^(\d{3})_(\d{3})_(.+)$", stem)
    if match:
        track_id = match.group(2)
        title = match.group(3)
    else:
        match = re.match(r"^(\d{3})_(.+)$", stem)
        if match:
            track_id = match.group(1)
            title = match.group(2)
        else:
            match = re.match(r"^(\d{3})[ .-]+(.+)$", stem)
            if not match:
                return filename
            track_id = match.group(1)
            title = match.group(2)

    safe_title = sanitize_title(title, track_id=track_id)
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
