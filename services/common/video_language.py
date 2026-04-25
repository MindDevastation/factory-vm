from __future__ import annotations

import re

DEFAULT_VIDEO_LANGUAGE = "en"

_LANGUAGE_LABEL_MAP: dict[str, str] = {
    "english": "en",
    "ukrainian": "uk",
    "russian": "ru",
    "spanish": "es",
}

_ALPHA2_PATTERN = re.compile(r"^[a-z]{2}$")


def normalize_video_language(raw_value: str | None) -> str | None:
    """Normalize operator input to a safe 2-letter language code.

    Returns ``None`` when the value is unsafe or unsupported.

    Notes:
    - "UK" is intentionally not mapped to "uk" because it is commonly used
      for country shorthand (United Kingdom) and is ambiguous in this context.
    - BCP-47 variants (e.g. ``en-US``) are not accepted to keep upload payloads
      strict and predictable for YouTube metadata fields.
    """

    raw = str(raw_value or "").strip()
    if not raw:
        return None

    lowered = raw.lower()
    if lowered == "uk" and raw != "uk":
        return None

    mapped = _LANGUAGE_LABEL_MAP.get(lowered)
    if mapped:
        return mapped

    if _ALPHA2_PATTERN.fullmatch(lowered):
        return lowered

    return None
