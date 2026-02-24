from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass(frozen=True)
class LocalRelease:
    folder: Path
    meta_path: Path
    meta: Dict


def list_release_folders(origin_root: Path, channel_slug: str) -> List[Path]:
    incoming = origin_root / "channels" / channel_slug / "incoming"
    if not incoming.exists():
        return []
    return sorted([p for p in incoming.iterdir() if p.is_dir()])


def load_meta(folder: Path) -> Optional[LocalRelease]:
    meta_path = folder / "meta.json"
    if not meta_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return LocalRelease(folder=folder, meta_path=meta_path, meta=meta)


def resolve_asset_path(release_folder: Path, rel_path: str) -> Path:
    # rel_path from meta.json like "audio/track.wav"
    rel_path = rel_path.lstrip("/").replace("\\", "/")
    return (release_folder / rel_path).resolve()
