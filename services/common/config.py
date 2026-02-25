from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class ChannelCfg:
    slug: str
    display_name: str
    kind: str
    weight: float
    render_profile: str
    autopublish_enabled: bool
    yt_token_json_path: Optional[str]
    yt_client_secret_json_path: Optional[str]


@dataclass(frozen=True)
class RenderProfileCfg:
    name: str
    video_w: int
    video_h: int
    fps: float
    vcodec_required: str
    audio_sr: int
    audio_ch: int
    acodec_required: str


@dataclass(frozen=True)
class PoliciesCfg:
    raw: Dict[str, Any]


def _read_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def load_channels(cfg_path: str) -> List[ChannelCfg]:
    data = _read_yaml(Path(cfg_path))
    out: List[ChannelCfg] = []
    for c in data.get("channels", []):
        out.append(
            ChannelCfg(
                slug=str(c["slug"]),
                display_name=str(c["display_name"]),
                kind=str(c["kind"]),
                weight=float(c.get("weight", 1.0)),
                render_profile=str(c["render_profile"]),
                autopublish_enabled=bool(c.get("autopublish_enabled", False)),
                yt_token_json_path=(str(c["yt_token_json_path"]) if c.get("yt_token_json_path") else None),
                yt_client_secret_json_path=(str(c["yt_client_secret_json_path"]) if c.get("yt_client_secret_json_path") else None),
            )
        )
    return out


def load_render_profiles(cfg_path: str) -> List[RenderProfileCfg]:
    data = _read_yaml(Path(cfg_path))
    out: List[RenderProfileCfg] = []
    profiles = data.get("render_profiles", {})
    for name, p in profiles.items():
        out.append(
            RenderProfileCfg(
                name=str(name),
                video_w=int(p["video"]["width"]),
                video_h=int(p["video"]["height"]),
                fps=float(p["video"]["fps"]),
                vcodec_required=str(p["video"]["codec_required"]),
                audio_sr=int(p["audio"]["sample_rate"]),
                audio_ch=int(p["audio"]["channels"]),
                acodec_required=str(p["audio"]["codec_required"]),
            )
        )
    return out


def load_policies(cfg_path: str) -> PoliciesCfg:
    return PoliciesCfg(raw=_read_yaml(Path(cfg_path)))


def dump_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)
