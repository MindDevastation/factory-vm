from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def run(cmd: list[str]) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    return p.returncode, out, err


def ffprobe_json(path: Path) -> Dict[str, Any]:
    code, out, err = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(path),
        ]
    )
    if code != 0:
        raise RuntimeError(f"ffprobe failed: {err.strip()}")
    return json.loads(out)


def parse_fps(stream: Dict[str, Any]) -> Optional[float]:
    rate = stream.get("avg_frame_rate") or stream.get("r_frame_rate")
    if not rate or rate == "0/0":
        return None
    if isinstance(rate, str) and "/" in rate:
        a, b = rate.split("/", 1)
        try:
            return float(a) / float(b)
        except Exception:
            return None
    try:
        return float(rate)
    except Exception:
        return None


def volumedetect(path: Path, *, seconds: int = 60) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Return (mean_db, max_db, warn_text_if_any).

    By default we analyze only the first N seconds to keep QA fast
    even for multi-hour long-form videos.
    """
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-i",
        str(path),
    ]
    if seconds and seconds > 0:
        cmd += ["-t", str(int(seconds))]
    cmd += [
        "-af",
        "volumedetect",
        "-f",
        "null",
        "-",
    ]
    code, out, err = run(cmd)
    if code != 0:
        return None, None, "volumedetect failed"

    txt = out + "\n" + err
    m_mean = re.search(r"mean_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", txt)
    m_max = re.search(r"max_volume:\s*(-?\d+(?:\.\d+)?)\s*dB", txt)
    mean_db = float(m_mean.group(1)) if m_mean else None
    max_db = float(m_max.group(1)) if m_max else None
    return mean_db, max_db, None


def make_preview_60s(
    *,
    src_mp4: Path,
    dst_mp4: Path,
    seconds: int,
    width: int,
    height: int,
    fps: int,
    v_bitrate: str,
    a_bitrate: str,
) -> None:
    dst_mp4.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "error",
        "-i",
        str(src_mp4),
        "-t",
        str(seconds),
        "-vf",
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,fps={fps}",
        "-c:v",
        "libx264",
        "-b:v",
        v_bitrate,
        "-preset",
        "veryfast",
        "-c:a",
        "aac",
        "-b:a",
        a_bitrate,
        "-movflags",
        "+faststart",
        str(dst_mp4),
    ]
    code, out, err = run(cmd)
    if code != 0:
        raise RuntimeError(f"preview ffmpeg failed: {err.strip()}")
