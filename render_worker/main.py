import argparse
import json
import os
import re
import subprocess
import threading
import time
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# =========================
# CONFIG (можно править)
# =========================
FADE_SEC = 5.0
FADE_CURVE = "hsin"  # см. ffmpeg afade curve

AUDIO_SR = 48000
AUDIO_CH = 2

VIDEO_W = 1920
VIDEO_H = 1080
VIDEO_FPS = 24

AAC_BITRATE = "384k"

# Thermal controls (reduce CPU heat at the cost of render time)
# -threads affects overall worker threads; -filter_threads specifically limits filtergraph threading.
FFMPEG_THREADS = 6
FFMPEG_FILTER_THREADS = 2

PROGRESS_UPDATE_SEC = 2
MIN_OUTPUT_MB = 10
DURATION_TOL_SEC = 3.0

DIR_AUDIO = "Audio"
DIR_IMAGES = "Images"
DIR_RELEASE = "Release"
PLAYLISTS_FILE = "PlayLists.txt"
LOGS_FILE = "Logs.txt"

DIR_ASSETS = "_assets"  # cached generated assets (visualizer backgrounds etc)

# ===== TitanWave Sonic special features =====
TITANWAVE_PROJECT_NAME = "TitanWave Sonic"

# Visualizer band: reserved zone above subtitles
VIZ_BAND_Y = 760
VIZ_BAND_H = 100
VIZ_ALPHA = 0.80




# Right-third visualizers background (only for right-third modes)
VIZ_BG_ALPHA = 0.40
VIZ_BG_RADIUS = 22
VIZ_INNER_PAD = 12
# Linear visualizers (waveform/spectrum) only in the right third of the screen
VIZ_RIGHT_X = VIDEO_W * 2 // 3
VIZ_RIGHT_W = VIDEO_W - VIZ_RIGHT_X

# Circle visualizers (center)
VIZ_CIRCLE_SIZE = 720  # final on-screen size
VIZ_CIRCLE_STRIP_H = 180  # strip height before polar wrap (at final size)
VIZ_CIRCLE_R_IN = int(VIZ_CIRCLE_SIZE * 0.32)
VIZ_CIRCLE_R_OUT = int(VIZ_CIRCLE_SIZE * 0.48)

# Optimization: compute circle visualizers at lower internal resolution, then upscale.
# This significantly speeds up CPU-heavy geq/polar math without a visible quality hit.
VIZ_CIRCLE_INTERNAL_SIZE = 320
VIZ_CIRCLE_INTERNAL_STRIP_H = max(64, int(VIZ_CIRCLE_STRIP_H * (VIZ_CIRCLE_INTERNAL_SIZE / VIZ_CIRCLE_SIZE)))
VIZ_CIRCLE_INTERNAL_R_IN = max(8, int(VIZ_CIRCLE_R_IN * (VIZ_CIRCLE_INTERNAL_SIZE / VIZ_CIRCLE_SIZE)))
VIZ_CIRCLE_INTERNAL_R_OUT = max(VIZ_CIRCLE_INTERNAL_R_IN + 8, int(VIZ_CIRCLE_R_OUT * (VIZ_CIRCLE_INTERNAL_SIZE / VIZ_CIRCLE_SIZE)))

# TitanWave short ring (around text) – smaller and positioned bottom-left (used by "ring" short mode)
TW_RING_SIZE = 560
TW_RING_STRIP_H = 160
TW_RING_R_IN = int(TW_RING_SIZE * 0.34)
TW_RING_R_OUT = int(TW_RING_SIZE * 0.48)
TW_RING_INTERNAL_SIZE = 384
TW_RING_INTERNAL_STRIP_H = max(64, int(TW_RING_STRIP_H * (TW_RING_INTERNAL_SIZE / TW_RING_SIZE)))
TW_RING_INTERNAL_R_IN = max(8, int(TW_RING_R_IN * (TW_RING_INTERNAL_SIZE / TW_RING_SIZE)))
TW_RING_INTERNAL_R_OUT = max(TW_RING_INTERNAL_R_IN + 8, int(TW_RING_R_OUT * (TW_RING_INTERNAL_SIZE / TW_RING_SIZE)))
# Subtitle safe band (ASS placement via style MarginV)
SUBS_MARGIN_V = 55

# TitanWave text overlays
# Default SHORT placement: bottom-left
TITANWAVE_TEXT_MARGIN_L = 90
TITANWAVE_TEXT_MARGIN_B = 120

# Default SHORT panel placement (bottom-left)
TITANWAVE_PANEL_MARGIN_L = 60
TITANWAVE_PANEL_MARGIN_B = 150
TITANWAVE_PANEL_W = 860
TITANWAVE_PANEL_H_SHORT = 210

# LONG HUD: centered (inside main circle) – optional soft panel
TITANWAVE_LONG_PANEL_W = 980
TITANWAVE_LONG_PANEL_H = 280

# Glitch params (TitanWave only)
TITANWAVE_SHORT_TEXT_START = 2.0
TITANWAVE_SHORT_TEXT_END = 8.0
TITANWAVE_SHORT_GLITCH_PERIOD = 1.7
TITANWAVE_SHORT_GLITCH_LEN = 0.14
TITANWAVE_LONG_GLITCH_LEN = 0.18
TITANWAVE_LONG_GLITCH_GAP = 0.20

# TitanWave short overlay modes (switchable):
#   brief      - default: bottom-left text visible 2–8s, periodic glitch
#   persistent - bottom-left text for full track, glitch at random 5–10s intervals
#   ring       - like persistent, but visualizer ring is placed around the text (text inside ring)
TITANWAVE_SHORT_MODE_DEFAULT = "brief"
PROJECT_TITANWAVE_SHORT_MODE: Dict[str, str] = {
    "titanwave sonic": TITANWAVE_SHORT_MODE_DEFAULT,
}

# Track HUD (TitanWave multi-track)
HUD_FONT_SIZE = 38
HUD_TITLE_MAX_LEN = 46

# Legacy positions for whisper-based HUD (kept for backward compatibility; subtitles remain disabled by default)
HUD_POS_X = VIDEO_W // 2
HUD_POS_Y = 80

# Whisper autosubs (TitanWave only)
# Требует: pip install faster-whisper
WHISPER_MODEL = "small"      # "base" быстрее/проще, "small" лучше качество
WHISPER_DEVICE = "cpu"       # later можно "cuda"
WHISPER_COMPUTE_TYPE = "int8"
WHISPER_LANGUAGE = None      # "en" если всегда английский, иначе None (auto)
MIN_SUBS_SEGMENTS = 2        # если сегментов меньше — считаем, что сабов нет

# =========================
# VISUALIZER SELECTION (hardcoded now)
# Потом перенесём в Settings.txt внутри каждого проекта.
# =========================
# Возможные значения:
#   "none"
#   "waveform" (right third)
#   "waveform_tall" (right third, 2x height)
#   "waveform_circle" (center ring)
#   "thread" (right third, sine-thread style)
#   "thread_circle" (center ring, sine-thread style)
#   "spectrum_bars" (right third)
#   "spectrum_bars_circle" (center ring)
#   "vectorscope_lissajous"
#   "a3dscope", "abitscope", "ahistogram"
DEFAULT_VISUALIZER = "none"

PROJECT_VISUALIZER: Dict[str, str] = {
    "titanwave sonic": "thread_circle",
    # Для остальных можешь вручную переопределять:
    # "darkwood reverie": "none",
    "gravity lull": "none",
    # "aeternus library": "waveform",
    # "ancient dreamscape": "waveform",
}

# Subtitles only for TitanWave right now
PROJECT_SUBTITLES: Dict[str, bool] = {
    "titanwave sonic": False,
}


# =========================
# DATA
# =========================
@dataclass(frozen=True)
class EncoderProfile:
    name: str
    codec: str
    vopts: List[str]


@dataclass
class ReleaseJob:
    title: str
    track_ids: List[str]
    image_name: str
    status: str
    status_line_idx: Optional[int]
    block_start_idx: int
    block_end_idx: int  # inclusive


# =========================
# UTILS
# =========================
def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_stem(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[<>:\"/\\|?*\n\r\t]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "Untitled"


def clear_console() -> None:
    try:
        os.system("cls" if os.name == "nt" else "clear")
    except Exception:
        pass


def run_cmd(cmd: List[str], timeout: Optional[int] = None) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        return 124, out, err
    return p.returncode, out, err


def stderr_tail(stderr_text: str, max_lines: int = 8) -> str:
    lines = [ln for ln in stderr_text.strip().splitlines() if ln.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


def wait_file_stable(
    path: Path,
    *,
    stable_sec: float = 2,
    timeout_sec: float = 30,
    min_size: int = 1,
    release_dir: Optional[Path] = None,
    title: str = "INPUT",
) -> bool:
    start = time.time()
    stable_since: Optional[float] = None
    last_size: Optional[int] = None
    print(f"INPUT_STABLE_WAIT: path={path} stable_sec={stable_sec} timeout_sec={timeout_sec}", flush=True)

    while True:
        now = time.time()
        if now - start > timeout_sec:
            print(f"INPUT_STABLE_TIMEOUT: path={path} last_size={last_size}", flush=True)
            if release_dir is not None:
                log_append(release_dir, f"{now_ts()} | {title} | INPUT_STABLE_TIMEOUT | path={path} | size={last_size}")
            return False

        try:
            size = path.stat().st_size
        except FileNotFoundError:
            size = -1

        if size >= min_size and size == last_size:
            if stable_since is None:
                stable_since = now
            if now - stable_since >= stable_sec:
                print(f"INPUT_STABLE_OK: path={path} size={size}", flush=True)
                if release_dir is not None:
                    log_append(release_dir, f"{now_ts()} | {title} | INPUT_STABLE_OK | path={path} | size={size}")
                return True
        else:
            stable_since = None
            last_size = size

        time.sleep(0.2)


def validate_image_decodable(path: Path) -> Tuple[bool, str]:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-v",
        "error",
        "-i",
        str(path),
        "-frames:v",
        "1",
        "-f",
        "null",
        "-",
    ]
    code, _out, err = run_cmd(cmd, timeout=30)
    tail = stderr_tail(err)
    return code == 0, tail


def reencode_image_to_safe(path: Path, out_path: Path) -> bool:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"IMAGE_REENCODE_START: src={path} dst={out_path}", flush=True)

    png_cmd = [
        "ffmpeg", "-y", "-hide_banner", "-v", "error",
        "-i", str(path),
        "-frames:v", "1",
        "-vf", "format=rgba",
        str(out_path),
    ]
    code, _out, err = run_cmd(png_cmd, timeout=30)
    if code == 0 and out_path.exists() and out_path.stat().st_size > 0:
        print(f"IMAGE_REENCODE_DONE: src={path} dst={out_path}", flush=True)
        return True

    jpg_out = out_path.with_suffix(".jpg")
    jpg_cmd = [
        "ffmpeg", "-y", "-hide_banner", "-v", "error",
        "-i", str(path),
        "-frames:v", "1",
        "-vf", "format=yuvj420p",
        str(jpg_out),
    ]
    code2, _out2, err2 = run_cmd(jpg_cmd, timeout=30)
    if code2 == 0 and jpg_out.exists() and jpg_out.stat().st_size > 0:
        print(f"IMAGE_REENCODE_DONE: src={path} dst={jpg_out}", flush=True)
        return True

    print(
        "IMAGE_REENCODE_FAIL:",
        f"src={path}",
        f"png_err={stderr_tail(err)}",
        f"jpg_err={stderr_tail(err2)}",
        flush=True,
    )
    return False


def validate_or_reencode_image(
    path: Path,
    tmp_dir: Path,
    *,
    release_dir: Optional[Path] = None,
    title: str = "INPUT",
) -> Path:
    if not wait_file_stable(path, release_dir=release_dir, title=title):
        raise RuntimeError(f"image file not stable in time: {path}")

    ok, err_tail = validate_image_decodable(path)
    if ok:
        return path

    if release_dir is not None:
        log_append(release_dir, f"{now_ts()} | {title} | IMAGE_VALIDATE_FAIL | path={path} | stderr={err_tail}")

    safe_png = tmp_dir / f"{path.stem}__safe.png"
    if not reencode_image_to_safe(path, safe_png):
        raise RuntimeError(f"invalid/corrupted image: {path.name} | stderr={err_tail}")

    candidate = safe_png if safe_png.exists() else safe_png.with_suffix(".jpg")
    ok2, err_tail2 = validate_image_decodable(candidate)
    if not ok2:
        raise RuntimeError(
            f"invalid/corrupted image after reencode: {path.name} -> {candidate.name} | stderr={err_tail2 or err_tail}"
        )

    if release_dir is not None:
        log_append(release_dir, f"{now_ts()} | {title} | IMAGE_VALIDATE_RECOVERED | src={path} | safe={candidate}")
    return candidate


def ffprobe_json(path: Path) -> Dict[str, object]:
    cmd = [
        "ffprobe",
        "-hide_banner",
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    code, out, err = run_cmd(cmd)
    if code != 0:
        raise RuntimeError(f"ffprobe failed: {err.strip()}")
    return json.loads(out)


def get_duration_sec(path: Path) -> float:
    info = ffprobe_json(path)
    dur = info.get("format", {}).get("duration", None)  # type: ignore[union-attr]
    if dur is None:
        return 0.0
    try:
        return float(dur)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return 0.0


def mb_size(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def log_append(release_dir: Path, line: str) -> None:
    logs_path = release_dir / LOGS_FILE
    with logs_path.open("a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def cleanup_tmp_dir(tmp_dir: Path) -> None:
    if not tmp_dir.exists():
        return
    for p in tmp_dir.rglob("*"):
        if p.is_file():
            p.unlink(missing_ok=True)
    dirs = [d for d in tmp_dir.rglob("*") if d.is_dir()]
    for d in sorted(dirs, reverse=True):
        try:
            d.rmdir()
        except OSError:
            pass


def normalize_status(status: str) -> str:
    return status.strip().lower()


def is_runnable_status(status: str) -> bool:
    s = normalize_status(status)
    return s in ("", "not done")


def detect_encoders() -> str:
    code, out, _ = run_cmd(["ffmpeg", "-hide_banner", "-encoders"])
    if code != 0:
        return ""
    return out


def detect_filters() -> str:
    code, out, _ = run_cmd(["ffmpeg", "-hide_banner", "-filters"])
    if code != 0:
        return ""
    return out


def has_subtitles_filter(filters_text: str) -> bool:
    return " subtitles " in filters_text


def pick_hw_encoder(encoders_text: str) -> Optional[EncoderProfile]:
    if " h264_nvenc " in encoders_text:
        return EncoderProfile(
            name="NVENC",
            codec="h264_nvenc",
            vopts=[
                "-preset", "p5",
                "-rc", "vbr",
                "-cq", "19",
                "-b:v", "0",
                "-profile:v", "high",
            ],
        )
    if " h264_qsv " in encoders_text:
        return EncoderProfile(
            name="QSV",
            codec="h264_qsv",
            vopts=["-b:v", "2M"],
        )
    if " h264_amf " in encoders_text:
        return EncoderProfile(
            name="AMF",
            codec="h264_amf",
            vopts=["-b:v", "2M"],
        )
    return None


def cpu_encoder() -> EncoderProfile:
    return EncoderProfile(
        name="CPU(libx264)",
        codec="libx264",
        vopts=[
            "-preset", "superfast",
            "-crf", "28",
            "-tune", "stillimage",
            "-profile:v", "high",
        ],
    )


def normalize_track_id(raw: str) -> Optional[str]:
    s = raw.strip()
    if not s:
        return None
    if re.fullmatch(r"\d+", s):
        return str(int(s))  # "001" -> "1"
    return s


def truncate_text(s: str, max_len: int) -> str:
    s = s.strip()
    if len(s) <= max_len:
        return s
    return s[: max(0, max_len - 1)].rstrip() + "…"


def parse_track_meta_from_filename(path: Path) -> Tuple[str, str]:
    """
    "001_Title_Name" -> ("001", "Title Name")
    """
    stem = path.stem
    m = re.match(r"^(\d{3,})_(.+)$", stem)
    if not m:
        return "", stem
    tid = m.group(1)
    title = m.group(2).replace("_", " ").strip()
    return tid, title


def project_key(project_dir: Path) -> str:
    return project_dir.name.strip().lower()


def get_visualizer_type(project_dir: Path) -> str:
    key = project_key(project_dir)
    return PROJECT_VISUALIZER.get(key, DEFAULT_VISUALIZER)


def subtitles_enabled(project_dir: Path) -> bool:
    key = project_key(project_dir)
    return PROJECT_SUBTITLES.get(key, False)


def ffmpeg_filter_escape_path(p: Path) -> str:
    s = str(p).replace("\\", "/")
    m = re.match(r"^([A-Za-z]):/(.*)$", s)
    if m:
        drive = m.group(1)
        rest = m.group(2)
        s = f"{drive}\\:/{rest}"
    s = s.replace("'", "\\'")
    return s


# =========================
# ASSETS (generated once, cached)
# =========================
def ensure_rounded_bg_png(assets_dir: Path, *, w: int, h: int, alpha: float, radius: int) -> Path:
    """Generate (once) a rounded-rect semi-transparent background PNG.

    Why: applying a rounded-corners alpha mask with geq on EVERY frame is expensive.
    We pre-render the rounded rectangle into a PNG and simply overlay it during render.
    """
    assets_dir.mkdir(parents=True, exist_ok=True)
    a = int(round(alpha * 100))
    out = assets_dir / f"bg_round_v2_{w}x{h}_a{a}_r{radius}.png"
    if out.exists():
        validated, _ = validate_image_decodable(out)
        if validated:
            return out
        out.unlink(missing_ok=True)

    r = int(max(0, radius))

    # Mask expression: 1 inside rounded-rect, 0 outside.
    x1 = f"(W-{r}-1)"
    y1 = f"(H-{r}-1)"
    r2 = f"pow({r},2)"

    tl = f"lt(X,{r})*lt(Y,{r})"
    tr = f"gt(X,{x1})*lt(Y,{r})"
    bl = f"lt(X,{r})*gt(Y,{y1})"
    br = f"gt(X,{x1})*gt(Y,{y1})"

    tl_in = f"lte(pow(X-{r},2)+pow(Y-{r},2),{r2})"
    tr_in = f"lte(pow(X-({x1}),2)+pow(Y-{r},2),{r2})"
    bl_in = f"lte(pow(X-{r},2)+pow(Y-({y1}),2),{r2})"
    br_in = f"lte(pow(X-({x1}),2)+pow(Y-({y1}),2),{r2})"

    inside = (
        f"if({tl},{tl_in},"
        f"if({tr},{tr_in},"
        f"if({bl},{bl_in},"
        f"if({br},{br_in},1))))"
    )

    a_val = int(round(255 * alpha))

    vf = (
        "format=rgba,"
        f"geq=r=0:g=0:b=0:a='if({inside},{a_val},0)'"
    )

    
    tmp_out = assets_dir / f".{out.name}.tmp"
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-v", "error",
        "-f", "lavfi",
        "-i", f"color=c=black:s={w}x{h}:d=0.1",
        "-vf", vf,
        "-frames:v", "1",
        str(tmp_out),
    ]
    code, out_s, err_s = run_cmd(cmd)
    if code != 0 or not tmp_out.exists():
        tmp_out.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to generate bg png: {err_s.strip() or out_s.strip()}")
    ok, err_tail = validate_image_decodable(tmp_out)
    if not ok:
        tmp_out.unlink(missing_ok=True)
        raise RuntimeError(f"Failed to generate bg png (invalid): {err_tail}")
    os.replace(tmp_out, out)
    return out


# =========================
# PROJECT DISCOVERY
# =========================
def discover_projects(root_dir: Path) -> List[Path]:
    projects: List[Path] = []
    if not root_dir.exists():
        return projects
    for p in root_dir.iterdir():
        if not p.is_dir():
            continue
        pl = p / PLAYLISTS_FILE
        if pl.exists():
            projects.append(p)
    return projects


# =========================
# PLAYLISTS PARSE/WRITE
# =========================
def parse_playlists_file(playlists_path: Path) -> Tuple[List[str], List[ReleaseJob]]:
    lines = playlists_path.read_text(encoding="utf-8").splitlines()
    jobs: List[ReleaseJob] = []

    i = 0
    n = len(lines)

    def is_blank(s: str) -> bool:
        return not s.strip()

    while i < n:
        while i < n and is_blank(lines[i]):
            i += 1
        if i >= n:
            break

        block_start = i

        m = re.match(r"^\s*([^:]+)\s*:\s*(.*)\s*$", lines[i])
        if not m:
            i += 1
            continue

        title = m.group(1).strip()
        track_part = m.group(2).strip()
        track_ids = [t.strip() for t in track_part.split() if t.strip()]

        image_name = ""
        status = ""
        status_line_idx: Optional[int] = None

        i += 1
        while i < n and not is_blank(lines[i]):
            line = lines[i].strip()

            m_img = re.match(r"^Image\s*:\s*(.+)\s*$", line, flags=re.IGNORECASE)
            if m_img:
                image_name = m_img.group(1).strip()

            m_stat = re.match(r"^Status\s*:\s*(.+)\s*$", line, flags=re.IGNORECASE)
            if m_stat:
                status = m_stat.group(1).strip()
                status_line_idx = i

            i += 1

        block_end = i - 1

        jobs.append(
            ReleaseJob(
                title=title,
                track_ids=track_ids,
                image_name=image_name,
                status=status,
                status_line_idx=status_line_idx,
                block_start_idx=block_start,
                block_end_idx=block_end,
            )
        )

    return lines, jobs


def update_job_status_in_lines(lines: List[str], job: ReleaseJob, new_status: str) -> None:
    if job.status_line_idx is not None:
        prefix = re.match(
            r"^(\s*Status\s*:\s*).*$",
            lines[job.status_line_idx],
            flags=re.IGNORECASE,
        )
        if prefix:
            lines[job.status_line_idx] = prefix.group(1) + new_status
        else:
            lines[job.status_line_idx] = f"Status: {new_status}"
        return

    insert_at = job.block_end_idx + 1
    lines.insert(insert_at, f"Status: {new_status}")


def write_playlists_file(playlists_path: Path, lines: List[str]) -> None:
    data = "\n".join(lines) + "\n"
    tmp = playlists_path.with_suffix(playlists_path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(playlists_path)


# =========================
# AUDIO INDEX
# =========================
def build_audio_index(audio_dir: Path) -> Dict[str, Path]:
    """
    Индексируем WAV внутри Audio/**.
    Ключи:
      - "001" (как в имени)
      - "1"   (без ведущих нулей)
    """
    idx: Dict[str, Path] = {}
    if not audio_dir.exists():
        return idx

    for p in audio_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() != ".wav":
            continue

        m = re.match(r"^(\d{3,})_", p.stem)
        if not m:
            continue

        raw_id = m.group(1)
        norm_id = str(int(raw_id))

        idx.setdefault(raw_id, p)
        idx.setdefault(norm_id, p)

    return idx


# =========================
# AUDIO MERGE WITH FADES
# =========================
def build_merged_wav_with_fades(
    tmp_dir: Path,
    title: str,
    track_paths: List[Path],
    force_rebuild: bool,
    release_dir: Path,
) -> Tuple[Path, float]:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    merged = tmp_dir / f"{safe_stem(title)}__merged.flac"

    if merged.exists() and not force_rebuild:
        dur = get_duration_sec(merged)
        return merged, dur

    if merged.exists():
        merged.unlink(missing_ok=True)

    durations: List[float] = []
    warnings: List[str] = []

    for tp in track_paths:
        d = get_duration_sec(tp)
        durations.append(d)
        if d <= 0:
            warnings.append(f"ffprobe duration=0 for {tp.name}")

    filter_parts: List[str] = []
    a_labels: List[str] = []

    for i, dur in enumerate(durations):
        fade_d = min(FADE_SEC, max(0.05, dur / 2.0 if dur > 0 else FADE_SEC))
        if dur > 0 and fade_d < FADE_SEC:
            warnings.append(f"{track_paths[i].name}: fade shortened to {fade_d:.2f}s (track is {dur:.2f}s)")

        st_out = max(0.0, (dur - fade_d) if dur > 0 else 0.0)

        label = f"a{i}"
        a_labels.append(f"[{label}]")

        part = (
            f"[{i}:a]"
            f"aformat=sample_fmts=s16:sample_rates={AUDIO_SR}:channel_layouts=stereo,"
            f"afade=t=in:st=0:d={fade_d:.3f}:curve={FADE_CURVE},"
            f"afade=t=out:st={st_out:.3f}:d={fade_d:.3f}:curve={FADE_CURVE}"
            f"[{label}]"
        )
        filter_parts.append(part)

    concat_part = "".join(a_labels) + f"concat=n={len(track_paths)}:v=0:a=1[aout]"
    filter_complex = ";".join(filter_parts + [concat_part])

    ff_loglevel = str(os.environ.get("FFMPEG_LOGLEVEL", "error") or "error").strip()
    use_stats = str(os.environ.get("FFMPEG_STATS", "")).strip().lower() in ("1","true","yes","y","on")
    echo_stderr = str(os.environ.get("FFMPEG_ECHO_STDERR", "")).strip().lower() in ("1","true","yes","y","on")

    cmd: List[str] = ["ffmpeg", "-y", "-hide_banner", "-v", "error"]
    for tp in track_paths:
        cmd += ["-i", str(tp)]
    cmd += [
        "-filter_complex",
        filter_complex,
        "-map",
        "[aout]",
        "-c:a",
        "flac",
        str(merged),
    ]

    log_append(release_dir, f"{now_ts()} | {title} | MERGE_AUDIO_START | tracks={len(track_paths)} | curve={FADE_CURVE}")
    code, out, err = run_cmd(cmd)
    if code != 0 or not merged.exists():
        reason = (err.strip() or out.strip() or "unknown ffmpeg error").replace("\n", " | ")
        log_append(release_dir, f"{now_ts()} | {title} | MERGE_AUDIO_FAIL | {reason}")
        raise RuntimeError(f"Audio merge failed: {reason}")

    for w in warnings:
        log_append(release_dir, f"{now_ts()} | {title} | MERGE_AUDIO_WARN | {w}")

    dur = get_duration_sec(merged)
    log_append(release_dir, f"{now_ts()} | {title} | MERGE_AUDIO_DONE | duration={dur:.3f}s")
    return merged, dur


# =========================
# ASS GENERATION (TitanWave only)
# =========================
def ass_time(sec: float) -> str:
    if sec < 0:
        sec = 0
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    cs = int(round((s - int(s)) * 100))
    return f"{h}:{m:02d}:{int(s):02d}.{cs:02d}"


def transcribe_whisper_segments(audio_path: Path) -> List[Tuple[float, float, str]]:
    """
    Returns list of (start_sec, end_sec, text).
    Requires faster-whisper installed.
    """
    try:
        from faster_whisper import WhisperModel  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Missing dependency: faster-whisper. Install with: pip install faster-whisper"
        ) from e

    model = WhisperModel(
        WHISPER_MODEL,
        device=WHISPER_DEVICE,
        compute_type=WHISPER_COMPUTE_TYPE,
    )

    segments, _info = model.transcribe(
        str(audio_path),
        language=WHISPER_LANGUAGE,
        vad_filter=True,
        beam_size=5,
    )

    out: List[Tuple[float, float, str]] = []
    for seg in segments:
        text = (seg.text or "").strip()
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        out.append((float(seg.start), float(seg.end), text))
    return out



def prepare_whisper_audio(tmp_dir: Path, src_audio: Path) -> Path:
    """Convert audio to a Whisper-friendly format (mono 16kHz WAV).

    This makes transcription faster and avoids edge cases with some codecs.
    The file is placed into _tmp and will be cleaned after the job.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    out = tmp_dir / f"{safe_stem(src_audio.stem)}__whisper.wav"
    if out.exists():
        return out
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-v", "error",
        "-i", str(src_audio),
        "-ac", "1",
        "-ar", "16000",
        "-c:a", "pcm_s16le",
        str(out),
    ]
    code, out_s, err_s = run_cmd(cmd)
    if code != 0 or not out.exists():
        raise RuntimeError(f"Failed to prepare whisper audio: {err_s.strip() or out_s.strip()}")
    return out

def build_titanwave_overlay_ass(
    out_ass: Path,
    title: str,
    audio_for_subs: Path,
    tmp_dir: Path,
    track_paths: List[Path],
    track_durations: List[float],
    *,
    include_hud: bool,
) -> None:
    """
    One ASS file that contains:
      - Lyrics subtitles (auto whisper)
      - Track HUD (prev/current/next) if include_hud=True
    """
    # 1) subtitles (lyrics)
    wh_audio = prepare_whisper_audio(tmp_dir, audio_for_subs)
    segments = transcribe_whisper_segments(wh_audio)
    if len(segments) < MIN_SUBS_SEGMENTS:
        raise RuntimeError(f"autosubs produced too few segments: {len(segments)} (min {MIN_SUBS_SEGMENTS})")

    # 2) HUD timings
    track_starts: List[float] = []
    t = 0.0
    for d in track_durations:
        track_starts.append(t)
        t += max(0.0, d)

    track_meta: List[Tuple[str, str]] = []
    for tp in track_paths:
        tid, tname = parse_track_meta_from_filename(tp)
        tid_disp = tid if tid else ""
        tname_disp = truncate_text(tname, HUD_TITLE_MAX_LEN)
        track_meta.append((tid_disp, tname_disp))

    # Styles:
    # - Lyrics: bottom center, readable on any bg (box + outline)
    # - HUD: top center, 3 lines, with per-line alpha tags
    header = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"Title: {title}",
        "PlayResX: 1920",
        "PlayResY: 1080",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, "
        "Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding",
        # Lyrics
        "Style: Lyrics,Arial,54,&H00FFFFFF,&H00FFFFFF,&H00000000,&H99000000,0,0,0,0,100,100,0,0,3,3,0,2,80,80,"
        f"{SUBS_MARGIN_V},1",
        # HUD
        f"Style: HUD,Arial,{HUD_FONT_SIZE},&H00FFFFFF,&H00FFFFFF,&H00000000,&H99000000,0,0,0,0,100,100,0,0,3,2,0,8,80,80,30,1",
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]

    events: List[str] = []

    # Lyrics events
    for st, en, txt in segments:
        if en <= st:
            continue
        # Minimal sanitation for ASS
        txt = txt.replace("{", "").replace("}", "")
        txt = txt.replace("\n", " ").strip()
        txt = txt.replace("\\", "\\\\")
        # Keep 1–2 lines: optional wrap can be done later, keeping simple now.
        events.append(f"Dialogue: 0,{ass_time(st)},{ass_time(en)},Lyrics,,0,0,0,,{txt}")

    # HUD events (only multi-track)
    if include_hud and len(track_meta) > 1:
        for i, st in enumerate(track_starts):
            en = st + max(0.0, track_durations[i])
            if en <= st:
                continue

            prev_line = " "
            cur_line = " "
            next_line = " "

            if i - 1 >= 0:
                pid, pname = track_meta[i - 1]
                prev_line = f"{pid}. {pname}".strip(". ").strip()

            cid, cname = track_meta[i]
            cur_line = f"{cid}. {cname}".strip(". ").strip()

            if i + 1 < len(track_meta):
                nid, nname = track_meta[i + 1]
                next_line = f"{nid}. {nname}".strip(". ").strip()

            # Inline alpha tags:
            # 50% opacity ~ alpha 0x80, opaque alpha 0x00
            hud_text = (
                f"{{\\an8\\pos({HUD_POS_X},{HUD_POS_Y})}}"
                f"{{\\alpha&H80&}}{prev_line}\\N"
                f"{{\\alpha&H00&}}{cur_line}\\N"
                f"{{\\alpha&H80&}}{next_line}"
            )
            hud_text = hud_text.replace("{", "\\{").replace("}", "\\}")  # avoid accidental ass tags from titles
            # We actually WANT our tags, so restore them:
            hud_text = hud_text.replace("\\{\\an8", "{\\an8").replace("\\{\\alpha", "{\\alpha").replace("\\}", "}")
            events.append(f"Dialogue: 1,{ass_time(st)},{ass_time(en)},HUD,,0,0,0,,{hud_text}")

    out_ass.parent.mkdir(parents=True, exist_ok=True)
    out_ass.write_text("\n".join(header + events) + "\n", encoding="utf-8")
# =========================
# TITANWAVE TEXT OVERLAY (NO SUBTITLES)
# =========================
def _glitch_text(s: str, *, seed: int) -> str:
    """Deterministic 'glitch' variant for a string (ASS-safe)."""
    rnd = random.Random(seed)
    s2 = []
    pool = list("@#$%&*+=?/~")
    for ch in s:
        if ch.isspace():
            s2.append(ch)
            continue
        if rnd.random() < 0.18:
            s2.append(rnd.choice(pool))
        elif rnd.random() < 0.06:
            # drop character
            continue
        else:
            s2.append(ch)
    # occasional splice
    if len(s2) > 6 and rnd.random() < 0.35:
        k = rnd.randint(2, min(6, len(s2) - 1))
        s2.insert(k, rnd.choice(pool))
    return "".join(s2)


def build_titanwave_text_overlay_ass(
    out_ass: Path,
    *,
    video_title: str,
    track_paths: List[Path],
    track_durations: List[float],
) -> None:
    """
    TitanWave Sonic overlay ASS (NO subtitles):
      - Short (1 track): 2 lines left, visible 2–8s, periodic glitch pulses
      - Long  (>1 track): 3-line HUD left for each track segment,
                          glitch pulses only at track changes
    """
    is_short = len(track_paths) <= 1
    tw_mode = PROJECT_TITANWAVE_SHORT_MODE.get("titanwave sonic", TITANWAVE_SHORT_MODE_DEFAULT).strip().lower()
    if tw_mode not in ("brief", "persistent", "ring"):
        tw_mode = TITANWAVE_SHORT_MODE_DEFAULT

    # Track starts
    track_starts: List[float] = []
    t = 0.0
    for d in track_durations:
        track_starts.append(t)
        t += max(0.0, d)

    # Track names (by position, not by ID)
    track_names: List[str] = []
    for tp in track_paths:
        _tid, tname = parse_track_meta_from_filename(tp)
        track_names.append(truncate_text(tname, HUD_TITLE_MAX_LEN))

    def esc_ass_text(s: str) -> str:
        # keep override tags intact where we put them ourselves
        s = s.replace("{", "").replace("}", "")
        s = s.replace("\n", " ").strip()
        s = s.replace("\\", "\\\\")
        return s

    header = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"Title: {video_title}",
        "PlayResX: 1920",
        "PlayResY: 1080",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, "
        "Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
        "Alignment, MarginL, MarginR, MarginV, Encoding",
        # Base style: left/top (an7), outline for readability, transparent back (panel is separate PNG)
        "Style: TW,Arial,46,&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,3,0,7,0,0,0,1",
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]

    events: List[str] = []

    # Common anchors
    bl_x = TITANWAVE_TEXT_MARGIN_L
    bl_y = VIDEO_H - TITANWAVE_TEXT_MARGIN_B

    # Center of the main circle (long)
    cx = VIDEO_W // 2
    cy = VIDEO_H // 2

    if is_short:
        # Short: channel + track title
        channel = TITANWAVE_PROJECT_NAME
        track_title = track_names[0] if track_names else video_title

        def wrap_words(text: str, max_chars: int, max_lines: int) -> str:
            words = re.sub(r"\s+", " ", text).strip().split(" ")
            if not words:
                return ""
            lines: List[str] = []
            cur: List[str] = []
            for w in words:
                nxt = (" ".join(cur + [w])).strip()
                if len(nxt) <= max_chars or not cur:
                    cur.append(w)
                else:
                    lines.append(" ".join(cur))
                    cur = [w]
                    if len(lines) >= max_lines - 1:
                        break
            if cur and len(lines) < max_lines:
                lines.append(" ".join(cur))
            if len(lines) > max_lines:
                lines = lines[:max_lines]
            return "\\N".join(lines)

        # Mode-specific placement & timing
        if tw_mode == "ring":
            # Text is centered inside the ring placed bottom-left.
            # We keep it compact and wrap aggressively so it stays inside the circle.
            ring_cx = TITANWAVE_PANEL_MARGIN_L + TW_RING_SIZE // 2
            ring_cy = VIDEO_H - TITANWAVE_PANEL_MARGIN_B - TW_RING_SIZE // 2
            base_an = "\\an5"
            base_pos = f"\\pos({ring_cx},{ring_cy})"
            fs_channel = 28
            fs_title = 42
            title_wrapped = wrap_words(track_title, max_chars=22, max_lines=3)
            base_text_body = (
                f"{{{base_an}{base_pos}}}"
                f"{{\\fs{fs_channel}}}{esc_ass_text(channel)}\\N"
                f"{{\\fs{fs_title}}}{esc_ass_text(title_wrapped)}"
            )
            t0 = 0.0
            t1 = max(0.1, track_durations[0] if track_durations else 0.1)
        else:
            # Bottom-left, left aligned
            base_an = "\\an1"
            base_pos = f"\\pos({bl_x},{bl_y})"
            fs_channel = 30
            fs_title = 58
            base_text_body = (
                f"{{{base_an}{base_pos}}}"
                f"{{\\fs{fs_channel}}}{esc_ass_text(channel)}\\N"
                f"{{\\fs{fs_title}}}{esc_ass_text(track_title)}"
            )
            if tw_mode == "brief":
                t0 = TITANWAVE_SHORT_TEXT_START
                t1 = TITANWAVE_SHORT_TEXT_END
            else:
                t0 = 0.0
                t1 = max(0.1, track_durations[0] if track_durations else 0.1)

        events.append(f"Dialogue: 1,{ass_time(t0)},{ass_time(t1)},TW,,0,0,0,,{base_text_body}")

        # Glitch pulses
        glen = TITANWAVE_SHORT_GLITCH_LEN
        seed_base = abs(hash((video_title, track_title))) % (2**31)

        if tw_mode == "brief":
            # Periodic pulses inside [2,8]
            period = TITANWAVE_SHORT_GLITCH_PERIOD
            k = 0
            tt = t0
            while tt < t1:
                st = tt + 0.20
                en = min(t1, st + glen)
                if en > st:
                    jitter_x = bl_x + ((k % 3) - 1) * 6
                    jitter_y = bl_y + (((k + 1) % 3) - 1) * 4
                    gtext = (
                        f"{{\\an1\\pos({jitter_x},{jitter_y})\\bord4\\blur1}}"
                        f"{{\\fs{fs_channel}}}{esc_ass_text(_glitch_text(channel, seed=seed_base + 1000 + k))}\\N"
                        f"{{\\fs{fs_title}}}{esc_ass_text(_glitch_text(track_title, seed=seed_base + 2000 + k))}"
                    )
                    events.append(f"Dialogue: 3,{ass_time(st)},{ass_time(en)},TW,,0,0,0,,{gtext}")
                k += 1
                tt += period
        else:
            # Random pulses every 5–10 seconds over the full track
            rnd = random.Random(seed_base)
            t = 3.0
            k = 0
            while t < (t1 - 0.5):
                t += rnd.uniform(5.0, 10.0)
                st = t
                en = min(t1, st + glen)
                if en <= st:
                    break
                if tw_mode == "ring":
                    ring_cx = TITANWAVE_PANEL_MARGIN_L + TW_RING_SIZE // 2
                    ring_cy = VIDEO_H - TITANWAVE_PANEL_MARGIN_B - TW_RING_SIZE // 2
                    jitter_x = ring_cx + ((k % 3) - 1) * 7
                    jitter_y = ring_cy + (((k + 1) % 3) - 1) * 5
                    title_wrapped_g = wrap_words(_glitch_text(track_title, seed=seed_base + 2000 + k), 22, 3)
                    gtext = (
                        f"{{\\an5\\pos({jitter_x},{jitter_y})\\bord4\\blur1}}"
                        f"{{\\fs{fs_channel}}}{esc_ass_text(_glitch_text(channel, seed=seed_base + 1000 + k))}\\N"
                        f"{{\\fs{fs_title}}}{esc_ass_text(title_wrapped_g)}"
                    )
                else:
                    jitter_x = bl_x + ((k % 3) - 1) * 7
                    jitter_y = bl_y + (((k + 1) % 3) - 1) * 5
                    gtext = (
                        f"{{\\an1\\pos({jitter_x},{jitter_y})\\bord4\\blur1}}"
                        f"{{\\fs{fs_channel}}}{esc_ass_text(_glitch_text(channel, seed=seed_base + 1000 + k))}\\N"
                        f"{{\\fs{fs_title}}}{esc_ass_text(_glitch_text(track_title, seed=seed_base + 2000 + k))}"
                    )
                events.append(f"Dialogue: 3,{ass_time(st)},{ass_time(en)},TW,,0,0,0,,{gtext}")
                k += 1

    else:
        # Long: 3-line HUD for each track segment — centered inside the main circle
        for i, st in enumerate(track_starts):
            en = st + max(0.0, track_durations[i])
            if en <= st:
                continue

            prev_line = " "
            cur_line = " "
            next_line = " "

            if i - 1 >= 0:
                prev_line = f"{i:02d}. {track_names[i-1]}"

            cur_line = f"{i+1:02d}. {track_names[i]}"

            if i + 1 < len(track_names):
                next_line = f"{i+2:02d}. {track_names[i+1]}"

            hud_text = (
                f"{{\\an5\\pos({cx},{cy})}}"
                f"{{\\fs34\\alpha&H80&}}{esc_ass_text(prev_line)}\\N"
                f"{{\\fs46\\alpha&H00&}}{esc_ass_text(cur_line)}\\N"
                f"{{\\fs34\\alpha&H80&}}{esc_ass_text(next_line)}"
            )
            events.append(f"Dialogue: 1,{ass_time(st)},{ass_time(en)},TW,,0,0,0,,{hud_text}")

        # Glitch pulses only at track changes (start of each track except 0)
        for i in range(1, len(track_starts)):
            st0 = track_starts[i]
            # build the HUD lines for the new current track i
            prev_line = f"{i:02d}. {track_names[i-1]}" if i - 1 >= 0 else " "
            cur_line = f"{i+1:02d}. {track_names[i]}"
            next_line = f"{i+2:02d}. {track_names[i+1]}" if (i + 1) < len(track_names) else " "

            for pulse_idx, dt in enumerate((0.02, TITANWAVE_LONG_GLITCH_GAP + 0.02)):
                st = st0 + dt
                en = st + TITANWAVE_LONG_GLITCH_LEN
                jitter_x = cx + ((pulse_idx % 3) - 1) * 8
                jitter_y = cy + (((pulse_idx + 1) % 3) - 1) * 6
                gtext = (
                    f"{{\\an5\\pos({jitter_x},{jitter_y})\\bord4\\blur1}}"
                    f"{{\\fs34\\alpha&H40&}}{esc_ass_text(_glitch_text(prev_line, seed=3000+i*10+pulse_idx))}\\N"
                    f"{{\\fs46\\alpha&H00&}}{esc_ass_text(_glitch_text(cur_line, seed=4000+i*10+pulse_idx))}\\N"
                    f"{{\\fs34\\alpha&H40&}}{esc_ass_text(_glitch_text(next_line, seed=5000+i*10+pulse_idx))}"
                )
                events.append(f"Dialogue: 3,{ass_time(st)},{ass_time(en)},TW,,0,0,0,,{gtext}")

    out_ass.parent.mkdir(parents=True, exist_ok=True)
    out_ass.write_text("\n".join(header + events) + "\n", encoding="utf-8")

def normalize_viz_type(viz_type: str) -> str:
    """Normalize/alias visualizer names.

    Public supported names (per protocol):
      none,
      waveform, waveform_right, waveform_right_tall, waveform_circle,
      thread, thread_right, thread_circle,
      spectrum_bars, spectrum_bars_right, spectrum_bars_circle,
      vectorscope_lissajous, avectorscope, a3dscope, abitscope, ahistogram.

    Additional internal/special names:
      thread_circle_text  (TitanWave short "ring" mode)
    """
    vt = (viz_type or "").strip().lower()

    aliases = {
        # explicit right-third naming
        "waveform_right": "waveform",
        "waveform_right_tall": "waveform_tall",
        "thread_right": "thread",
        "spectrum_bars_right": "spectrum_bars",
        # legacy / backward compat
        "waveform_tall_right": "waveform_tall",
        "waveform_right2": "waveform_tall",
    }
    vt = aliases.get(vt, vt)
    return vt

# =========================
# VISUALIZER FILTERS
# =========================
def build_visualizer_filter(viz_type: str, *, assets_dir: Path) -> Optional[str]:
    """Return filtergraph fragment producing [viz] from audio [1:a].

    IMPORTANT optimization:
      - Right-third modes use a pre-rendered rounded-rect PNG background (overlay),
        instead of geq masking every frame.
      - Circle modes render at lower internal resolution then upscale.
    """
    viz_type = normalize_viz_type(viz_type)
    if viz_type in ("none", ""):
        return None

    # Default band (full width, reserved zone above subtitles)
    w = VIDEO_W
    h = VIZ_BAND_H

    # =========================
    # Right-third linear variants
    # =========================
    right_third = viz_type in ("waveform", "spectrum_bars", "waveform_tall", "thread")
    if right_third:
        w = VIZ_RIGHT_W
        h = (VIZ_BAND_H * 2) if (viz_type == "waveform_tall") else VIZ_BAND_H

        pad = max(0, int(VIZ_INNER_PAD))
        inner_w = max(16, w - pad * 2)
        inner_h = max(16, h - pad * 2)

        # --- inner viz (transparent bg) ---
        if viz_type == "spectrum_bars":
            inner = (
                f"[1:a]showfreqs=s={inner_w}x{inner_h}:mode=bar:fscale=log:ascale=log:win_size=4096:averaging=2,"
                f"format=rgba,"
                f"colorkey=0x000000:0.20:0.0,"
                f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[inner]"
            )
        elif viz_type == "thread":
            inner = (
                f"[1:a]showwaves=s={inner_w}x{inner_h}:mode=cline:rate={VIDEO_FPS},"
                f"format=rgba,"
                f"colorkey=0x000000:0.25:0.0,"
                f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[inner]"
            )
        else:
            inner = (
                f"[1:a]showwaves=s={inner_w}x{inner_h}:mode=line:rate={VIDEO_FPS},"
                f"format=rgba,"
                f"colorkey=0x000000:0.25:0.0,"
                f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[inner]"
            )

        # Pad inner into fixed tile
        pad_tile = f"[inner]pad={w}:{h}:{pad}:{pad}:color=black@0[vizraw]"

        # Pre-rendered rounded bg (cached)
        bg_png = ensure_rounded_bg_png(
            assets_dir,
            w=w,
            h=h,
            alpha=VIZ_BG_ALPHA,
            radius=int(VIZ_BG_RADIUS),
        )
        bgp = ffmpeg_filter_escape_path(bg_png)
        bg = f"movie='{bgp}',loop=loop=-1:size=1:start=0,format=rgba[bgimg]"

        mix = f"[bgimg][vizraw]overlay=0:0:format=auto[viz]"
        return ";".join([inner, pad_tile, bg, mix])

    # =========================
    # Circle variants (center) — computed at lower res, then upscaled
    # =========================
    if viz_type in ("waveform_circle", "spectrum_bars_circle", "thread_circle", "thread_circle_text"):
        # "thread_circle_text" is a TitanWave-only short mode: a smaller ring intended to surround text.
        is_text_ring = (viz_type == "thread_circle_text")

        if is_text_ring:
            out_size = TW_RING_SIZE
            size = TW_RING_INTERNAL_SIZE
            strip_h = TW_RING_INTERNAL_STRIP_H
            pad_top = max(0, (size - strip_h) // 2)
            r_in = TW_RING_INTERNAL_R_IN
            r_out = TW_RING_INTERNAL_R_OUT
        else:
            out_size = VIZ_CIRCLE_SIZE
            size = VIZ_CIRCLE_INTERNAL_SIZE
            strip_h = VIZ_CIRCLE_INTERNAL_STRIP_H
            pad_top = max(0, (size - strip_h) // 2)
            r_in = VIZ_CIRCLE_INTERNAL_R_IN
            r_out = VIZ_CIRCLE_INTERNAL_R_OUT

        if viz_type == "spectrum_bars_circle":
            strip = (
                f"[1:a]showfreqs=s={size}x{strip_h}:mode=bar:fscale=log:ascale=log:win_size=4096:averaging=2,"
                f"format=rgba,"
                f"colorkey=0x000000:0.25:0.0,"
                f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[strip]"
            )
        elif viz_type in ("thread_circle", "thread_circle_text"):
            strip = (
                f"[1:a]showwaves=s={size}x{strip_h}:mode=cline:rate={VIDEO_FPS},"
                f"format=rgba,"
                f"colorkey=0x000000:0.25:0.0,"
                f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[strip]"
            )
        else:
            strip = (
                f"[1:a]showwaves=s={size}x{strip_h}:mode=line:rate={VIDEO_FPS},"
                f"format=rgba,"
                f"colorkey=0x000000:0.25:0.0,"
                f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[strip]"
            )

        pad = f"[strip]pad={size}:{size}:0:{pad_top}:color=black@0[p]"

        src_x = "((atan2(Y-H/2,X-W/2)+PI)/(2*PI))*(W-1)"
        src_y = f"({pad_top})+(1-((hypot(X-W/2,Y-H/2)-{r_in})/({r_out}-{r_in})))*({strip_h}-1)"

        geq = (
            f"[p]geq="
            f"r='if(between(hypot(X-W/2,Y-H/2),{r_in},{r_out}),r({src_x},{src_y}),0)':"
            f"g='if(between(hypot(X-W/2,Y-H/2),{r_in},{r_out}),g({src_x},{src_y}),0)':"
            f"b='if(between(hypot(X-W/2,Y-H/2),{r_in},{r_out}),b({src_x},{src_y}),0)':"
            f"a='if(between(hypot(X-W/2,Y-H/2),{r_in},{r_out}),alpha({src_x},{src_y}),0)'"
            f"[vizsmall]"
        )

        upscale = f"[vizsmall]scale={out_size}:{out_size}:flags=bicubic[viz]"
        return ";".join([strip, pad, geq, upscale])

    # =========================
    # Band / full width variants
    # =========================
    if viz_type == "avectorscope":
        # Plain avectorscope (default mode=lissajous)
        return (
            f"[1:a]avectorscope=s={h}x{h}:mode=lissajous:draw=line:scale=sqrt,"
            f"format=rgba,"
            f"colorkey=0x000000:0.25:0.0,"
            f"colorchannelmixer=aa={VIZ_ALPHA:.2f},"
            f"scale={w}:{h}:flags=bicubic[viz]"
        )

    if viz_type == "vectorscope_lissajous":
        return (
            f"[1:a]avectorscope=s={h}x{h}:mode=lissajous:draw=line:scale=sqrt,"
            f"format=rgba,"
            f"colorkey=0x000000:0.25:0.0,"
            f"colorchannelmixer=aa={VIZ_ALPHA:.2f},"
            f"scale={w}:{h}:flags=bicubic[viz]"
        )

    if viz_type == "a3dscope":
        return (
            f"[1:a]a3dscope=s={w}x{h}:r={VIDEO_FPS},"
            f"format=rgba,"
            f"colorkey=0x000000:0.25:0.0,"
            f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[viz]"
        )

    if viz_type == "abitscope":
        return (
            f"[1:a]abitscope=s={w}x{h}:r={VIDEO_FPS},"
            f"format=rgba,"
            f"colorkey=0x000000:0.25:0.0,"
            f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[viz]"
        )

    if viz_type == "ahistogram":
        return (
            f"[1:a]ahistogram=s={w}x{h}:r={VIDEO_FPS},"
            f"format=rgba,"
            f"colorkey=0x000000:0.25:0.0,"
            f"colorchannelmixer=aa={VIZ_ALPHA:.2f}[viz]"
        )

    return None


def get_visualizer_overlay_xy(viz_type: str) -> Tuple[int, int]:
    vt = (viz_type or "none").strip().lower()

    if vt == "thread_circle_text":
        # TitanWave short "ring" mode placement: bottom-left around the text.
        x = TITANWAVE_PANEL_MARGIN_L
        y = VIDEO_H - TITANWAVE_PANEL_MARGIN_B - TW_RING_SIZE
        return x, y

    # Circle variants: centered
    if vt in ("waveform_circle", "spectrum_bars_circle", "thread_circle"):
        x = (VIDEO_W - VIZ_CIRCLE_SIZE) // 2
        y = (VIDEO_H - VIZ_CIRCLE_SIZE) // 2
        return x, y

    # Right-third linear variants
    if vt in ("waveform", "spectrum_bars", "thread"):
        return VIZ_RIGHT_X, VIZ_BAND_Y

    if vt == "waveform_tall":
        return VIZ_RIGHT_X, (VIZ_BAND_Y - VIZ_BAND_H)

    # Default: full width band
    return 0, VIZ_BAND_Y

# =========================
# RENDER WITH PROGRESS
# =========================
class OutputGrowthWatchdog:
    """Detects a stuck ffmpeg run when output files stop growing.

    This is intentionally simple and testable (no subprocess dependency).
    """

    def __init__(self, *, start_ts: float, grace_sec: float, idle_sec: float, min_delta_bytes: int) -> None:
        self._start_ts = float(start_ts)
        self._grace_sec = max(0.0, float(grace_sec))
        self._idle_sec = max(1.0, float(idle_sec))
        self._min_delta = max(1, int(min_delta_bytes))

        self._last_bytes: Optional[int] = None
        self._last_growth_ts: float = self._start_ts

    def update(self, *, total_bytes: int, now_ts: float) -> None:
        now = float(now_ts)
        b = max(0, int(total_bytes))

        if self._last_bytes is None:
            self._last_bytes = b
            if b > 0:
                self._last_growth_ts = now
            return

        if b >= (self._last_bytes + self._min_delta):
            self._last_bytes = b
            self._last_growth_ts = now

    def is_stuck(self, *, now_ts: float) -> bool:
        now = float(now_ts)
        if (now - self._start_ts) < self._grace_sec:
            return False
        return (now - self._last_growth_ts) >= self._idle_sec

    @property
    def last_growth_ts(self) -> float:
        return self._last_growth_ts


def _render_output_total_bytes(out_mp4: Path) -> int:
    """Sum sizes of possible output files (final + common temp suffixes)."""
    candidates = {
        out_mp4,
        out_mp4.with_name(out_mp4.name + ".tmp"),
        out_mp4.with_name(out_mp4.name + ".part"),
        out_mp4.with_suffix(out_mp4.suffix + ".tmp"),
        out_mp4.with_suffix(out_mp4.suffix + ".part"),
    }
    total = 0
    for p in candidates:
        try:
            if p.exists():
                total += p.stat().st_size
        except Exception:
            continue
    return total


def render_video_with_progress(
    title: str,
    image_path: Path,
    audio_wav: Path,
    out_mp4: Path,
    enc: EncoderProfile,
    expected_duration_sec: float,
    session_start: float,
    *,
    viz_type: str,
    overlay_ass: Optional[Path],
    assets_dir: Path,
    text_panel: Optional[Tuple[Path, int, int, Optional[str]]],
) -> Tuple[bool, str, float]:
    out_mp4.parent.mkdir(parents=True, exist_ok=True)
    out_mp4.unlink(missing_ok=True)

    bg_vf = (
        f"scale={VIDEO_W}:{VIDEO_H}:force_original_aspect_ratio=decrease,"
        f"pad={VIDEO_W}:{VIDEO_H}:(ow-iw)/2:(oh-ih)/2,"
        f"format=rgba"
    )

    cmd: List[str] = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "error",
        "-progress",
        "pipe:1",
        "-threads",
        str(FFMPEG_THREADS),
        "-filter_threads",
        str(FFMPEG_FILTER_THREADS),
        "-loop",
        "1",
        "-i",
        str(image_path),
        "-i",
        str(audio_wav),
    ]
    # FFMPEG logging/debug knobs
    ff_loglevel = str(os.environ.get("FFMPEG_LOGLEVEL", "error") or "error").strip()
    use_stats = str(os.environ.get("FFMPEG_STATS", "")).strip().lower() in ("1", "true", "yes", "y", "on")

    # By default, echo ffmpeg stderr into the job log. Can be disabled via FFMPEG_ECHO_STDERR=0.
    _echo_raw = os.environ.get("FFMPEG_ECHO_STDERR")
    if _echo_raw is None or str(_echo_raw).strip() == "":
        echo_stderr = True
    else:
        echo_stderr = str(_echo_raw).strip().lower() in ("1", "true", "yes", "y", "on")

    # Watchdog: detect stuck runs when output file stops growing.
    wd_idle_sec = float(os.environ.get("RENDER_WATCHDOG_IDLE_SEC", "120") or "120")
    wd_grace_sec = float(os.environ.get("RENDER_WATCHDOG_GRACE_SEC", "30") or "30")
    wd_min_delta = int(os.environ.get("RENDER_WATCHDOG_MIN_DELTA_BYTES", "1024") or "1024")
    wd_kill_after_sec = float(os.environ.get("RENDER_WATCHDOG_KILL_AFTER_SEC", "15") or "15")


    # Apply stats mode and loglevel knobs (useful for debugging)
    if use_stats:
        try:
            i = cmd.index("-nostats")
            cmd[i] = "-stats"
        except ValueError:
            pass

    try:
        i = cmd.index("-loglevel")
        cmd[i + 1] = ff_loglevel
    except ValueError:
        pass

    print("FFMPEG_CMD:", " ".join(cmd), flush=True)

    start = time.time()
    watchdog = OutputGrowthWatchdog(
        start_ts=start,
        grace_sec=wd_grace_sec,
        idle_sec=wd_idle_sec,
        min_delta_bytes=wd_min_delta,
    )
    watchdog_triggered = False
    watchdog_triggered_at: Optional[float] = None
    watchdog_reason: str = ""

    last_print = 0.0
    out_time_ms = 0

    stderr_lines: List[str] = []
    stderr_lock = threading.Lock()
    state_lock = threading.Lock()

    def drain_stderr(proc: subprocess.Popen) -> None:
        try:
            assert proc.stderr is not None
            for line in proc.stderr:
                if echo_stderr:
                    try:
                        print("FFMPEG_ERR:", line.rstrip(), flush=True)
                    except Exception:
                        pass
                with stderr_lock:
                    if len(stderr_lines) < 200:
                        stderr_lines.append(line.rstrip())
        except Exception:
            return


    def print_progress(percent: float, elapsed: float) -> None:
        # Emit a single-line progress marker ending with '%' (parsed by orchestrator).
        pct = max(0.0, min(100.0, float(percent)))
        print(f"{pct:.1f} %", flush=True)

        # Also append to per-release Logs.txt for debugging (best-effort).
        try:
            log_append(out_mp4.parent, f"{now_ts()} | {title} | RENDER_PROGRESS | {pct:.1f}% | out_time_ms={out_time_ms} | elapsed={elapsed:.1f}s")
        except Exception:
            pass

        # If running interactively, show a nicer UI.
        try:
            if sys.stdout.isatty():
                clear_console()
                bar_len = 20
                filled = int(bar_len * pct / 100.0)
                bar = '|' + ('█' * filled) + ('—' * (bar_len - filled)) + f'| {pct:5.1f}%'
                print(f"Title: {title}")
                print(f"Progress: {bar}")
                print(f"Time: {elapsed:,.1f}s | {time.time() - session_start:,.1f}s")
                print('-' * 40)
        except Exception:
            pass


    viz_filter = build_visualizer_filter(viz_type, assets_dir=assets_dir)

    # If we have viz or overlay_ass -> use filter_complex
    use_complex = (viz_filter is not None) or (overlay_ass is not None)

    if not use_complex:
        # simplest path (minimum heat)
        cmd += [
            "-vf",
            bg_vf.replace("format=rgba", "format=yuv420p"),
            "-r",
            str(VIDEO_FPS),
            "-c:v",
            enc.codec,
            *enc.vopts,
            "-c:a",
            "aac",
            "-b:a",
            AAC_BITRATE,
            "-ar",
            str(AUDIO_SR),
            "-ac",
            str(AUDIO_CH),
            "-shortest",
            "-movflags",
            "+faststart",
            str(out_mp4),
        ]
    else:
        parts: List[str] = []
        parts.append(f"[0:v]{bg_vf}[bg]")

        cur_v = "[bg]"

        if viz_filter is not None:
            parts.append(viz_filter)
            # overlay viz band in reserved zone
            x_viz, y_viz = get_visualizer_overlay_xy(viz_type)
            parts.append(f"{cur_v}[viz]overlay=x={x_viz}:y={y_viz}:format=auto[v1]")
            cur_v = "[v1]"

        if text_panel is not None:
            panel_png, px, py, enable_expr = text_panel
            pp = ffmpeg_filter_escape_path(panel_png)
            parts.append(f"movie='{pp}',loop=loop=-1:size=1:start=0,format=rgba[pnl]")
            if enable_expr:
                parts.append(f"{cur_v}[pnl]overlay=x={px}:y={py}:enable='{enable_expr}':format=auto[vp]")
            else:
                parts.append(f"{cur_v}[pnl]overlay=x={px}:y={py}:format=auto[vp]")
            cur_v = "[vp]"

        if overlay_ass is not None:
            sp = ffmpeg_filter_escape_path(overlay_ass)
            parts.append(f"{cur_v}subtitles='{sp}'[v2]")
            cur_v = "[v2]"

        parts.append(f"{cur_v}format=yuv420p[vout]")
        cur_v = "[vout]"


        filter_complex = ";".join(parts)

        cmd += [
            "-filter_complex",
            filter_complex,
            "-map",
            cur_v,
            "-map",
            "1:a",
            "-r",
            str(VIDEO_FPS),
            "-c:v",
            enc.codec,
            *enc.vopts,
            "-c:a",
            "aac",
            "-b:a",
            AAC_BITRATE,
            "-ar",
            str(AUDIO_SR),
            "-ac",
            str(AUDIO_CH),
            "-shortest",
            "-movflags",
            "+faststart",
            str(out_mp4),
        ]

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    t_stderr = threading.Thread(target=drain_stderr, args=(p,), daemon=True)
    t_stderr.start()

    tick_stop = threading.Event()

    def ticker() -> None:
        # Print progress periodically even if ffmpeg does not emit progress lines yet.
        while not tick_stop.is_set() and p.poll() is None:
            now = time.time()
            elapsed = now - start
            with state_lock:
                cur_out_ms = out_time_ms
            if expected_duration_sec > 0:
                percent = min(100.0, (cur_out_ms/1_000_000.0)/expected_duration_sec * 100.0)
            else:
                percent = 0.0
            total_bytes = _render_output_total_bytes(out_mp4)
            watchdog.update(total_bytes=total_bytes, now_ts=now)
            if watchdog.is_stuck(now_ts=now):
                if not watchdog_triggered:
                    watchdog_triggered = True
                    watchdog_triggered_at = now
                    watchdog_reason = f"output not growing for {int(wd_idle_sec)}s (bytes={total_bytes})"
                    print("WATCHDOG_STUCK:", watchdog_reason, flush=True)
                    try:
                        log_append(out_mp4.parent, f"{now_ts()} | {title} | WATCHDOG_STUCK | {watchdog_reason}")
                    except Exception:
                        pass
                    try:
                        p.terminate()
                    except Exception:
                        pass
                else:
                    if watchdog_triggered_at is not None and (now - watchdog_triggered_at) >= wd_kill_after_sec:
                        try:
                            p.kill()
                        except Exception:
                            pass

            print_progress(percent, elapsed)
            tick_stop.wait(PROGRESS_UPDATE_SEC)

    t_tick = threading.Thread(target=ticker, daemon=True)
    t_tick.start()

    try:
        assert p.stdout is not None
        for line in p.stdout:
            line = line.strip()
            if line.startswith("out_time_ms="):
                try:
                    with state_lock:
                        out_time_ms = int(line.split("=", 1)[1])
                except ValueError:
                    pass

        p.wait()
    finally:
        try:
            if p.stdout:
                p.stdout.close()
        except Exception:
            pass

        tick_stop.set()
        try:
            t_tick.join(timeout=1.0)
        except Exception:
            pass

    t_stderr.join(timeout=1.0)
    render_time = time.time() - start

    if watchdog_triggered:
        with stderr_lock:
            tail = " | ".join(stderr_lines[-10:]) if stderr_lines else ""
        reason = f"watchdog_stuck: {watchdog_reason}"
        if tail:
            reason += f" | stderr_tail: {tail}"
        return False, reason, render_time

    if p.returncode != 0:
        with stderr_lock:
            tail = " | ".join(stderr_lines[-10:]) if stderr_lines else "unknown ffmpeg error"
        return False, tail, render_time

    if not out_mp4.exists():
        return False, "ffmpeg finished but output file missing", render_time

    return True, "ok", render_time


# =========================
# QA
# =========================
def qa_hard(out_mp4: Path, expected_duration_sec: float) -> Tuple[bool, str]:
    if not out_mp4.exists():
        return False, "output file missing"

    size_mb = mb_size(out_mp4)
    if size_mb < MIN_OUTPUT_MB:
        return False, f"too small file size: {size_mb:.1f}MB < {MIN_OUTPUT_MB}MB"

    try:
        info = ffprobe_json(out_mp4)
    except Exception as e:
        return False, f"ffprobe error: {e}"

    streams = info.get("streams", [])
    if not isinstance(streams, list):
        return False, "ffprobe malformed streams"

    has_v = any(isinstance(s, dict) and s.get("codec_type") == "video" for s in streams)
    has_a = any(isinstance(s, dict) and s.get("codec_type") == "audio" for s in streams)
    if not has_v:
        return False, "no video stream"
    if not has_a:
        return False, "no audio stream"

    v = next((s for s in streams if isinstance(s, dict) and s.get("codec_type") == "video"), None)
    if not isinstance(v, dict):
        return False, "video stream missing"

    w = v.get("width", 0)
    h = v.get("height", 0)
    try:
        if int(w) != VIDEO_W or int(h) != VIDEO_H:
            return False, f"wrong resolution: {w}x{h} (expected {VIDEO_W}x{VIDEO_H})"
    except Exception:
        return False, f"wrong resolution types: {w}x{h}"

    fmt = info.get("format", {})
    if not isinstance(fmt, dict):
        return False, "ffprobe malformed format"

    dur_val = fmt.get("duration", 0.0) or 0.0
    try:
        dur = float(dur_val)
    except (ValueError, TypeError):
        dur = 0.0

    if expected_duration_sec > 0 and abs(dur - expected_duration_sec) > DURATION_TOL_SEC:
        return False, (
            f"duration mismatch: {dur:.2f}s vs expected {expected_duration_sec:.2f}s "
            f"(tol {DURATION_TOL_SEC}s)"
        )

    return True, "ok"


def qa_soft_volumedetect(out_mp4: Path) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-v",
        "error",
        "-i",
        str(out_mp4),
        "-t",
        "60",
        "-af",
        "volumedetect",
        "-f",
        "null",
        "-",
    ]
    code, out, err = run_cmd(cmd)
    if code != 0:
        return None, None, "volumedetect failed"

    text = err + "\n" + out
    m_mean = re.search(r"mean_volume:\s*(-?\d+(\.\d+)?)\s*dB", text)
    m_max = re.search(r"max_volume:\s*(-?\d+(\.\d+)?)\s*dB", text)

    mean_db = float(m_mean.group(1)) if m_mean else None
    max_db = float(m_max.group(1)) if m_max else None

    warn = None
    if max_db is not None and max_db >= -0.1:
        warn = f"possible clipping: max_volume={max_db:.2f} dB"
    return mean_db, max_db, warn


# =========================
# MAIN PIPELINE
# =========================
def process_project(
    project_dir: Path,
    hw_encoder_profile: EncoderProfile,
    session_start: float,
    filters_text: str,
) -> None:
    playlists_path = project_dir / PLAYLISTS_FILE
    audio_dir = project_dir / DIR_AUDIO
    images_dir = project_dir / DIR_IMAGES
    release_dir = project_dir / DIR_RELEASE
    release_dir.mkdir(parents=True, exist_ok=True)

    audio_index = build_audio_index(audio_dir)
    tmp_dir = release_dir / "_tmp"
    assets_dir = release_dir / DIR_ASSETS

    viz_type = get_visualizer_type(project_dir)
    subs_on = subtitles_enabled(project_dir)

    subtitles_supported = has_subtitles_filter(filters_text)

    while True:
        lines, jobs = parse_playlists_file(playlists_path)
        job = next((j for j in jobs if is_runnable_status(j.status)), None)
        if job is None:
            return

        title = job.title
        out_mp4 = release_dir / f"{safe_stem(title)}.mp4"

        try:
            # If already rendered -> QA -> Done
            if out_mp4.exists():
                ok, reason = qa_hard(out_mp4, expected_duration_sec=0.0)
                if ok:
                    update_job_status_in_lines(lines, job, "Done")
                    write_playlists_file(playlists_path, lines)
                    log_append(release_dir, f"{now_ts()} | {title} | DONE_ALREADY_PRESENT | {out_mp4.name}")
                    continue
                log_append(release_dir, f"{now_ts()} | {title} | EXISTS_BUT_QA_FAIL | {reason}")

            # Validate image
            if not job.image_name:
                update_job_status_in_lines(lines, job, "Fail (missing Image: ...)")
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | missing Image field in PlayLists.txt")
                continue

            image_path = images_dir / job.image_name
            if not image_path.exists():
                update_job_status_in_lines(lines, job, f"Fail (image not found: {job.image_name})")
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | image not found: {image_path}")
                continue

            try:
                image_path = validate_or_reencode_image(
                    image_path,
                    tmp_dir,
                    release_dir=release_dir,
                    title=title,
                )
            except Exception as e:
                fail_msg = f"Fail (invalid/corrupted image: {job.image_name})"
                update_job_status_in_lines(lines, job, fail_msg)
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | invalid/corrupted image: {job.image_name} | {e}")
                continue

            # Validate tracks
            track_paths: List[Path] = []
            missing: List[str] = []
            for tid in job.track_ids:
                norm = normalize_track_id(tid)
                if norm is None:
                    continue
                if norm not in audio_index:
                    missing.append(tid.strip())
                    continue
                track_paths.append(audio_index[norm])

            if missing:
                update_job_status_in_lines(lines, job, f"Fail (missing tracks: {' '.join(missing)})")
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | missing tracks: {' '.join(missing)}")
                continue

            if not track_paths:
                update_job_status_in_lines(lines, job, "Fail (empty track list)")
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | empty track list")
                continue

            # TitanWave subtitles require ffmpeg subtitles filter
            if subs_on and not subtitles_supported:
                reason = "ffmpeg missing subtitles filter (libass). Install full ffmpeg build with libass."
                update_job_status_in_lines(lines, job, f"Fail ({reason})")
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | {reason}")
                continue

            # Track durations for HUD (only needed for TitanWave + multi)
            track_durations: List[float] = []
            for tp in track_paths:
                track_durations.append(get_duration_sec(tp))

            merged_wav: Optional[Path] = None
            expected_dur = 0.0
            last_fail_reason = "unknown"

            # Overlay ASS path (only TitanWave, regen on attempt 3)
            overlay_ass_path = tmp_dir / f"{safe_stem(title)}__overlay.ass"

            for attempt in (1, 2, 3):
                force_rebuild_audio = (attempt == 3)
                use_cpu = (attempt >= 2)

                try:
                    merged_wav, expected_dur = build_merged_wav_with_fades(
                        tmp_dir=tmp_dir,
                        title=title,
                        track_paths=track_paths,
                        force_rebuild=force_rebuild_audio,
                        release_dir=release_dir,
                    )
                except Exception as e:
                    last_fail_reason = f"merge_audio: {e}"
                    log_append(release_dir, f"{now_ts()} | {title} | ATTEMPT_{attempt} | FAIL | {last_fail_reason}")
                    continue

                # TitanWave Sonic: text overlays (NO subtitles)
                overlay_ass: Optional[Path] = None
                text_panel: Optional[Tuple[Path, int, int, Optional[str]]] = None

                is_titanwave = (project_key(project_dir) == TITANWAVE_PROJECT_NAME.lower())
                if is_titanwave:
                    tw_short_mode = PROJECT_TITANWAVE_SHORT_MODE.get(
                        TITANWAVE_PROJECT_NAME.lower(), TITANWAVE_SHORT_MODE_DEFAULT
                    ).strip().lower()
                    if tw_short_mode not in ("brief", "persistent", "ring"):
                        tw_short_mode = TITANWAVE_SHORT_MODE_DEFAULT

                    # If short "ring" mode is enabled, force a special visualizer layout.
                    if (len(track_paths) <= 1) and (tw_short_mode == "ring"):
                        viz_type = "thread_circle_text"

                    try:
                        if (not overlay_ass_path.exists()) or force_rebuild_audio:
                            log_append(release_dir, f"{now_ts()} | {title} | TW_TEXT_BUILD_START")
                            build_titanwave_text_overlay_ass(
                                out_ass=overlay_ass_path,
                                video_title=title,
                                track_paths=track_paths,
                                track_durations=track_durations,
                            )
                            log_append(release_dir, f"{now_ts()} | {title} | TW_TEXT_BUILD_DONE | file={overlay_ass_path.name}")
                        overlay_ass = overlay_ass_path
                        # Text background panel is disabled for TitanWave (per latest requirement)
                        text_panel = None
                    except Exception as e:
                        last_fail_reason = f"titanwave_overlay: {e}"
                        log_append(release_dir, f"{now_ts()} | {title} | ATTEMPT_{attempt} | TW_TEXT_FAIL | {last_fail_reason}")
                        # TitanWave overlays are required for this pipeline
                        continue

                enc = cpu_encoder() if use_cpu else hw_encoder_profile
                log_append(
                    release_dir,
                    f"{now_ts()} | {title} | RENDER_START | attempt={attempt} | encoder={enc.name} | viz={viz_type} | overlay={'yes' if overlay_ass else 'no'}",
                )

                ok, reason, rtime = render_video_with_progress(
                    title=title,
                    image_path=image_path,
                    audio_wav=merged_wav,
                    out_mp4=out_mp4,
                    enc=enc,
                    expected_duration_sec=expected_dur,
                    session_start=session_start,
                    viz_type=viz_type,
                    overlay_ass=overlay_ass,
                    assets_dir=assets_dir,
                    text_panel=text_panel,
                )

                if not ok:
                    last_fail_reason = f"ffmpeg: {reason}"
                    log_append(release_dir, f"{now_ts()} | {title} | ATTEMPT_{attempt} | RENDER_FAIL | {last_fail_reason}")
                    out_mp4.unlink(missing_ok=True)
                    continue

                ok2, reason2 = qa_hard(out_mp4, expected_duration_sec=expected_dur)
                if not ok2:
                    last_fail_reason = f"QA_hard: {reason2}"
                    log_append(release_dir, f"{now_ts()} | {title} | ATTEMPT_{attempt} | QA_FAIL | {last_fail_reason}")
                    out_mp4.unlink(missing_ok=True)
                    continue

                mean_db, max_db, warn = qa_soft_volumedetect(out_mp4)
                if mean_db is not None or max_db is not None:
                    log_append(release_dir, f"{now_ts()} | {title} | QA_SOFT | mean={mean_db} dB | max={max_db} dB")
                if warn:
                    log_append(release_dir, f"{now_ts()} | {title} | QA_SOFT_WARN | {warn}")

                update_job_status_in_lines(lines, job, "Done")
                write_playlists_file(playlists_path, lines)
                log_append(
                    release_dir,
                    (
                        f"{now_ts()} | {title} | DONE | file={out_mp4.name} | duration={expected_dur:.2f}s | "
                        f"render_time={rtime:.1f}s | encoder={enc.name}"
                    ),
                )
                break
            else:
                update_job_status_in_lines(lines, job, f"Fail ({last_fail_reason})")
                write_playlists_file(playlists_path, lines)
                log_append(release_dir, f"{now_ts()} | {title} | FAIL | {last_fail_reason}")

        finally:
            # Clear _tmp after each job
            cleanup_tmp_dir(tmp_dir)


def reset_logs_for_projects(projects: List[Path]) -> None:
    for project_dir in projects:
        release_dir = project_dir / DIR_RELEASE
        release_dir.mkdir(parents=True, exist_ok=True)
        logs_path = release_dir / LOGS_FILE
        logs_path.write_text(f"=== Render session start: {now_ts()} ===\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=str, default="", help="Path to YouTube root folder")
    args = parser.parse_args()

    if args.root:
        root_dir = Path(args.root).expanduser()
    else:
        home = Path.home()

        candidates: List[Path] = []
        candidates.append(home / "Desktop" / "YouTube")

        onedrive = (
            os.environ.get("OneDrive")
            or os.environ.get("OneDriveConsumer")
            or os.environ.get("OneDriveCommercial")
        )
        if onedrive:
            candidates.append(Path(onedrive) / "Desktop" / "YouTube")

        candidates.append(home / "OneDrive" / "Desktop" / "YouTube")
        candidates.append(Path.cwd() / "YouTube")

        root_dir = next((p for p in candidates if p.exists()), candidates[-1])

    projects = discover_projects(root_dir)
    if not projects:
        print(f"No projects found in: {root_dir}")
        print(f"Expected: {root_dir}/<ProjectName>/{PLAYLISTS_FILE}")
        return

    reset_logs_for_projects(projects)

    enc_text = detect_encoders()
    force_cpu = str(os.environ.get("FORCE_CPU_ENCODER", "")).strip().lower() in ("1","true","yes","y","on")
    hw = None if force_cpu else pick_hw_encoder(enc_text)
    hw_profile = cpu_encoder() if force_cpu else (hw if hw else cpu_encoder())

    filters_text = detect_filters()

    print(f"Root: {root_dir}")
    print(f"Projects: {[p.name for p in projects]}")
    print(f"HW encoder: {hw_profile.name} ({hw_profile.codec})")
    print(f"Subtitles filter available: {'yes' if has_subtitles_filter(filters_text) else 'no'}")
    print("-" * 40)

    session_start = time.time()

    for project_dir in projects:
        print(f"\n=== Project: {project_dir.name} ===")
        process_project(
            project_dir,
            hw_encoder_profile=hw_profile,
            session_start=session_start,
            filters_text=filters_text,
        )

    print("\nAll done.")


if __name__ == "__main__":
    main()
