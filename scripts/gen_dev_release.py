from __future__ import annotations

import argparse
import base64
import json
import math
import time
import wave
from pathlib import Path
from typing import Any, Dict

TINY_PNG_B64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/6X+Jb8AAAAASUVORK5CYII="


def _write_sine_wav(path: Path, seconds: int = 30, sr: int = 48000, freq: float = 220.0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = seconds * sr
    amp = 0.25
    with wave.open(str(path), "wb") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)  # 16-bit
        wf.setframerate(sr)
        for i in range(n):
            v = int(amp * 32767.0 * math.sin(2.0 * math.pi * freq * (i / sr)))
            frame = v.to_bytes(2, "little", signed=True) * 2
            wf.writeframesraw(frame)


def _write_cover_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = base64.b64decode(TINY_PNG_B64)
    path.write_bytes(data)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--channel", required=True, help="channel slug, e.g. darkwood-reverie")
    parser.add_argument("--origin", default="local_origin", help="local origin root")
    parser.add_argument("--seconds", type=int, default=30, help="generated wav duration")
    args = parser.parse_args()

    ts = time.strftime("%Y%m%d_%H%M%S")
    rel_name = f"release_{ts}"
    release_dir = Path(args.origin) / "channels" / args.channel / "incoming" / rel_name
    audio_path = release_dir / "audio" / "my_track.wav"
    cover_path = release_dir / "images" / "cover.png"
    meta_path = release_dir / "meta.json"

    _write_sine_wav(audio_path, seconds=args.seconds)
    _write_cover_png(cover_path)

    meta: Dict[str, Any] = {
        "channel_slug": args.channel,
        "title": f"DEV Smoke Test ({ts})",
        "description": "DEV release generated locally for pipeline smoke test.",
        "tags": ["#DEV", "#SmokeTest"],
        "assets": {
            "audio": ["audio/my_track.wav"],
            "cover": "images/cover.png"
        }
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Created dev release: {release_dir.resolve()}")
    print("Next: run `make local-up` and verify job goes to WAIT_APPROVAL (mock upload).")


if __name__ == "__main__":
    main()
