from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REQ_FILE = Path(__file__).resolve().parents[1] / "requirements-yamnet.txt"


def main() -> int:
    if not REQ_FILE.is_file():
        print(f"requirements file not found: {REQ_FILE}")
        return 2

    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "-r",
        str(REQ_FILE),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "pip install failed").strip()
        print(detail)
        return proc.returncode

    print("yamnet dependencies installed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
