from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from services.common.pydeps import get_py_deps_dir


REQ_FILE = Path(__file__).resolve().parents[1] / "requirements-yamnet.txt"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Install Yamnet runtime dependencies")
    parser.add_argument("--target", default="", help="Shared pip target directory for dependencies")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not REQ_FILE.is_file():
        print(f"requirements file not found: {REQ_FILE}")
        return 2

    target_dir = args.target.strip() or get_py_deps_dir()
    Path(target_dir).mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--upgrade",
        "--target",
        target_dir,
        "-r",
        str(REQ_FILE),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.stderr:
        print(proc.stderr.strip())
    if proc.returncode != 0:
        return proc.returncode

    print(f"yamnet dependencies installed into {target_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
