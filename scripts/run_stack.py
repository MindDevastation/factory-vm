from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from typing import List

from services.common.profile import load_profile_env
from services.common.env import Env


def _popen(args: List[str]) -> subprocess.Popen:
    return subprocess.Popen(args, stdout=None, stderr=None)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="local", choices=["local", "prod"])
    parser.add_argument("--with-bot", type=int, default=0)
    args = parser.parse_args()

    os.environ["FACTORY_PROFILE"] = args.profile
    load_profile_env()
    env = Env.load()

    procs: List[subprocess.Popen] = []

    def stop_all(*_) -> None:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        time.sleep(1)
        for p in procs:
            try:
                if p.poll() is None:
                    p.kill()
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, stop_all)
    signal.signal(signal.SIGTERM, stop_all)

    py = sys.executable

    procs.append(_popen([py, "-m", "services.factory_api"]))
    time.sleep(0.8)

    for role in ["importer", "orchestrator", "qa", "uploader", "cleanup"]:
        procs.append(_popen([py, "-m", "services.workers", "--role", role]))

    if args.with_bot == 1:
        procs.append(_popen([py, "-m", "services.bot"]))

    print(f"Stack started (profile={args.profile}). Dashboard: http://{env.bind}:{env.port}/")
    print("Press Ctrl+C to stop.")

    while True:
        for p in list(procs):
            code = p.poll()
            if code is not None:
                print(f"[WARN] process exited code={code}: {p.args}")
                procs.remove(p)
        time.sleep(2)


if __name__ == "__main__":
    main()
