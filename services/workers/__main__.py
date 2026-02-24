from __future__ import annotations

import argparse
import os
import time
import uuid

from dotenv import load_dotenv
from services.common.profile import load_profile_env

from services.common.env import Env
from services.common.logging_setup import setup_logging, get_logger
from services.workers.importer import importer_cycle
from services.workers.orchestrator import orchestrator_cycle
from services.workers.qa import qa_cycle
from services.workers.uploader import uploader_cycle
from services.workers.cleanup import cleanup_cycle


ROLE_FUNCS = {
    "importer": importer_cycle,
    "orchestrator": orchestrator_cycle,
    "qa": qa_cycle,
    "uploader": uploader_cycle,
    "cleanup": cleanup_cycle,
}


def main() -> None:
    load_profile_env()
    env = Env.load()

    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=list(ROLE_FUNCS.keys()) + ["all"])
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    setup_logging(env, service=f"worker-{args.role}")
    log = get_logger("workers")

    worker_id = f"{args.role}:{uuid.uuid4().hex[:8]}"

    def run_one(role: str) -> None:
        func = ROLE_FUNCS[role]
        try:
            func(env=env, worker_id=worker_id)
        except Exception as e:
            log.exception("worker cycle crashed role=%s err=%s", role, e)

    while True:
        if args.role == "all":
            for r in ROLE_FUNCS.keys():
                run_one(r)
        else:
            run_one(args.role)

        if args.once:
            return

        time.sleep(env.worker_sleep_sec)


if __name__ == "__main__":
    main()
