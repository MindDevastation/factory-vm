#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from services.common.env import Env
from services.ops_retention.config import load_ops_retention_config
from services.ops_retention.runner import execute_retention


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operational artifact retention runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("scan", help="Scan retention candidates without deleting files")
    run_parser = subparsers.add_parser("run", help="Run retention and safely delete policy-approved artifacts")
    run_parser.add_argument("--urgent", action="store_true", help="Reserved urgent mode flag")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger = logging.getLogger("ops.retention")

    env = Env.load()
    cfg = load_ops_retention_config(env)

    execution_mode = "scan" if args.command == "scan" else "run"
    outcome = execute_retention(
        env=env,
        windows=cfg.windows,
        execution_mode=execution_mode,
        logger=logger,
    )

    if getattr(args, "urgent", False):
        logger.info("retention.urgent.mode.not_enabled", extra={"retention_event": {"event_name": "retention.skip", "execution_mode": execution_mode, "result": "urgent_not_enabled"}})

    print(f"retention {execution_mode} complete deleted={outcome.deleted} skipped={outcome.skipped} failed={outcome.failed}")
    return 0 if outcome.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
