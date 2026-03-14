#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging

from services.common.disk_guard import emit_disk_pressure_event, evaluate_disk_pressure_for_env
from services.common.env import Env
from services.ops_retention.config import load_ops_retention_config
from services.ops_retention.runner import execute_retention


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operational artifact retention runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("scan", help="Scan retention candidates without deleting files")
    run_parser = subparsers.add_parser("run", help="Run retention and safely delete policy-approved artifacts")
    run_parser.add_argument("--urgent", action="store_true", help="Enable urgent retention reductions only under critical disk pressure")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logger = logging.getLogger("ops.retention")

    env = Env.load()
    cfg = load_ops_retention_config(env)
    disk_snapshot = evaluate_disk_pressure_for_env(env=env)
    emit_disk_pressure_event(logger=logger, snapshot=disk_snapshot, stage="ops_retention")

    execution_mode = "scan" if args.command == "scan" else "run"
    outcome = execute_retention(
        env=env,
        windows=cfg.windows,
        execution_mode=execution_mode,
        logger=logger,
        disk_pressure=disk_snapshot.pressure,
        urgent_requested=bool(getattr(args, "urgent", False)),
    )

    print(f"retention {execution_mode} complete deleted={outcome.deleted} skipped={outcome.skipped} failed={outcome.failed}")
    return 0 if outcome.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
