#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from scripts.ops_backup_restore import main as backup_restore_main


def _latest_successful_backup_id(backup_dir: Path) -> str:
    latest_path = backup_dir / "latest_successful"
    backup_id = latest_path.read_text(encoding="utf-8").strip()
    if not backup_id:
        raise ValueError("latest_successful marker is empty")
    return backup_id


def _run_once(*, verify: bool = True) -> int:
    create_code = backup_restore_main(["backup", "create"])
    if create_code != 0:
        return create_code

    if not verify:
        return 0

    backup_dir = Path(os.environ["FACTORY_BACKUP_DIR"])
    backup_id = _latest_successful_backup_id(backup_dir)
    verify_code = backup_restore_main(["backup", "verify", "--backup-id", backup_id])
    if verify_code == 0:
        print(f"scheduled_backup_ok backup_id={backup_id}")
    return verify_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scheduled backup wrapper for systemd timers")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Create backup snapshot and verify latest successful backup")
    run.add_argument(
        "--skip-verify",
        action="store_true",
        help="Only create backup snapshot (verification step disabled)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "run":
        try:
            return _run_once(verify=not args.skip_verify)
        except KeyError:
            print("error_code=OPS_BACKUP_CONFIG_INVALID message=FACTORY_BACKUP_DIR is required")
            return 2
        except (OSError, ValueError) as exc:
            print(f"error_code=OPS_BACKUP_CONFIG_INVALID message={exc}")
            return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
