#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from services.ops.backup_restore import BackupSettings, create_backup, list_snapshots, restore_snapshot


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Factory VM backup/restore operations")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("backup", help="Create a timestamped backup snapshot")
    sub.add_parser("list", help="List existing snapshot directories")

    restore = sub.add_parser("restore", help="Restore from a snapshot directory")
    restore.add_argument("--snapshot", required=True, help="Snapshot directory name under FACTORY_BACKUP_DIR")
    restore.add_argument(
        "--services-stopped-file",
        required=True,
        help="Path to a file that must exist to confirm services are stopped before restore",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = BackupSettings.from_env()

    if args.command == "backup":
        snapshot = create_backup(settings)
        print(f"backup_created={snapshot}")
        return 0

    if args.command == "list":
        for item in list_snapshots(settings):
            print(item.name)
        return 0

    snapshot_dir = settings.backup_dir / args.snapshot
    if not snapshot_dir.exists():
        print(f"snapshot_not_found={snapshot_dir}")
        return 2

    restore_snapshot(settings, snapshot_dir, services_stopped_file=Path(args.services_stopped_file))
    print(f"restore_completed={snapshot_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
