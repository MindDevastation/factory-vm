#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path


from services.ops.backup_restore import (
    BackupSettings,
    OpsRestoreError,
    create_backup,
    list_backups,
    prune_backups,
    resolve_snapshot_from_index,
    restore_snapshot,
    verify_backup_by_id,
)

LOGGER = logging.getLogger(__name__)


def _print_error(code: str, message: str) -> int:
    print(f"error_code={code} message={message}")
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Factory VM backup/restore operations")
    sub = parser.add_subparsers(dest="command", required=True)

    backup = sub.add_parser("backup", help="Backup operations")
    backup_sub = backup.add_subparsers(dest="backup_command", required=True)
    backup_sub.add_parser("create", help="Create a timestamped backup snapshot")
    backup_sub.add_parser("list", help="List indexed backups")
    backup_sub.add_parser("prune", help="Prune backups with configured retention policy")
    verify = backup_sub.add_parser("verify", help="Verify a backup manifest and checksums")
    verify.add_argument("--backup-id", required=True, help="Backup id in index.json")

    restore = sub.add_parser("restore", help="Restore from a snapshot directory")
    restore.add_argument("--backup-id", required=True, help="Backup id in index.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        settings = BackupSettings.from_env()
    except ValueError as exc:
        return _print_error("OPS_BACKUP_CONFIG_INVALID", str(exc))

    try:
        if args.command == "backup" and args.backup_command == "create":
            snapshot = create_backup(settings)
            print(f"backup_created={snapshot}")
            return 0

        if args.command == "backup" and args.backup_command == "list":
            for item in list_backups(settings):
                print(f"{item.get('backup_id')}\t{item.get('status')}\t{item.get('created_at')}")
            return 0

        if args.command == "backup" and args.backup_command == "verify":
            snapshot = verify_backup_by_id(settings, args.backup_id)
            print(f"verify_ok backup_id={args.backup_id} snapshot={snapshot}")
            return 0

        if args.command == "backup" and args.backup_command == "prune":
            removed = prune_backups(settings.backup_dir)
            print(f"prune_ok removed={len(removed)}")
            return 0

        marker = Path(os.environ.get("FACTORY_SERVICES_STOPPED_FILE", settings.backup_dir / ".services_stopped"))
        snapshot = resolve_snapshot_from_index(settings, args.backup_id)
        summary = restore_snapshot(settings, snapshot, services_stopped_file=marker)
        print(
            f"restore_ok backup_id={args.backup_id} restore_id={summary['restore_id']} "
            f"quarantine_dir={summary['quarantine_dir']} restored={summary['restored']}"
        )
        return 0
    except OpsRestoreError as exc:
        return _print_error(exc.code, str(exc))
    except Exception:
        LOGGER.exception("ops.backup_restore.cli.failure")
        return _print_error("OPS_BACKUP_CONFIG_INVALID", "backup/restore command failed")


if __name__ == "__main__":
    raise SystemExit(main())
