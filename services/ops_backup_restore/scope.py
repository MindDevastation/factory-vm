from __future__ import annotations

import os
from pathlib import Path

from services.ops_backup_restore.models import BackupScope


def _required_env(env: dict[str, str], key: str) -> str:
    value = env.get(key, "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _parse_colon_list(raw: str) -> tuple[Path, ...]:
    value = raw.strip()
    if not value:
        return tuple()
    return tuple(Path(part.strip()) for part in value.split(":") if part.strip())


def resolve_backup_scope(env: dict[str, str] | None = None) -> BackupScope:
    source = dict(os.environ if env is None else env)
    return BackupScope(
        backup_dir=Path(_required_env(source, "FACTORY_BACKUP_DIR")),
        db_path=Path(_required_env(source, "FACTORY_DB_PATH")),
        env_files=_parse_colon_list(source.get("FACTORY_ENV_FILES", "")),
        config_paths=_parse_colon_list(source.get("FACTORY_BACKUP_CONFIG_PATHS", "")),
        export_paths=_parse_colon_list(source.get("FACTORY_BACKUP_EXPORT_DIRS", "")),
    )
