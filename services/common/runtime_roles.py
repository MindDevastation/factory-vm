from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


_BASE_WORKER_ROLES = ("importer", "orchestrator", "track_jobs", "qa", "uploader", "cleanup")
_ALWAYS_REQUIRED_PROD = ("orchestrator", "qa", "uploader", "cleanup")
_RUNTIME_INPUTS_FILE_ENV = "FACTORY_RUNTIME_INPUTS_FILE"
_DEFAULT_RUNTIME_INPUTS_FILE = "/tmp/factory_runtime_inputs.json"


@dataclass(frozen=True)
class RuntimeRoleResolution:
    resolved_profile: str
    required_roles: list[str]
    optional_roles: list[str]
    importer_enabled: bool
    bot_enabled: bool
    track_catalog_enabled: bool


@dataclass(frozen=True)
class RuntimeRoleInputs:
    profile: str
    no_importer_flag: bool
    with_bot_flag: bool


def runtime_inputs_store_path(*, environ: Mapping[str, str] | None = None) -> Path:
    env = environ if environ is not None else os.environ
    return Path(env.get(_RUNTIME_INPUTS_FILE_ENV, _DEFAULT_RUNTIME_INPUTS_FILE)).expanduser()


def persist_runtime_role_inputs(inputs: RuntimeRoleInputs, *, environ: Mapping[str, str] | None = None) -> None:
    path = runtime_inputs_store_path(environ=environ)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "profile": inputs.profile,
        "no_importer_flag": "1" if inputs.no_importer_flag else "0",
        "with_bot_flag": "1" if inputs.with_bot_flag else "0",
    }
    tmp_path.write_text(json.dumps(payload), encoding="utf-8")
    tmp_path.replace(path)


def _read_persisted_runtime_inputs(*, environ: Mapping[str, str] | None = None) -> Mapping[str, str]:
    path = runtime_inputs_store_path(environ=environ)
    if not path.is_file():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return {str(k): str(v) for k, v in raw.items()}


def _parse_enabled(raw: str | None, *, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _parse_runtime_flag(name: str, raw: str | None, *, default: bool) -> bool:
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be one of: 0,1,false,true,no,yes,off,on")


def runtime_role_inputs_from_runtime(*, profile: str, no_importer_flag: bool = False, with_bot_flag: bool = False, environ: Mapping[str, str] | None = None) -> RuntimeRoleInputs:
    env = environ if environ is not None else os.environ
    persisted = _read_persisted_runtime_inputs(environ=env)

    runtime_profile = (profile or "").strip().lower()
    persisted_profile = persisted.get("profile", "").strip().lower()
    resolved_profile = runtime_profile or persisted_profile or "prod"

    no_importer_raw = env.get("FACTORY_RUNTIME_NO_IMPORTER")
    if no_importer_raw is None:
        no_importer_raw = persisted.get("no_importer_flag")
    resolved_no_importer = _parse_runtime_flag(
        "FACTORY_RUNTIME_NO_IMPORTER",
        no_importer_raw,
        default=no_importer_flag,
    )

    with_bot_raw = env.get("FACTORY_RUNTIME_WITH_BOT")
    if with_bot_raw is None:
        with_bot_raw = persisted.get("with_bot_flag")
    resolved_with_bot = _parse_runtime_flag(
        "FACTORY_RUNTIME_WITH_BOT",
        with_bot_raw,
        default=with_bot_flag,
    )
    return RuntimeRoleInputs(
        profile=resolved_profile,
        no_importer_flag=resolved_no_importer,
        with_bot_flag=resolved_with_bot,
    )


def importer_enabled(*, no_importer_flag: bool = False, environ: Mapping[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return _parse_enabled(env.get("IMPORTER_ENABLED"), default=not no_importer_flag)


def bot_enabled(*, with_bot_flag: bool = False, environ: Mapping[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return _parse_enabled(env.get("BOT_ENABLED"), default=with_bot_flag)


def track_catalog_enabled(*, environ: Mapping[str, str] | None = None) -> bool:
    env = environ if environ is not None else os.environ
    return _parse_enabled(env.get("TRACK_CATALOG_ENABLED"), default=True)


def worker_roles_for_runtime(*, no_importer_flag: bool = False, with_bot_flag: bool = False, environ: Mapping[str, str] | None = None) -> list[str]:
    env = environ if environ is not None else os.environ
    roles = list(_BASE_WORKER_ROLES)
    if not importer_enabled(no_importer_flag=no_importer_flag, environ=env):
        roles.remove("importer")
    if not track_catalog_enabled(environ=env) and "track_jobs" in roles:
        roles.remove("track_jobs")
    if bot_enabled(with_bot_flag=with_bot_flag, environ=env):
        roles.append("bot")
    return roles


def launched_worker_roles_for_runtime(*, profile: str, no_importer_flag: bool = False, with_bot_flag: bool = False, environ: Mapping[str, str] | None = None) -> list[str]:
    resolved = resolve_required_runtime_roles(
        profile=profile,
        no_importer_flag=no_importer_flag,
        with_bot_flag=with_bot_flag,
        environ=environ,
    )
    if resolved.resolved_profile == "prod":
        return list(resolved.required_roles)
    return list(resolved.optional_roles)


def resolve_required_runtime_roles(*, profile: str, no_importer_flag: bool = False, with_bot_flag: bool = False, environ: Mapping[str, str] | None = None) -> RuntimeRoleResolution:
    env = environ if environ is not None else os.environ
    inputs = runtime_role_inputs_from_runtime(
        profile=profile,
        no_importer_flag=no_importer_flag,
        with_bot_flag=with_bot_flag,
        environ=env,
    )
    resolved_profile = inputs.profile

    importer_on = importer_enabled(no_importer_flag=inputs.no_importer_flag, environ=env)
    bot_on = bot_enabled(with_bot_flag=inputs.with_bot_flag, environ=env)
    track_on = track_catalog_enabled(environ=env)

    required: list[str] = []
    optional: list[str] = []

    if resolved_profile == "prod":
        required.extend(_ALWAYS_REQUIRED_PROD)
        if importer_on:
            required.append("importer")
        else:
            optional.append("importer")
        if bot_on:
            required.append("bot")
        else:
            optional.append("bot")
        if track_on:
            required.append("track_jobs")
        else:
            optional.append("track_jobs")
    else:
        optional.extend(
            worker_roles_for_runtime(
                no_importer_flag=inputs.no_importer_flag,
                with_bot_flag=inputs.with_bot_flag,
                environ=env,
            )
        )

    return RuntimeRoleResolution(
        resolved_profile=resolved_profile,
        required_roles=required,
        optional_roles=optional,
        importer_enabled=importer_on,
        bot_enabled=bot_on,
        track_catalog_enabled=track_on,
    )
