from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


_BASE_WORKER_ROLES = ("importer", "orchestrator", "track_jobs", "qa", "uploader", "cleanup")
_ALWAYS_REQUIRED_PROD = ("orchestrator", "qa", "uploader", "cleanup")


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
    resolved_profile = (profile or "").strip().lower() or "prod"
    resolved_no_importer = _parse_runtime_flag(
        "FACTORY_RUNTIME_NO_IMPORTER",
        env.get("FACTORY_RUNTIME_NO_IMPORTER"),
        default=no_importer_flag,
    )
    resolved_with_bot = _parse_runtime_flag(
        "FACTORY_RUNTIME_WITH_BOT",
        env.get("FACTORY_RUNTIME_WITH_BOT"),
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
