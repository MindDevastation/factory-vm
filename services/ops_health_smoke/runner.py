from __future__ import annotations

import base64
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from services.common.env import Env
from services.common.disk_thresholds import DiskThresholds, evaluate_disk_status, load_disk_thresholds
from services.common.runtime_roles import resolve_required_runtime_roles, runtime_role_inputs_from_runtime

from .models import CheckResult, OverallStatus, SmokeSummary


class SmokeCheck(Protocol):
    check_id: str
    title: str
    category: str
    severity: str

    def run(self, context: "SmokeContext") -> CheckResult:
        ...


@dataclass(frozen=True)
class SmokeContext:
    profile: str
    env: Env
    prior_results: tuple[CheckResult, ...] = ()


def _local_api_base_url(env: Env) -> str:
    host = env.bind.strip() or "127.0.0.1"
    if host in {"0.0.0.0", "::"}:
        host = "127.0.0.1"
    return f"http://{host}:{env.port}"


def _resolve_storage_paths(env: Env) -> tuple[list[Path], list[Path]]:
    storage_root = Path(env.storage_root).expanduser().resolve()
    required = [
        Path(env.db_path).expanduser().resolve().parent,
        storage_root,
        storage_root / "workspace",
        storage_root / "outbox",
        storage_root / "logs",
        storage_root / "qa",
        storage_root / "previews",
    ]
    optional = []
    if env.origin_backend == "local":
        optional.append(Path(env.origin_local_root).expanduser().resolve())
    return required, optional


def _evaluate_disk_status(*, free_percent: float, free_gib: float, warn_percent: float, warn_gib: float, fail_percent: float, fail_gib: float) -> str:
    thresholds = DiskThresholds(
        warn_percent=warn_percent,
        warn_gib=warn_gib,
        fail_percent=fail_percent,
        fail_gib=fail_gib,
    )
    return evaluate_disk_status(free_percent=free_percent, free_gib=free_gib, thresholds=thresholds)


def _workers_api_url(env: Env) -> str:
    return f"{_local_api_base_url(env)}/v1/workers"


def _resolved_runtime_roles(context: "SmokeContext"):
    inputs = runtime_role_inputs_from_runtime(profile=context.profile)
    return resolve_required_runtime_roles(
        profile=inputs.profile,
        no_importer_flag=inputs.no_importer_flag,
        with_bot_flag=inputs.with_bot_flag,
    )


def _fetch_workers(env: Env) -> list[dict[str, Any]]:
    url = _workers_api_url(env)
    creds = f"{env.basic_user}:{env.basic_pass}".encode("utf-8")
    token = base64.b64encode(creds).decode("ascii")
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {token}"})
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        status_attr = getattr(resp, "status", None)
        status = int(status_attr if status_attr is not None else resp.getcode())
        if status != 200:
            raise ValueError(f"workers endpoint returned HTTP {status}")
        payload = json.loads(resp.read().decode("utf-8"))
    workers = payload.get("workers", [])
    if not isinstance(workers, list):
        raise ValueError("workers payload malformed")
    return [w for w in workers if isinstance(w, dict)]


def _evaluate_worker_heartbeats(*, workers: list[dict[str, Any]], stale_after_sec: int, now_ts: float | None = None) -> dict[str, Any]:
    current_ts = float(now_ts if now_ts is not None else time.time())
    active_workers: list[str] = []
    heartbeat_ages: dict[str, float | None] = {}
    stale_workers: list[str] = []
    present_roles: set[str] = set()
    stale_roles: set[str] = set()

    for worker in workers:
        worker_id = str(worker.get("worker_id") or "")
        role = str(worker.get("role") or "")
        last_seen_raw = worker.get("last_seen")
        age: float | None = None
        if last_seen_raw is not None:
            try:
                age = max(0.0, current_ts - float(last_seen_raw))
            except (TypeError, ValueError):
                age = None

        active = bool(worker_id) and age is not None and age <= stale_after_sec
        stale = bool(worker_id) and (age is None or age > stale_after_sec)

        if worker_id:
            heartbeat_ages[worker_id] = None if age is None else round(age, 2)
        if active:
            active_workers.append(worker_id)
        if stale:
            stale_workers.append(worker_id)

        if role:
            present_roles.add(role)
            if stale:
                stale_roles.add(role)

    return {
        "active_workers": sorted(active_workers),
        "heartbeat_ages": heartbeat_ages,
        "stale_workers": sorted(stale_workers),
        "present_roles": sorted(present_roles),
        "stale_roles": sorted(stale_roles),
    }


class RunnerBootstrapCheck:
    check_id = "runner_bootstrap"
    title = "Smoke runner bootstrap"
    category = "framework"
    severity = "info"

    def run(self, context: SmokeContext) -> CheckResult:
        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result="PASS",
            message=f"Smoke runner initialized for profile '{context.profile}'",
            details={"profile": context.profile},
        )


class ApiHealthCheck:
    check_id = "api_health"
    title = "API health endpoint"
    category = "local/core"
    severity = "critical"

    def run(self, context: SmokeContext) -> CheckResult:
        url = f"{_local_api_base_url(context.env)}/health"
        started = time.monotonic()
        status = None
        payload: Any = None
        try:
            with urllib.request.urlopen(url, timeout=2.0) as resp:
                status_attr = getattr(resp, "status", None)
                status = int(status_attr if status_attr is not None else resp.getcode())
                payload = resp.read().decode("utf-8")
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message=f"Health endpoint check failed: {exc}",
                details={"url": url, "http_status": status, "duration_ms": int((time.monotonic() - started) * 1000)},
            )

        duration_ms = int((time.monotonic() - started) * 1000)
        if status != 200:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message=f"Health endpoint returned HTTP {status}",
                details={"url": url, "http_status": status, "duration_ms": duration_ms},
            )

        is_healthy = False
        malformed = False
        try:
            import json

            parsed = json.loads(payload)
            is_healthy = bool(parsed.get("ok") is True)
        except Exception:
            malformed = True

        if malformed:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message="Health endpoint returned malformed payload",
                details={"url": url, "http_status": status, "duration_ms": duration_ms},
            )
        if not is_healthy:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message="Health endpoint payload indicates unhealthy state",
                details={"url": url, "http_status": status, "duration_ms": duration_ms},
            )

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result="PASS",
            message="Health endpoint is reachable and healthy",
            details={"url": url, "http_status": status, "duration_ms": duration_ms},
        )


class DbAccessCheck:
    check_id = "db_access"
    title = "Database accessibility"
    category = "local/core"
    severity = "critical"

    def run(self, context: SmokeContext) -> CheckResult:
        db_path = Path(context.env.db_path).expanduser().resolve()
        exists = db_path.is_file()
        if not exists:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message="Database file is missing",
                details={"db_path": str(db_path), "file_exists": False, "quick_check_result": None},
            )

        quick_check_result = ""
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
            try:
                row = conn.execute("PRAGMA quick_check;").fetchone()
                quick_check_result = str(row[0]) if row else ""
            finally:
                conn.close()
        except sqlite3.Error as exc:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message=f"Database quick_check failed: {exc}",
                details={"db_path": str(db_path), "file_exists": True, "quick_check_result": None},
            )

        result = "PASS" if quick_check_result.lower() == "ok" else "FAIL"
        message = "Database is accessible" if result == "PASS" else f"Database quick_check returned {quick_check_result!r}"
        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result=result,
            message=message,
            details={"db_path": str(db_path), "file_exists": True, "quick_check_result": quick_check_result},
        )


class StoragePathsCheck:
    check_id = "storage_paths"
    title = "Runtime storage paths"
    category = "local/core"
    severity = "critical"

    def run(self, context: SmokeContext) -> CheckResult:
        required, optional = _resolve_storage_paths(context.env)

        missing_required = [str(path) for path in required if not path.exists()]
        missing_optional = [str(path) for path in optional if not path.exists()]
        inaccessible_required = [str(path) for path in required if path.exists() and not os.access(path, os.R_OK | os.W_OK | os.X_OK)]

        details = {
            "required_paths": [str(path) for path in required],
            "optional_paths": [str(path) for path in optional],
            "missing_required": missing_required,
            "inaccessible_required": inaccessible_required,
            "missing_optional": missing_optional,
        }
        if missing_required or inaccessible_required:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message="One or more required runtime paths are missing/inaccessible",
                details=details,
            )
        if missing_optional:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="WARN",
                message="Optional runtime paths are missing",
                details=details,
            )
        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result="PASS",
            message="All required runtime paths are accessible",
            details=details,
        )


class FfmpegAvailableCheck:
    check_id = "ffmpeg_available"
    title = "FFmpeg availability"
    category = "local/core"
    severity = "critical"

    def run(self, context: SmokeContext) -> CheckResult:
        ffmpeg_path = shutil.which("ffmpeg")
        details = {"resolved_path": ffmpeg_path, "version_first_line": None}
        if not ffmpeg_path:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message="ffmpeg binary not found",
                details=details,
            )
        try:
            completed = subprocess.run([ffmpeg_path, "-version"], capture_output=True, text=True, timeout=5)
        except Exception as exc:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message=f"ffmpeg invocation failed: {exc}",
                details=details,
            )

        first_line = (completed.stdout or completed.stderr).splitlines()[0] if (completed.stdout or completed.stderr) else ""
        details["version_first_line"] = first_line
        if completed.returncode != 0:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message="ffmpeg -version returned non-zero exit code",
                details=details,
            )

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result="PASS",
            message="ffmpeg is available",
            details=details,
        )


class DiskSpaceCheck:
    check_id = "disk_space"
    title = "Disk space thresholds"
    category = "local/core"
    severity = "warning"

    @staticmethod
    def _nearest_existing_ancestor(path: Path) -> Path | None:
        current = path
        while True:
            if current.exists():
                return current
            parent = current.parent
            if parent == current:
                return None
            current = parent

    def run(self, context: SmokeContext) -> CheckResult:
        thresholds = load_disk_thresholds()

        monitored_paths = [
            Path(context.env.db_path).expanduser().resolve(),
            Path(context.env.storage_root).expanduser().resolve(),
            Path(context.env.storage_root).expanduser().resolve() / "workspace",
            Path(context.env.storage_root).expanduser().resolve() / "outbox",
        ]
        seen_mounts: set[str] = set()
        path_details: list[dict[str, Any]] = []
        overall_result = "PASS"

        for monitored in monitored_paths:
            target = self._nearest_existing_ancestor(monitored)
            if target is None:
                path_details.append(
                    {
                        "monitored_path": str(monitored),
                        "total_bytes": None,
                        "free_bytes": None,
                        "free_percent": None,
                        "status": "FAIL",
                        "error": "no_existing_ancestor",
                    }
                )
                overall_result = "FAIL"
                continue

            try:
                usage = shutil.disk_usage(target)
            except OSError as exc:
                path_details.append(
                    {
                        "monitored_path": str(monitored),
                        "total_bytes": None,
                        "free_bytes": None,
                        "free_percent": None,
                        "status": "FAIL",
                        "error": f"disk_usage_error:{exc.__class__.__name__}",
                    }
                )
                overall_result = "FAIL"
                continue
            free_percent = (usage.free / usage.total) * 100.0 if usage.total else 0.0
            free_gib = usage.free / (1024**3)
            status = evaluate_disk_status(free_percent=free_percent, free_gib=free_gib, thresholds=thresholds)
            mount_key = str(target)
            if mount_key in seen_mounts:
                continue
            seen_mounts.add(mount_key)
            path_details.append(
                {
                    "monitored_path": str(monitored),
                    "total_bytes": usage.total,
                    "free_bytes": usage.free,
                    "free_percent": round(free_percent, 2),
                    "status": status,
                }
            )
            if status == "FAIL":
                overall_result = "FAIL"
            elif status == "WARN" and overall_result != "FAIL":
                overall_result = "WARN"

        message = "Disk free space is within thresholds"
        if overall_result == "WARN":
            message = "Disk free space is below warning threshold"
        elif overall_result == "FAIL":
            message = "Disk free space is below fail threshold"

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result=overall_result,
            message=message,
            details={
                "thresholds": {
                    "warn_percent": thresholds.warn_percent,
                    "warn_gib": thresholds.warn_gib,
                    "fail_percent": thresholds.fail_percent,
                    "fail_gib": thresholds.fail_gib,
                },
                "paths": path_details,
            },
        )


class WorkerHeartbeatCheck:
    check_id = "worker_heartbeat"
    title = "Worker heartbeat freshness"
    category = "local/runtime"
    severity = "critical"

    def run(self, context: SmokeContext) -> CheckResult:
        stale_after_sec = int(os.environ.get("FACTORY_SMOKE_WORKER_STALE_SEC", "120"))
        try:
            workers = _fetch_workers(context.env)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message=f"Worker heartbeat check failed: {exc}",
                details={"stale_after_sec": stale_after_sec},
            )

        eval_result = _evaluate_worker_heartbeats(workers=workers, stale_after_sec=stale_after_sec)
        stale_workers = eval_result["stale_workers"]
        result = "PASS" if not stale_workers else "FAIL"
        message = "All workers are fresh" if result == "PASS" else "Stale or missing worker heartbeat detected"

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result=result,
            message=message,
            details={"stale_after_sec": stale_after_sec, **eval_result},
        )


class RequiredRuntimeRolesCheck:
    check_id = "required_runtime_roles"
    title = "Required runtime roles"
    category = "local/runtime"
    severity = "critical"

    def run(self, context: SmokeContext) -> CheckResult:
        stale_after_sec = int(os.environ.get("FACTORY_SMOKE_WORKER_STALE_SEC", "120"))
        inputs = runtime_role_inputs_from_runtime(profile=context.profile)
        resolved = _resolved_runtime_roles(context)

        try:
            workers = _fetch_workers(context.env)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            return CheckResult(
                check_id=self.check_id,
                title=self.title,
                category=self.category,
                severity=self.severity,
                result="FAIL",
                message=f"Required runtime role check failed: {exc}",
                details={
                    "resolved_profile": resolved.resolved_profile,
                    "required_roles": resolved.required_roles,
                    "optional_roles": resolved.optional_roles,
                    "stale_after_sec": stale_after_sec,
                    "runtime_inputs": {"no_importer_flag": inputs.no_importer_flag, "with_bot_flag": inputs.with_bot_flag},
                },
            )

        eval_result = _evaluate_worker_heartbeats(workers=workers, stale_after_sec=stale_after_sec)
        present_roles = set(eval_result["present_roles"])
        stale_roles = set(eval_result["stale_roles"])
        required_roles = set(resolved.required_roles)

        missing_required = sorted(role for role in required_roles if role not in present_roles)
        stale_required = sorted(role for role in required_roles if role in stale_roles)

        result = "PASS"
        message = "All required runtime roles are present and fresh"
        if missing_required or stale_required:
            result = "FAIL"
            message = "One or more required runtime roles are missing or stale"

        details = {
            "resolved_profile": resolved.resolved_profile,
            "required_roles": sorted(required_roles),
            "optional_roles": sorted(set(resolved.optional_roles)),
            "present_roles": sorted(present_roles),
            "stale_roles": sorted(stale_roles),
            "missing_roles": missing_required,
            "stale_after_sec": stale_after_sec,
            "runtime_inputs": {"no_importer_flag": inputs.no_importer_flag, "with_bot_flag": inputs.with_bot_flag},
        }
        if stale_required:
            details["stale_required_roles"] = stale_required

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result=result,
            message=message,
            details=details,
        )


class PipelineReadinessCheck:
    check_id = "pipeline_readiness"
    title = "Pipeline readiness gate"
    category = "local/core"
    severity = "critical"

    @staticmethod
    def _planner_enabled() -> bool:
        raw = os.environ.get("FACTORY_PLANNER_ENABLED")
        if raw is None:
            return True
        return raw.strip().lower() not in {"0", "false", "no", "off"}

    @staticmethod
    def _planner_structures_ready(*, db_path: str) -> tuple[bool, dict[str, Any]]:
        expected_columns = {
            "id",
            "channel_slug",
            "content_type",
            "title",
            "publish_at",
            "notes",
            "status",
            "created_at",
            "updated_at",
        }
        resolved_path = Path(db_path).expanduser().resolve()
        details: dict[str, Any] = {
            "planner_table": "planned_releases",
            "planner_db_path": str(resolved_path),
            "planner_required_columns": sorted(expected_columns),
            "planner_table_exists": False,
            "planner_missing_columns": sorted(expected_columns),
        }
        if not resolved_path.is_file():
            details["planner_schema_error"] = "planner_db_path_missing"
            return False, details

        try:
            with sqlite3.connect(f"file:{resolved_path}?mode=ro", uri=True, timeout=2) as conn:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
                    ("planned_releases",),
                ).fetchone()
                if not row:
                    return False, details
                details["planner_table_exists"] = True

                rows = conn.execute("PRAGMA table_info(planned_releases)").fetchall()
                actual_columns = {str(raw[1]) for raw in rows if len(raw) > 1}
        except sqlite3.Error as exc:
            details["planner_schema_error"] = str(exc)
            return False, details

        missing = sorted(expected_columns - actual_columns)
        details["planner_missing_columns"] = missing
        return not missing, details

    def run(self, context: SmokeContext) -> CheckResult:
        prior = {result.check_id: result for result in context.prior_results}
        resolved_roles = _resolved_runtime_roles(context)

        db_result = prior.get("db_access")
        api_result = prior.get("api_health")
        storage_result = prior.get("storage_paths")
        ffmpeg_result = prior.get("ffmpeg_available")
        worker_roles_result = prior.get("required_runtime_roles")
        youtube_result = prior.get("youtube_ready")

        db_ready = bool(db_result and db_result.result == "PASS")
        api_ready = bool(api_result and api_result.result == "PASS")
        workers_ready = bool(worker_roles_result and worker_roles_result.result == "PASS")
        storage_ready = bool(storage_result and storage_result.result == "PASS")
        render_dependency_ready = bool(ffmpeg_result and ffmpeg_result.result == "PASS")
        uploader_ready = bool(youtube_result and youtube_result.result == "PASS") if context.env.upload_backend == "youtube" else True

        planner_enabled = self._planner_enabled()
        planner_structures_ready = True
        planner_details: dict[str, Any] = {}
        if planner_enabled:
            db_path = (db_result.details.get("db_path") if db_result else None) or str(context.env.db_path)
            planner_structures_ready, planner_details = self._planner_structures_ready(db_path=db_path)

        blockers: list[str] = []
        if not db_ready:
            blockers.append("db_access")
        if not api_ready:
            blockers.append("api_health")
        if not storage_ready:
            blockers.append("storage_paths")
        if not render_dependency_ready:
            blockers.append("ffmpeg_available")
        if not workers_ready:
            blockers.append("required_runtime_roles")
        if not uploader_ready:
            blockers.append("youtube_ready")
        if planner_enabled and not planner_structures_ready:
            blockers.append("planner_data_structures")
        if resolved_roles.track_catalog_enabled and not workers_ready:
            blockers.append("track_jobs_role_not_ready")

        unique_blockers = sorted(set(blockers))
        result = "PASS" if not unique_blockers else "FAIL"
        message = "Pipeline is ready for production jobs" if result == "PASS" else "Pipeline is not ready for production jobs"
        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result=result,
            message=message,
            details={
                "planner_enabled": planner_enabled,
                "uploader_ready": uploader_ready,
                "workers_ready": workers_ready,
                "db_ready": db_ready,
                "storage_ready": storage_ready,
                "render_dependency_ready": render_dependency_ready,
                "integration_blockers": unique_blockers,
                "planner_structures_ready": planner_structures_ready,
                "api_ready": api_ready,
                **planner_details,
            },
        )


class TelegramReadyCheck:
    check_id = "telegram_ready"
    title = "Telegram readiness"
    category = "integrations"
    severity = "warning"

    def run(self, context: SmokeContext) -> CheckResult:
        resolved = _resolved_runtime_roles(context)
        bot_required = "bot" in set(resolved.required_roles)
        severity = "critical" if bot_required else "warning"

        config_present = bool(context.env.tg_bot_token and context.env.tg_admin_chat_id)
        init_ok = False
        init_error = None
        if config_present:
            try:
                from aiogram import Bot
                from aiogram.enums import ParseMode

                _ = Bot(token=context.env.tg_bot_token, parse_mode=ParseMode.HTML)
                init_ok = True
            except Exception as exc:
                init_error = str(exc)

        result = "PASS" if config_present and init_ok else "FAIL"
        if result == "FAIL" and severity == "warning":
            result = "WARN"

        details = {
            "config_present": config_present,
            "init_ok": init_ok,
            "bot_required_by_profile": bot_required,
        }
        if init_error:
            details["init_error"] = init_error

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=severity,
            result=result,
            message="Telegram config and local client init are ready" if result == "PASS" else "Telegram readiness is incomplete",
            details=details,
        )


class YouTubeReadyCheck:
    check_id = "youtube_ready"
    title = "YouTube readiness"
    category = "integrations"
    severity = "warning"

    def run(self, context: SmokeContext) -> CheckResult:
        upload_capable = context.profile == "prod" and context.env.upload_backend == "youtube"
        severity = "critical" if upload_capable else "warning"

        channel_slug = os.environ.get("FACTORY_YT_CHANNEL_SLUG", "").strip()
        tokens_root = Path(context.env.yt_tokens_dir).expanduser().resolve() if context.env.yt_tokens_dir else None
        token_path = tokens_root / channel_slug / "token.json" if tokens_root and channel_slug else None
        client_secret_path = Path(context.env.yt_client_secret_json).expanduser().resolve() if context.env.yt_client_secret_json else None

        config_paths_present = bool(
            client_secret_path
            and token_path
            and client_secret_path.is_file()
            and token_path.is_file()
        )

        token_load_ok = False
        client_load_ok = False
        token_error = None
        client_error = None
        if config_paths_present:
            try:
                from google.oauth2.credentials import Credentials

                creds = Credentials.from_authorized_user_file(str(token_path), ["https://www.googleapis.com/auth/youtube.upload"])
                token_load_ok = bool(creds.token)
            except Exception as exc:
                token_error = str(exc)

            try:
                token_json = json.loads(token_path.read_text(encoding="utf-8"))
                secret_json = json.loads(client_secret_path.read_text(encoding="utf-8"))
                has_installed = isinstance(secret_json, dict) and any(k in secret_json for k in ("web", "installed"))
                has_token_shape = isinstance(token_json, dict) and any(k in token_json for k in ("access_token", "refresh_token", "token"))
                client_load_ok = bool(has_installed and has_token_shape)
            except Exception as exc:
                client_error = str(exc)

        result = "PASS" if config_paths_present and token_load_ok and client_load_ok else "FAIL"
        if result == "FAIL" and severity == "warning":
            result = "WARN"

        details = {
            "channel_slug": channel_slug,
            "channel_context_available": bool(channel_slug),
            "token_path": str(token_path) if token_path else None,
            "config_paths_present": config_paths_present,
            "token_load_ok": token_load_ok,
            "client_load_ok": client_load_ok,
        }
        if not channel_slug:
            details["channel_context_error"] = "FACTORY_YT_CHANNEL_SLUG is not set"
        if token_error:
            details["token_error"] = token_error
        if client_error:
            details["client_error"] = client_error

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=severity,
            result=result,
            message="YouTube config/token parsed without upload actions" if result == "PASS" else "YouTube readiness is incomplete",
            details=details,
        )


class GDriveReadyCheck:
    check_id = "gdrive_ready"
    title = "Google Drive readiness"
    category = "integrations"
    severity = "warning"

    def run(self, context: SmokeContext) -> CheckResult:
        resolved = _resolved_runtime_roles(context)
        drive_flow_enabled = context.env.origin_backend == "gdrive" and resolved.importer_enabled
        severity = "critical" if drive_flow_enabled else "warning"

        sa_path = Path(context.env.gdrive_sa_json).expanduser().resolve() if context.env.gdrive_sa_json else None
        oauth_client = Path(context.env.gdrive_oauth_client_json).expanduser().resolve() if context.env.gdrive_oauth_client_json else None
        oauth_token = Path(context.env.gdrive_oauth_token_json).expanduser().resolve() if context.env.gdrive_oauth_token_json else None

        service_account_mode = bool(sa_path and sa_path.is_file())
        oauth_mode = bool(oauth_client and oauth_token and oauth_client.is_file() and oauth_token.is_file())
        credential_path_present = bool(service_account_mode or oauth_mode)

        credential_parse_ok = False
        client_init_ok = False
        parse_error = None
        init_error = None
        if credential_path_present:
            try:
                if service_account_mode:
                    from google.oauth2 import service_account

                    creds = service_account.Credentials.from_service_account_file(
                        str(sa_path), scopes=["https://www.googleapis.com/auth/drive"]
                    )
                    credential_parse_ok = bool(getattr(creds, "service_account_email", ""))
                else:
                    from google.oauth2.credentials import Credentials

                    creds = Credentials.from_authorized_user_file(
                        str(oauth_token), ["https://www.googleapis.com/auth/drive"]
                    )
                    credential_parse_ok = bool(creds.token)
            except Exception as exc:
                parse_error = str(exc)

            if credential_parse_ok:
                try:
                    if service_account_mode:
                        secret_json = json.loads(sa_path.read_text(encoding="utf-8"))
                        client_init_ok = isinstance(secret_json, dict) and secret_json.get("type") == "service_account"
                    else:
                        client_json = json.loads(oauth_client.read_text(encoding="utf-8"))
                        token_json = json.loads(oauth_token.read_text(encoding="utf-8"))
                        has_client = isinstance(client_json, dict) and any(k in client_json for k in ("web", "installed"))
                        has_token = isinstance(token_json, dict) and any(k in token_json for k in ("access_token", "refresh_token", "token"))
                        client_init_ok = bool(has_client and has_token)
                except Exception as exc:
                    init_error = str(exc)

        result = "PASS" if credential_path_present and credential_parse_ok and client_init_ok else "FAIL"
        if result == "FAIL" and severity == "warning":
            result = "WARN"

        details = {
            "credential_path_present": credential_path_present,
            "credential_parse_ok": credential_parse_ok,
            "client_init_ok": client_init_ok,
        }
        if parse_error:
            details["credential_error"] = parse_error
        if init_error:
            details["client_error"] = init_error

        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=severity,
            result=result,
            message="Google Drive credentials parsed and initialized locally" if result == "PASS" else "Google Drive readiness is incomplete",
            details=details,
        )


def default_checks() -> list[SmokeCheck]:
    return [
        RunnerBootstrapCheck(),
        ApiHealthCheck(),
        DbAccessCheck(),
        StoragePathsCheck(),
        FfmpegAvailableCheck(),
        TelegramReadyCheck(),
        YouTubeReadyCheck(),
        GDriveReadyCheck(),
        WorkerHeartbeatCheck(),
        RequiredRuntimeRolesCheck(),
        PipelineReadinessCheck(),
        DiskSpaceCheck(),
    ]


def _compute_summary(results: list[CheckResult]) -> SmokeSummary:
    return SmokeSummary(
        total_checks=len(results),
        pass_count=sum(1 for r in results if r.result == "PASS"),
        warn_count=sum(1 for r in results if r.result == "WARN"),
        fail_count=sum(1 for r in results if r.result == "FAIL"),
        skip_count=sum(1 for r in results if r.result == "SKIP"),
    )


def _compute_overall(results: list[CheckResult]) -> tuple[OverallStatus, int]:
    critical_fail = any(r.severity == "critical" and r.result == "FAIL" for r in results)
    warning_attention = any(
        (r.severity == "warning" and r.result in {"FAIL", "WARN"}) or (r.severity == "info" and r.result == "FAIL") for r in results
    )
    if critical_fail:
        return ("FAIL", 2)
    if warning_attention:
        return ("WARNING", 1)
    return ("OK", 0)


def run_production_smoke(*, profile: str, selected_check_ids: set[str] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    context = SmokeContext(profile=profile, env=Env.load())

    checks = default_checks()
    if selected_check_ids:
        checks = [c for c in checks if c.check_id in selected_check_ids]
        if not checks:
            raise ValueError("No checks matched --checks filter")

    results: list[CheckResult] = []
    for check in checks:
        check_context = SmokeContext(profile=context.profile, env=context.env, prior_results=tuple(results))
        results.append(check.run(check_context))
    summary = _compute_summary(results)
    overall, exit_code = _compute_overall(results)

    return {
        "schema_version": "factory_production_smoke/1",
        "generated_at": datetime.now(UTC).isoformat(),
        "hostname": socket.gethostname(),
        "profile": profile,
        "overall_status": overall,
        "exit_code": exit_code,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "summary": summary.to_dict(),
        "checks": [r.to_dict() for r in results],
    }


def run_checks_with_error_capture(*, profile: str, selected_check_ids: set[str] | None = None) -> dict[str, Any]:
    try:
        return run_production_smoke(profile=profile, selected_check_ids=selected_check_ids)
    except Exception as exc:
        return {
            "schema_version": "factory_production_smoke/1",
            "generated_at": datetime.now(UTC).isoformat(),
            "hostname": socket.gethostname(),
            "profile": profile,
            "overall_status": "FAIL",
            "exit_code": 3,
            "duration_ms": 0,
            "summary": {
                "total_checks": 1,
                "pass_count": 0,
                "warn_count": 0,
                "fail_count": 1,
                "skip_count": 0,
            },
            "checks": [
                {
                    "check_id": "runner_error",
                    "title": "Smoke runner execution",
                    "category": "framework",
                    "severity": "critical",
                    "result": "FAIL",
                    "message": "Smoke runner failed before completing checks",
                    "details": {"error": str(exc)},
                }
            ],
        }
