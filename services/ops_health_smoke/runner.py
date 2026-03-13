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
    if free_percent < fail_percent or free_gib < fail_gib:
        return "FAIL"
    if free_percent < warn_percent or free_gib < warn_gib:
        return "WARN"
    return "PASS"


def _workers_api_url(env: Env) -> str:
    return f"{_local_api_base_url(env)}/v1/workers"


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
        warn_percent = float(os.environ.get("FACTORY_SMOKE_DISK_WARN_PERCENT", "15"))
        warn_gib = float(os.environ.get("FACTORY_SMOKE_DISK_WARN_GIB", "20"))
        fail_percent = float(os.environ.get("FACTORY_SMOKE_DISK_FAIL_PERCENT", "8"))
        fail_gib = float(os.environ.get("FACTORY_SMOKE_DISK_FAIL_GIB", "10"))

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
            status = _evaluate_disk_status(
                free_percent=free_percent,
                free_gib=free_gib,
                warn_percent=warn_percent,
                warn_gib=warn_gib,
                fail_percent=fail_percent,
                fail_gib=fail_gib,
            )
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
                    "warn_percent": warn_percent,
                    "warn_gib": warn_gib,
                    "fail_percent": fail_percent,
                    "fail_gib": fail_gib,
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
        resolved = resolve_required_runtime_roles(
            profile=inputs.profile,
            no_importer_flag=inputs.no_importer_flag,
            with_bot_flag=inputs.with_bot_flag,
        )

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


def default_checks() -> list[SmokeCheck]:
    return [
        RunnerBootstrapCheck(),
        ApiHealthCheck(),
        DbAccessCheck(),
        StoragePathsCheck(),
        FfmpegAvailableCheck(),
        WorkerHeartbeatCheck(),
        RequiredRuntimeRolesCheck(),
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


def _compute_overall(summary: SmokeSummary) -> tuple[OverallStatus, int]:
    if summary.fail_count > 0:
        return ("FAIL", 2)
    if summary.warn_count > 0:
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

    results = [check.run(context) for check in checks]
    summary = _compute_summary(results)
    overall, exit_code = _compute_overall(summary)

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
