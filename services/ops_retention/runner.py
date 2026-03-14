from __future__ import annotations

import logging
import os
import re
import sqlite3
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from services.common.env import Env
from services.ops_retention.artifact_policy import ArtifactCategory
from services.ops_retention.config import RetentionWindows


ExecutionMode = str


@dataclass(frozen=True)
class Candidate:
    category: ArtifactCategory
    path: Path
    root_path: Path


@dataclass(frozen=True)
class Decision:
    candidate: Candidate
    should_delete: bool
    reason_code: str
    protected_flag: bool
    size_bytes: int
    age_sec: int


@dataclass(frozen=True)
class RetentionOutcome:
    deleted: int
    skipped: int
    failed: int


PROTECTED_TOKENS = frozenset({"backup", "backups", "snapshot", "snapshots", "quarantine", "config", "configs", "media", "library", "final_output"})
ACTIVE_WORKSPACE_MARKERS = (".active", ".lock", ".pid")
TERMINAL_JOB_STATES = frozenset({"RENDER_FAILED", "FAILED", "QA_FAILED", "UPLOAD_FAILED", "REJECTED", "PUBLISHED", "CANCELLED", "CLEANED"})
_WORKSPACE_JOB_RE = re.compile(r"^job_(\d+)$")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _age_seconds(path: Path, now: datetime) -> int:
    return max(0, int(now.timestamp() - path.stat().st_mtime))


def _size_bytes(path: Path) -> int:
    if path.is_file() or path.is_symlink():
        return int(path.lstat().st_size)
    total = 0
    for child in path.rglob("*"):
        try:
            total += int(child.lstat().st_size)
        except FileNotFoundError:
            continue
    return total


def _inside_scope(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _is_protected_path(path: Path) -> bool:
    lowered_parts = [part.lower() for part in path.parts]
    return any(token in lowered_parts for token in PROTECTED_TOKENS)


def _workspace_has_runtime_reference(path: Path) -> bool:
    return any((path / marker).exists() for marker in ACTIVE_WORKSPACE_MARKERS)


def _workspace_job_id(path: Path) -> int | None:
    match = _WORKSPACE_JOB_RE.match(path.name)
    if not match:
        return None
    return int(match.group(1))


def _workspace_has_active_job_reference(path: Path, env: Env) -> tuple[bool, bool]:
    """Return (is_active, is_uncertain)."""
    job_id = _workspace_job_id(path)
    if job_id is None:
        return False, True

    db_path = Path(env.db_path)
    if not db_path.exists():
        return False, True

    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute("SELECT state FROM jobs WHERE id = ?", (job_id,)).fetchone()
    except sqlite3.Error:
        return False, True

    if row is None:
        return False, False

    state = str(row[0] or "").strip().upper()
    if not state:
        return False, True
    if state in TERMINAL_JOB_STATES:
        return False, False
    return True, False


def _workspace_is_active_or_uncertain(path: Path, env: Env) -> bool:
    if _workspace_has_runtime_reference(path):
        return True
    is_active, is_uncertain = _workspace_has_active_job_reference(path, env)
    return is_active or is_uncertain


def _window_seconds(category: ArtifactCategory, windows: RetentionWindows, path: Path) -> int:
    if category == ArtifactCategory.TEMP_PREVIEWS:
        return windows.preview_hours * 3600
    if category == ArtifactCategory.TEMP_EXPORTS:
        return windows.export_days * 86400
    if category == ArtifactCategory.TRANSIENT_REPORTS:
        return windows.transient_report_days * 86400
    if category == ArtifactCategory.STALE_SCRATCH_DIRS:
        return windows.stale_scratch_hours * 3600
    if category == ArtifactCategory.TERMINAL_WORKSPACES:
        if "failed" in path.name.lower() or (path / ".failed").exists():
            return windows.failed_workspace_days * 86400
        return windows.terminal_workspace_hours * 3600
    return 0


def _delete_reason(category: ArtifactCategory) -> str:
    if category == ArtifactCategory.TEMP_PREVIEWS:
        return "RETENTION_DELETE_TEMP_PREVIEW_EXPIRED"
    if category == ArtifactCategory.TEMP_EXPORTS:
        return "RETENTION_DELETE_EXPORT_EXPIRED"
    if category == ArtifactCategory.TERMINAL_WORKSPACES:
        return "RETENTION_DELETE_TERMINAL_WORKSPACE_EXPIRED"
    if category == ArtifactCategory.TRANSIENT_REPORTS:
        return "RETENTION_DELETE_EXPORT_EXPIRED"
    return "RETENTION_DELETE_EXPORT_EXPIRED"


def build_allowlist(env: Env) -> dict[ArtifactCategory, Path]:
    root = Path(env.storage_root).resolve()
    return {
        ArtifactCategory.TEMP_PREVIEWS: Path(os.environ.get("FACTORY_RETENTION_PREVIEW_DIR", str(root / "previews"))).resolve(),
        ArtifactCategory.TEMP_EXPORTS: Path(os.environ.get("FACTORY_RETENTION_EXPORT_DIR", str(root / "outbox"))).resolve(),
        ArtifactCategory.TRANSIENT_REPORTS: Path(os.environ.get("FACTORY_RETENTION_TRANSIENT_REPORT_DIR", str(root / "qa"))).resolve(),
        ArtifactCategory.TERMINAL_WORKSPACES: Path(os.environ.get("FACTORY_RETENTION_WORKSPACE_DIR", str(root / "workspace"))).resolve(),
        ArtifactCategory.STALE_SCRATCH_DIRS: Path(os.environ.get("FACTORY_RETENTION_SCRATCH_DIR", str(root / "tmp"))).resolve(),
    }


def scan_candidates(allowlist: dict[ArtifactCategory, Path]) -> list[Candidate]:
    out: list[Candidate] = []
    for category, root in allowlist.items():
        if not root.exists():
            continue
        for child in root.iterdir():
            out.append(Candidate(category=category, path=child, root_path=root))
    return out


def decide(candidate: Candidate, windows: RetentionWindows, now: datetime, env: Env) -> Decision:
    size_bytes = _size_bytes(candidate.path)
    age_sec = _age_seconds(candidate.path, now)
    if not _inside_scope(candidate.path, candidate.root_path):
        return Decision(candidate, False, "RETENTION_SKIP_OUTSIDE_SCOPE", False, size_bytes, age_sec)
    if _is_protected_path(candidate.path):
        return Decision(candidate, False, "RETENTION_SKIP_PROTECTED_PATH", True, size_bytes, age_sec)
    if candidate.category == ArtifactCategory.TERMINAL_WORKSPACES and _workspace_is_active_or_uncertain(candidate.path, env):
        return Decision(candidate, False, "RETENTION_SKIP_ACTIVE_WORKSPACE", True, size_bytes, age_sec)

    threshold = _window_seconds(candidate.category, windows, candidate.path)
    if age_sec < threshold:
        return Decision(candidate, False, "RETENTION_SKIP_TOO_RECENT", False, size_bytes, age_sec)
    return Decision(candidate, True, _delete_reason(candidate.category), False, size_bytes, age_sec)


def _emit(
    logger: logging.Logger,
    *,
    event_name: str,
    execution_mode: ExecutionMode,
    decision: Decision | None,
    result: str,
    error_code: str = "",
    sink: Callable[[dict[str, object]], None] | None = None,
) -> None:
    payload: dict[str, object] = {
        "event_name": event_name,
        "timestamp": _now_utc().isoformat(),
        "category": decision.candidate.category.value if decision else "",
        "path": str(decision.candidate.path) if decision else "",
        "reason_code": decision.reason_code if decision else "",
        "size_bytes": decision.size_bytes if decision else 0,
        "age_sec": decision.age_sec if decision else 0,
        "protected_flag": decision.protected_flag if decision else False,
        "execution_mode": execution_mode,
        "result": result,
        "error_code": error_code,
    }
    logger.info("retention.event", extra={"retention_event": payload})
    if sink:
        sink(payload)


def execute_retention(
    *,
    env: Env,
    windows: RetentionWindows,
    execution_mode: ExecutionMode,
    logger: logging.Logger,
    event_sink: Callable[[dict[str, object]], None] | None = None,
) -> RetentionOutcome:
    allowlist = build_allowlist(env)
    candidates = scan_candidates(allowlist)
    now = _now_utc()
    _emit(logger, event_name="retention.scan.start", execution_mode=execution_mode, decision=None, result="started", sink=event_sink)

    deleted = 0
    skipped = 0
    failed = 0
    for candidate in candidates:
        decision = decide(candidate, windows, now, env)
        if not decision.should_delete:
            skipped += 1
            _emit(logger, event_name="retention.skip", execution_mode=execution_mode, decision=decision, result="skipped", sink=event_sink)
            continue

        if execution_mode == "scan":
            skipped += 1
            _emit(logger, event_name="retention.skip", execution_mode=execution_mode, decision=decision, result="dry_run", sink=event_sink)
            continue

        try:
            if decision.candidate.path.is_dir() and not decision.candidate.path.is_symlink():
                shutil.rmtree(decision.candidate.path)
            else:
                decision.candidate.path.unlink(missing_ok=True)
            deleted += 1
            _emit(
                logger,
                event_name="retention.delete.success",
                execution_mode=execution_mode,
                decision=decision,
                result="deleted",
                sink=event_sink,
            )
        except OSError as exc:
            failed += 1
            _emit(
                logger,
                event_name="retention.delete.failure",
                execution_mode=execution_mode,
                decision=decision,
                result="failed",
                error_code=exc.__class__.__name__,
                sink=event_sink,
            )

    _emit(
        logger,
        event_name="retention.scan.complete",
        execution_mode=execution_mode,
        decision=None,
        result=f"complete deleted={deleted} skipped={skipped} failed={failed}",
        sink=event_sink,
    )
    return RetentionOutcome(deleted=deleted, skipped=skipped, failed=failed)
