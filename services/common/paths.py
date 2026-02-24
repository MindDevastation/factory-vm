from __future__ import annotations

from pathlib import Path
from services.common.env import Env


def storage_root(env: Env) -> Path:
    return Path(env.storage_root).resolve()


def workspace_dir(env: Env, job_id: int) -> Path:
    return storage_root(env) / "workspace" / f"job_{job_id}"


def outbox_dir(env: Env, job_id: int) -> Path:
    return storage_root(env) / "outbox" / f"job_{job_id}"


def logs_path(env: Env, job_id: int) -> Path:
    return storage_root(env) / "logs" / f"job_{job_id}.log"


def qa_path(env: Env, job_id: int) -> Path:
    return storage_root(env) / "qa" / f"job_{job_id}.json"


def preview_path(env: Env, job_id: int) -> Path:
    return storage_root(env) / "previews" / f"job_{job_id}_preview60.mp4"


def cancel_flag_path(env: Env, job_id: int) -> Path:
    """Marker file used to request cancellation of an in-flight job."""
    return workspace_dir(env, job_id) / 'YouTubeRoot' / '.cancel'
