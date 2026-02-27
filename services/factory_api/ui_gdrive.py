from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from services.common import db as dbm
from services.common.env import Env
from services.integrations.gdrive import DriveClient, DriveItem
from services.factory_api.oauth_tokens import oauth_token_path


@dataclass
class PreflightResult:
    ok: bool
    field_errors: Dict[str, List[str]]
    resolved: Dict[str, object]


def _ci_eq(a: str, b: str) -> bool:
    return (a or "").strip().lower() == (b or "").strip().lower()


def _is_folder(it: DriveItem) -> bool:
    return it.mime_type == "application/vnd.google-apps.folder"


def resolve_project_folder_ids(drive: DriveClient, gdrive_root_id: str, channel_title: str) -> Dict[str, str]:
    project = [x for x in drive.list_children(gdrive_root_id) if _is_folder(x) and _ci_eq(x.name, channel_title)]
    if len(project) != 1:
        raise ValueError(f"project folder '{channel_title}' matches={len(project)}")
    project_id = project[0].id

    children = [x for x in drive.list_children(project_id) if _is_folder(x)]
    image = [x for x in children if _ci_eq(x.name, "Image")]
    covers = [x for x in children if _ci_eq(x.name, "Covers")]
    audio = [x for x in children if _ci_eq(x.name, "Audio")]

    if len(image) != 1:
        raise ValueError(f"Image folder matches={len(image)}")
    if len(covers) != 1:
        raise ValueError(f"Covers folder matches={len(covers)}")
    if len(audio) != 1:
        raise ValueError(f"Audio folder matches={len(audio)}")

    return {
        "project_id": project_id,
        "image_id": image[0].id,
        "covers_id": covers[0].id,
        "audio_id": audio[0].id,
    }


def _list_recursive_files(drive: DriveClient, folder_id: str) -> List[DriveItem]:
    out: List[DriveItem] = []
    stack = [folder_id]
    while stack:
        cur = stack.pop()
        for it in drive.list_children(cur):
            if _is_folder(it):
                stack.append(it.id)
            else:
                out.append(it)
    return out


def resolve_background(drive: DriveClient, image_id: str, name: str, ext: str) -> DriveItem:
    target = f"{name}.{ext}".lower()
    matches = [x for x in drive.list_children(image_id) if (x.name or "").lower() == target and not _is_folder(x)]
    if len(matches) != 1:
        raise ValueError(f"background '{name}.{ext}' matches={len(matches)}")
    return matches[0]


def resolve_cover(drive: DriveClient, covers_id: str, name: str, ext: str) -> DriveItem:
    target = f"{name}.{ext}".lower()
    matches = [x for x in drive.list_children(covers_id) if (x.name or "").lower() == target and not _is_folder(x)]
    if len(matches) != 1:
        raise ValueError(f"cover '{name}.{ext}' matches={len(matches)}")
    return matches[0]


def resolve_audio_tracks(drive: DriveClient, audio_id: str, ids: List[str]) -> List[DriveItem]:
    files = _list_recursive_files(drive, audio_id)
    by_id: Dict[str, List[DriveItem]] = {}
    for f in files:
        n = (f.name or "")
        ln = n.lower()
        if not ln.endswith(".wav") or "_" not in n:
            continue
        prefix = n.split("_", 1)[0]
        if len(prefix) == 3 and prefix.isdigit():
            by_id.setdefault(prefix, []).append(f)

    out: List[DriveItem] = []
    for aid in ids:
        m = by_id.get(aid, [])
        if len(m) != 1:
            raise ValueError(f"audio id {aid} matches={len(m)}")
        out.append(m[0])
    return out


def run_preflight_for_job(conn, env: Env, job_id: int, drive: Optional[DriveClient] = None) -> PreflightResult:
    draft = dbm.get_ui_job_draft(conn, job_id)
    job = dbm.get_job(conn, job_id)
    errors: Dict[str, List[str]] = {"project": [], "title": [], "audio": [], "background": [], "cover": [], "tags": []}
    resolved: Dict[str, object] = {
        "background_file_id": None,
        "background_filename": None,
        "cover_file_id": None,
        "cover_filename": None,
        "track_file_ids": [],
        "tracks": [],
    }

    def _set_job_error_reason(reason: str) -> None:
        if not job:
            return
        dbm.update_job_state(
            conn,
            job_id,
            state=str(job["state"]),
            stage=str(job["stage"]),
            error_reason=reason,
        )

    if not draft or not job:
        errors["project"].append("job draft not found")
        _set_job_error_reason("job draft not found")
        return PreflightResult(ok=False, field_errors=errors, resolved=resolved)

    if not env.gdrive_root_id:
        errors["project"].append("GDRIVE_ROOT_ID is not configured")
        _set_job_error_reason("GDRIVE_ROOT_ID is not configured")
        return PreflightResult(ok=False, field_errors=errors, resolved=resolved)

    if drive is None:
        token_path = oauth_token_path(base_dir=env.gdrive_tokens_dir, channel_slug=str(job["channel_slug"]))
        drive = DriveClient(
            service_account_json=env.gdrive_sa_json,
            oauth_client_json=env.gdrive_oauth_client_json,
            oauth_token_json=str(token_path),
        )

    try:
        ids = resolve_project_folder_ids(drive, env.gdrive_root_id, str(job["channel_name"]))
    except Exception as e:
        message = str(e)
        errors["project"].append(message)
        _set_job_error_reason(message)
        return PreflightResult(ok=False, field_errors=errors, resolved=resolved)

    try:
        bg = resolve_background(drive, ids["image_id"], str(draft["background_name"]), str(draft["background_ext"]))
        resolved["background_file_id"] = bg.id
        resolved["background_filename"] = bg.name
    except Exception as e:
        errors["background"].append(str(e))

    cover_name = str(draft.get("cover_name") or "").strip()
    cover_ext = str(draft.get("cover_ext") or "").strip()
    if cover_name or cover_ext:
        if not cover_name or not cover_ext:
            errors["cover"].append("cover name/ext must be both set")
        else:
            try:
                cov = resolve_cover(drive, ids["covers_id"], cover_name, cover_ext)
                resolved["cover_file_id"] = cov.id
                resolved["cover_filename"] = cov.name
            except Exception as e:
                errors["cover"].append(str(e))

    raw_ids = [x.strip() for x in str(draft["audio_ids_text"]).split() if x.strip()]
    normalized: List[str] = []
    for x in raw_ids:
        if len(x) == 3 and x.isdigit():
            normalized.append(x)
        elif x.isdigit():
            normalized.append(f"{int(x):03d}")
        else:
            errors["audio"].append(f"invalid audio id '{x}'")

    if not normalized:
        errors["audio"].append("audio ids are required")

    if not errors["audio"]:
        try:
            tracks = resolve_audio_tracks(drive, ids["audio_id"], normalized)
            resolved["track_file_ids"] = [t.id for t in tracks]
            resolved["tracks"] = [{"file_id": t.id, "filename": t.name} for t in tracks]
        except Exception as e:
            errors["audio"].append(str(e))

    ok = not any(errors.values())
    first_error = next((messages for messages in errors.values() if messages), None)
    dbm.update_job_state(conn, job_id, state=str(job["state"]), stage=str(job["stage"]), error_reason=None if ok else "; ".join(first_error or []))
    return PreflightResult(ok=ok, field_errors=errors, resolved=resolved)
