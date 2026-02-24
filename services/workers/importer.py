from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import Any, Dict

from services.common.env import Env
from services.common import db as dbm
from services.common.config import load_channels
from services.common.logging_setup import get_logger
from services.integrations.gdrive import DriveClient
from services.integrations.local_fs import list_release_folders, load_meta, resolve_asset_path


log = get_logger("importer")


def importer_cycle(*, env: Env, worker_id: str) -> None:
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        dbm.touch_worker(
            conn,
            worker_id=worker_id,
            role="importer",
            pid=os.getpid(),
            hostname=socket.gethostname(),
            details={"origin_backend": env.origin_backend},
        )

        channels_cfg = load_channels("configs/channels.yaml")

        if env.origin_backend == "local":
            _import_from_local(env, conn, channels_cfg)
            return

        # gdrive mode
        if not env.gdrive_root_id:
            return

        drive = DriveClient(
            service_account_json=env.gdrive_sa_json,
            oauth_client_json=env.gdrive_oauth_client_json,
            oauth_token_json=env.gdrive_oauth_token_json,
        )

        channels_root = drive.find_child_folder(env.gdrive_root_id, "channels")
        if not channels_root:
            log.warning("Drive: missing /channels under root")
            return

        for c in channels_cfg:
            ch = dbm.get_channel_by_slug(conn, c.slug)
            if not ch:
                continue

            ch_folder = drive.find_child_folder(channels_root.id, c.slug)
            if not ch_folder:
                continue

            incoming = drive.find_child_folder(ch_folder.id, "incoming")
            if not incoming:
                continue

            for it in drive.list_children(incoming.id):
                if it.mime_type != "application/vnd.google-apps.folder":
                    continue
                release_folder_id = it.id

                meta = drive.find_child_file(release_folder_id, "meta.json")
                if not meta:
                    continue

                existing = conn.execute(
                    "SELECT id FROM releases WHERE origin_meta_file_id = ?",
                    (meta.id,),
                ).fetchone()

                if existing:
                    # allow promotion of WAITING_INPUTS
                    release_id = int(existing["id"])
                    wjob = conn.execute(
                        "SELECT id, state FROM jobs WHERE release_id = ? ORDER BY id DESC LIMIT 1",
                        (release_id,),
                    ).fetchone()
                    if wjob and wjob["state"] == "WAITING_INPUTS":
                        _gdrive_try_promote_waiting(conn, drive, ch, c, int(wjob["id"]), release_folder_id, meta.id)
                    continue

                # new release
                try:
                    meta_obj = json.loads(drive.download_text(meta.id))
                except Exception as e:
                    log.warning("meta.json parse failed: folder=%s err=%s", release_folder_id, e)
                    continue

                title = str(meta_obj.get("title", "")).strip()
                description = str(meta_obj.get("description", "")).strip()
                tags = meta_obj.get("tags") or []
                if not title:
                    continue

                ts = dbm.now_ts()
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at,
                                        origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(ch["id"]),
                        title,
                        description,
                        dbm.json_dumps(tags),
                        meta_obj.get("planned_at"),
                        release_folder_id,
                        meta.id,
                        ts,
                    ),
                )
                release_id = int(cur.lastrowid)

                job_type = "RENDER_TITANWAVE" if c.kind == "TITANWAVE" else "RENDER_LONG"

                audio_dir = drive.find_child_folder(release_folder_id, "audio")
                images_dir = drive.find_child_folder(release_folder_id, "images")
                if not audio_dir or not images_dir:
                    conn.execute(
                        """
                        INSERT INTO jobs(release_id, job_type, state, stage, priority, created_at, updated_at)
                        VALUES(?, ?, 'WAITING_INPUTS', 'FETCH', ?, ?, ?)
                        """,
                        (release_id, job_type, int(100 * c.weight), ts, ts),
                    )
                    continue

                cur2 = conn.execute(
                    """
                    INSERT INTO jobs(release_id, job_type, state, stage, priority, created_at, updated_at)
                    VALUES(?, ?, 'READY_FOR_RENDER', 'FETCH', ?, ?, ?)
                    """,
                    (release_id, job_type, int(100 * c.weight), ts, ts),
                )
                job_id = int(cur2.lastrowid)

                _gdrive_attach_assets(conn, drive, ch, job_id, release_folder_id, meta_obj)

    finally:
        conn.close()


def _gdrive_try_promote_waiting(conn, drive: DriveClient, ch: Dict[str, Any], c_cfg, job_id: int, release_folder_id: str, meta_id: str) -> None:
    audio_dir = drive.find_child_folder(release_folder_id, "audio")
    images_dir = drive.find_child_folder(release_folder_id, "images")
    if not audio_dir or not images_dir:
        return

    n = conn.execute("SELECT COUNT(1) AS n FROM job_inputs WHERE job_id = ?", (job_id,)).fetchone()
    if n and int(n["n"]) == 0:
        try:
            meta_obj = json.loads(drive.download_text(meta_id))
        except Exception:
            return
        _gdrive_attach_assets(conn, drive, ch, job_id, release_folder_id, meta_obj)

    conn.execute("UPDATE jobs SET state='READY_FOR_RENDER', stage='FETCH', updated_at=? WHERE id=? AND state!='CANCELLED'", (dbm.now_ts(), job_id))


def _gdrive_attach_assets(conn, drive: DriveClient, ch: Dict[str, Any], job_id: int, release_folder_id: str, meta_obj: Dict[str, Any]) -> None:
    audio_dir = drive.find_child_folder(release_folder_id, "audio")
    images_dir = drive.find_child_folder(release_folder_id, "images")
    if not audio_dir or not images_dir:
        return

    audio_list = meta_obj.get("assets", {}).get("audio") or []
    cover_path = meta_obj.get("assets", {}).get("cover") or ""

    order = 0
    for ap in audio_list:
        name = str(ap).split("/")[-1]
        f = drive.find_child_file(audio_dir.id, name)
        if not f:
            continue
        asset_id = dbm.create_asset(
            conn,
            channel_id=int(ch["id"]),
            kind="AUDIO",
            origin="GDRIVE",
            origin_id=f.id,
            name=f.name,
            path=f"gdrive:{f.id}",
        )
        dbm.link_job_input(conn, job_id, asset_id, "TRACK", order)
        order += 1

    cover_name = str(cover_path).split("/")[-1]
    cf = drive.find_child_file(images_dir.id, cover_name) if cover_name else None
    if cf:
        cover_asset_id = dbm.create_asset(
            conn,
            channel_id=int(ch["id"]),
            kind="IMAGE",
            origin="GDRIVE",
            origin_id=cf.id,
            name=cf.name,
            path=f"gdrive:{cf.id}",
        )
        dbm.link_job_input(conn, job_id, cover_asset_id, "COVER", 0)


def _import_from_local(env: Env, conn, channels_cfg) -> None:
    origin_root = Path(env.origin_local_root).resolve()
    if not origin_root.exists():
        log.warning("Local origin root does not exist: %s", origin_root)
        return

    for c in channels_cfg:
        ch = dbm.get_channel_by_slug(conn, c.slug)
        if not ch:
            continue

        for folder in list_release_folders(origin_root, c.slug):
            rel = load_meta(folder)
            if not rel:
                continue

            meta_id = str(rel.meta_path)
            existing = conn.execute("SELECT id FROM releases WHERE origin_meta_file_id = ?", (meta_id,)).fetchone()

            if existing:
                release_id = int(existing["id"])
                wjob = conn.execute("SELECT id, state FROM jobs WHERE release_id = ? ORDER BY id DESC LIMIT 1", (release_id,)).fetchone()
                if wjob and wjob["state"] == "WAITING_INPUTS":
                    _local_try_promote_waiting(conn, ch, c, int(wjob["id"]), rel)
                continue

            title = str(rel.meta.get("title", "")).strip()
            description = str(rel.meta.get("description", "")).strip()
            tags = rel.meta.get("tags") or []
            if not title:
                continue

            ts = dbm.now_ts()
            cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at,
                                    origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(ch["id"]), title, description, dbm.json_dumps(tags), rel.meta.get("planned_at"), str(rel.folder), meta_id, ts),
            )
            release_id = int(cur.lastrowid)

            job_type = "RENDER_TITANWAVE" if c.kind == "TITANWAVE" else "RENDER_LONG"

            if not (rel.folder/"audio").exists() or not (rel.folder/"images").exists():
                conn.execute(
                    """
                    INSERT INTO jobs(release_id, job_type, state, stage, priority, created_at, updated_at)
                    VALUES(?, ?, 'WAITING_INPUTS', 'FETCH', ?, ?, ?)
                    """,
                    (release_id, job_type, int(100 * c.weight), ts, ts),
                )
                continue

            cur2 = conn.execute(
                """
                INSERT INTO jobs(release_id, job_type, state, stage, priority, created_at, updated_at)
                VALUES(?, ?, 'READY_FOR_RENDER', 'FETCH', ?, ?, ?)
                """,
                (release_id, job_type, int(100 * c.weight), ts, ts),
            )
            job_id = int(cur2.lastrowid)

            _local_attach_assets(conn, ch, job_id, rel)


def _local_try_promote_waiting(conn, ch: Dict[str, Any], c_cfg, job_id: int, rel) -> None:
    if not (rel.folder/"audio").exists() or not (rel.folder/"images").exists():
        return
    n = conn.execute("SELECT COUNT(1) AS n FROM job_inputs WHERE job_id = ?", (job_id,)).fetchone()
    if n and int(n["n"]) == 0:
        _local_attach_assets(conn, ch, job_id, rel)
    conn.execute("UPDATE jobs SET state='READY_FOR_RENDER', stage='FETCH', updated_at=? WHERE id=? AND state!='CANCELLED'", (dbm.now_ts(), job_id))


def _local_attach_assets(conn, ch: Dict[str, Any], job_id: int, rel) -> None:
    audio_list = rel.meta.get("assets", {}).get("audio") or []
    cover_path = rel.meta.get("assets", {}).get("cover") or ""

    order = 0
    for ap in audio_list:
        apath = resolve_asset_path(rel.folder, str(ap))
        if not apath.exists():
            continue
        asset_id = dbm.create_asset(
            conn,
            channel_id=int(ch["id"]),
            kind="AUDIO",
            origin="LOCAL",
            origin_id=str(apath),
            name=apath.name,
            path=str(apath),
        )
        dbm.link_job_input(conn, job_id, asset_id, "TRACK", order)
        order += 1

    cpath = resolve_asset_path(rel.folder, str(cover_path)) if cover_path else None
    if cpath and cpath.exists():
        cover_asset_id = dbm.create_asset(
            conn,
            channel_id=int(ch["id"]),
            kind="IMAGE",
            origin="LOCAL",
            origin_id=str(cpath),
            name=cpath.name,
            path=str(cpath),
        )
        dbm.link_job_input(conn, job_id, cover_asset_id, "COVER", 0)
