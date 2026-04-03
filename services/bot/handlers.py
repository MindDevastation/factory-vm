from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from aiogram import Bot, Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import FSInputFile, ForceReply

from services.common.env import Env
from services.common import db as dbm
from services.common.paths import preview_path, qa_path, logs_path, outbox_dir
from services.common.logging_setup import get_logger
from services.factory_api.approval_actions import approve_job, reject_job, mark_job_published
from services.factory_api.publish_job_actions import execute_publish_job_action
from services.factory_api.publish_bulk_actions import create_bulk_preview_session, execute_bulk_preview_session, PublishBulkActionError
from services.bot.telegram_publish_notifications import send_critical_publish_notifications
from services.telegram_inbox.ops_controls import execute_single_ops_action
from services.telegram_inbox.read_views import build_and_persist_read_view
from services.telegram_operator import TelegramOperatorRegistry
from services.telegram_publish.actions import route_publish_action_via_gateway


router = Router()
log = get_logger("bot")


def _telegram_reply_payload(*, action: str, target: dict[str, Any], result: str, request_id: str, error_code: str | None = None) -> str:
    payload: dict[str, Any] = {
        "action": action,
        "target": target,
        "result": result,
        "request_id": request_id,
    }
    if error_code:
        payload["error_code"] = error_code
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _kb(job_id: int) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Approve", callback_data=f"approve:{job_id}")
    kb.button(text="❌ Reject", callback_data=f"reject:{job_id}")
    kb.button(text="📄 QA", callback_data=f"qa:{job_id}")
    kb.button(text="🧾 Logs", callback_data=f"logs:{job_id}")
    kb.button(text="✅ Mark Published", callback_data=f"published:{job_id}")
    kb.button(text="🔁 Retry Publish", callback_data=f"pubact:retry:{job_id}")
    kb.button(text="🧰 Move to Manual", callback_data=f"pubact:move_to_manual:{job_id}")
    kb.adjust(2, 2, 2)
    return kb




def run_telegram_bulk_action(
    *,
    env: Env,
    action: str,
    selected_job_ids: list[int],
    actor: str,
    scheduled_at: str | None = None,
) -> dict[str, Any]:
    conn = dbm.connect(env)
    try:
        preview = create_bulk_preview_session(
            conn,
            action=action,
            selected_job_ids=selected_job_ids,
            scheduled_at=scheduled_at,
            created_by=actor,
            ttl_seconds=1800,
        )
        execute = execute_bulk_preview_session(
            conn,
            preview_session_id=str(preview["preview_session_id"]),
            selected_job_ids=None,
            selection_fingerprint=str(preview["selection_fingerprint"]),
            executed_by=actor,
        )
        return {"ok": True, "preview": preview, "execute": execute}
    finally:
        conn.close()

def _ensure_admin(msg_or_cb, env: Env) -> bool:
    # only allow actions in admin chat
    chat_id = msg_or_cb.chat.id if hasattr(msg_or_cb, "chat") else msg_or_cb.message.chat.id
    return int(chat_id) == int(env.tg_admin_chat_id)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Factory bot online.")


@router.message(Command("whoami"))
async def cmd_whoami(message: Message):
    env = Env.load()
    if not _ensure_admin(message, env):
        await message.answer("Not allowed")
        return
    conn = dbm.connect(env)
    try:
        registry = TelegramOperatorRegistry(conn)
        identity = registry.get_identity(telegram_user_id=int(message.from_user.id))
        if not identity:
            await message.answer("E6A identity not enrolled.")
            return
        await message.answer(
            f"E6A identity: operator={identity.get('product_operator_id')} status={identity.get('telegram_access_status')} class={identity.get('max_permission_class')}"
        )
    finally:
        conn.close()


@router.message(Command("overview"))
async def cmd_overview(message: Message):
    env = Env.load()
    if not _ensure_admin(message, env):
        await message.answer("Not allowed")
        return
    conn = dbm.connect(env)
    try:
        rows = [
            {"job_id": int(r["id"]), "publish_state": str(r.get("publish_state") or "")}
            for r in conn.execute("SELECT id, publish_state FROM jobs ORDER BY id DESC LIMIT 100").fetchall()
        ]
        payload = build_and_persist_read_view(
            conn,
            product_operator_id=f"tg:{int(message.from_user.id)}",
            telegram_user_id=int(message.from_user.id),
            view_name="factory_overview",
            rows=rows,
            generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
    finally:
        conn.close()
    await message.answer(
        f"Overview: {payload['summary']}\nQueue groups: {json.dumps(payload.get('queue_groups', {}), ensure_ascii=False)}"
    )


@router.callback_query(F.data.startswith("qa:"))
async def cb_qa(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    job_id = int(call.data.split(":", 1)[1])
    p = qa_path(env, job_id)
    if not p.exists():
        await call.message.answer("QA: нет")
    else:
        txt = p.read_text(encoding="utf-8", errors="ignore")
        if len(txt) > 3800:
            txt = txt[:3800] + "\n..."
        await call.message.answer(f"<pre>{txt}</pre>")
    await call.answer()


@router.callback_query(F.data.startswith("logs:"))
async def cb_logs(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    job_id = int(call.data.split(":", 1)[1])
    p = logs_path(env, job_id)
    if not p.exists():
        await call.message.answer("Logs: нет")
    else:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()[-120:]
        txt = "\n".join(lines)
        if len(txt) > 3800:
            txt = txt[-3800:]
        await call.message.answer(f"<pre>{txt}</pre>")
    await call.answer()


@router.callback_query(F.data.startswith("approve:"))
async def cb_approve(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    job_id = int(call.data.split(":", 1)[1])
    conn = dbm.connect(env)
    try:
        approve_job(conn, job_id=job_id, comment="approved")
    finally:
        conn.close()
    await call.message.answer(f"✅ Approved job {job_id}. Теперь опубликуй в YouTube Studio и нажми Mark Published.")
    await call.answer("Approved")


@router.callback_query(F.data.startswith("reject:"))
async def cb_reject(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    job_id = int(call.data.split(":", 1)[1])
    conn = dbm.connect(env)
    try:
        dbm.set_pending_reply(conn, call.from_user.id, job_id, "reject")
    finally:
        conn.close()
    await call.message.answer(
        f"❌ Напиши причину отклонения одним сообщением (в ответ на это). Job {job_id}.",
        reply_markup=ForceReply(selective=True),
    )
    await call.answer("Send reason")


@router.message(F.reply_to_message)
async def on_reply(message: Message):
    env = Env.load()
    if int(message.chat.id) != int(env.tg_admin_chat_id):
        return
    conn = dbm.connect(env)
    try:
        pending = dbm.pop_pending_reply(conn, message.from_user.id)
        if not pending:
            return
        if pending["kind"] == "reject":
            job_id = int(pending["job_id"])
            reason = (message.text or "").strip()
            if not reason:
                await message.answer("Причина пустая. Нажми Reject ещё раз.")
                return
            reject_job(conn, job_id=job_id, comment=reason)
            await message.answer(f"Job {job_id} отклонён. Причина записана.")
    finally:
        conn.close()


@router.callback_query(F.data.startswith("published:"))
async def cb_published(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    job_id = int(call.data.split(":", 1)[1])
    conn = dbm.connect(env)
    try:
        result = mark_job_published(conn, job_id=job_id)
        history_id = result.get("history_id")
        if history_id is not None:
            log.info("playlist_builder.history.committed_written", extra={"job_id": job_id, "history_id": history_id})
    finally:
        conn.close()
    await call.message.answer(f"✅ Mark Published: job {job_id}. MP4 удалится через 48 часов.")
    await call.answer("Published marked")




@router.callback_query(F.data.startswith("pubact:"))
async def cb_publish_action(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    _, action, raw_job_id = str(call.data or "").split(":", 2)
    job_id = int(raw_job_id)
    request_id = f"tg-{action}-{job_id}-{int(dbm.now_ts())}"
    conn = dbm.connect(env)
    try:
        result = execute_publish_job_action(
            conn,
            job_id=job_id,
            action_type=action,
            actor=f"telegram:{call.from_user.id}",
            request_id=request_id,
            reason=f"telegram callback {action}",
            extra_payload={},
        )
    finally:
        conn.close()
    if hasattr(result, "status_code"):
        body = json.loads(result.body.decode("utf-8"))
        err = body.get("error") or {}
        await call.message.answer(
            _telegram_reply_payload(
                action=action,
                target={"job_id": job_id},
                result="failed",
                request_id=str(err.get("request_id") or request_id),
                error_code=str(err.get("code") or "E3_ACTION_NOT_ALLOWED"),
            )
        )
    else:
        await call.message.answer(
            _telegram_reply_payload(
                action=action,
                target={"job_id": job_id},
                result="ok",
                request_id=request_id,
                error_code=None,
            )
        )
    await call.answer(action)


@router.callback_query(F.data.startswith("e6a:pub:"))
async def cb_e6a_publish_action(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    _, _, action, raw_job_id, expected_state = str(call.data or "").split(":", 4)
    request_id = f"tg-e6a-{action}-{raw_job_id}-{int(dbm.now_ts())}"
    correlation_id = f"corr-{request_id}"
    conn = dbm.connect(env)
    try:
        result = route_publish_action_via_gateway(
            conn,
            telegram_user_id=int(call.from_user.id),
            chat_id=int(call.message.chat.id),
            thread_id=None,
            telegram_action=action,
            job_id=int(raw_job_id),
            expected_publish_state=(expected_state if expected_state != "_" else None),
            confirm=True,
            reason=f"telegram e6a callback {action}",
            request_id=request_id,
            correlation_id=correlation_id,
        )
    finally:
        conn.close()
    await call.message.answer(json.dumps(result, ensure_ascii=False, sort_keys=True))
    await call.answer(action)


@router.callback_query(F.data.startswith("e6a:ops:"))
async def cb_e6a_ops_action(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    _, _, action, raw_job_id = str(call.data or "").split(":", 3)
    request_id = f"tg-e6a-ops-{action}-{raw_job_id}-{int(dbm.now_ts())}"
    conn = dbm.connect(env)
    try:
        result = execute_single_ops_action(
            conn,
            job_id=int(raw_job_id),
            action=action,
            actor=f"telegram:{int(call.from_user.id)}",
            confirm=True,
            reason=f"telegram e6a ops {action}",
            request_id=request_id,
        )
    finally:
        conn.close()
    await call.message.answer(json.dumps(result, ensure_ascii=False, sort_keys=True))
    await call.answer(action)



@router.callback_query(F.data.startswith("pubbulk:"))
async def cb_publish_bulk_action(call: CallbackQuery):
    env = Env.load()
    if not _ensure_admin(call, env):
        await call.answer("Not allowed", show_alert=True)
        return
    parts = str(call.data or "").split(":", 3)
    action = parts[1] if len(parts) > 1 else ""
    ids_raw = parts[2] if len(parts) > 2 else ""
    scheduled_at = parts[3] if len(parts) > 3 else None
    selected_job_ids = [int(item) for item in ids_raw.split(",") if item.strip()]

    request_id = f"tg-bulk-{action}-{int(dbm.now_ts())}"
    try:
        result = run_telegram_bulk_action(
            env=env,
            action=action,
            selected_job_ids=selected_job_ids,
            actor=f"telegram:{call.from_user.id}",
            scheduled_at=scheduled_at,
        )
    except PublishBulkActionError as exc:
        await call.message.answer(
            _telegram_reply_payload(
                action=action,
                target={"job_ids": selected_job_ids, "count": len(selected_job_ids)},
                result="failed",
                request_id=request_id,
                error_code=exc.code,
            )
        )
        await call.answer("bulk failed")
        return

    summary = result["execute"]["summary"]
    await call.message.answer(
        _telegram_reply_payload(
            action=action,
            target={"job_ids": selected_job_ids, "count": len(selected_job_ids)},
            result=f"ok executed={summary['executed_count']} succeeded={summary['succeeded_count']} skipped={summary['skipped_count']}",
            request_id=request_id,
            error_code=None,
        )
    )
    await call.answer(f"bulk {action}")

async def start_background_notifier(dp: Dispatcher, bot: Bot, env: Env) -> None:
    async def loop():
        while True:
            try:
                await _notify_once(bot, env)
                await send_critical_publish_notifications(bot=bot, env=env)
            except Exception:
                pass
            await asyncio.sleep(5)

    asyncio.create_task(loop())


async def _notify_once(bot: Bot, env: Env) -> None:
    conn = dbm.connect(env)
    try:
        rows = conn.execute(
            """
            SELECT j.id, j.approval_notified_at, r.title, c.display_name, y.url, y.studio_url
            FROM jobs j
            JOIN releases r ON r.id = j.release_id
            JOIN channels c ON c.id = r.channel_id
            JOIN youtube_uploads y ON y.job_id = j.id
            WHERE j.state = 'WAIT_APPROVAL' AND (j.approval_notified_at IS NULL OR j.approval_notified_at = 0)
            ORDER BY j.created_at ASC
            LIMIT 3
            """
        ).fetchall()

        for r in rows:
            job_id = int(r["id"])
            # attach preview file
            pv = preview_path(env, job_id)
            caption = (
                f"<b>{r['display_name']}</b>\n"
                f"<b>{r['title']}</b>\n\n"
                f"🔗 {r['url']}\n"
                f"🛠 {r['studio_url']}\n"
                f"\nНажми Approve/Reject. После публикации в Studio — Mark Published."
            )
            kb = _kb(job_id).as_markup()
            if pv.exists():
                msg = await bot.send_video(chat_id=env.tg_admin_chat_id, video=FSInputFile(str(pv)), caption=caption, reply_markup=kb)
            else:
                msg = await bot.send_message(chat_id=env.tg_admin_chat_id, text=caption, reply_markup=kb)

            dbm.upsert_tg_message(conn, job_id, int(env.tg_admin_chat_id), int(msg.message_id))
            dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL", approval_notified_at=dbm.now_ts())
    finally:
        conn.close()
