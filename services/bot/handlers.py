from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from aiogram import Bot, Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import FSInputFile, ForceReply

from services.common.env import Env
from services.common import db as dbm
from services.common.paths import preview_path, qa_path, logs_path, outbox_dir


router = Router()


def _kb(job_id: int) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Approve", callback_data=f"approve:{job_id}")
    kb.button(text="❌ Reject", callback_data=f"reject:{job_id}")
    kb.button(text="📄 QA", callback_data=f"qa:{job_id}")
    kb.button(text="🧾 Logs", callback_data=f"logs:{job_id}")
    kb.button(text="✅ Mark Published", callback_data=f"published:{job_id}")
    kb.adjust(2, 2, 1)
    return kb


def _ensure_admin(msg_or_cb, env: Env) -> bool:
    # only allow actions in admin chat
    chat_id = msg_or_cb.chat.id if hasattr(msg_or_cb, "chat") else msg_or_cb.message.chat.id
    return int(chat_id) == int(env.tg_admin_chat_id)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer("Factory bot online.")


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
        dbm.set_approval(conn, job_id, "APPROVE", "approved")
        dbm.update_job_state(conn, job_id, state="APPROVED", stage="APPROVAL")
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
            dbm.set_approval(conn, job_id, "REJECT", reason)
            dbm.update_job_state(conn, job_id, state="REJECTED", stage="APPROVAL", error_reason=reason)
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
        ts = dbm.now_ts()
        delete_at = ts + 48 * 3600
        dbm.update_job_state(conn, job_id, state="PUBLISHED", stage="APPROVAL", published_at=ts, delete_mp4_at=delete_at)
    finally:
        conn.close()
    await call.message.answer(f"✅ Mark Published: job {job_id}. MP4 удалится через 48 часов.")
    await call.answer("Published marked")


async def start_background_notifier(dp: Dispatcher, bot: Bot, env: Env) -> None:
    async def loop():
        while True:
            try:
                await _notify_once(bot, env)
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
