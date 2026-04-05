from __future__ import annotations

from typing import Any

from .literals import CHAT_BINDING_KINDS, ensure_chat_binding_kind


def normalize_binding_context(*, telegram_user_id: Any, chat_id: Any, thread_id: Any, chat_binding_kind: str) -> dict[str, int | None | str]:
    kind = ensure_chat_binding_kind(chat_binding_kind)
    user = int(telegram_user_id)
    chat = int(chat_id)
    thread = int(thread_id) if thread_id is not None else None

    if kind == "PRIVATE_CHAT":
        if chat >= 0:
            raise ValueError("PRIVATE_CHAT requires negative chat_id")
        if thread is not None:
            raise ValueError("PRIVATE_CHAT does not support thread_id")
    elif kind == "GROUP_CHAT":
        if chat >= 0:
            raise ValueError("GROUP_CHAT requires negative chat_id")
        thread = None
    elif kind == "GROUP_THREAD":
        if chat >= 0:
            raise ValueError("GROUP_THREAD requires negative chat_id")
        if thread is None:
            raise ValueError("GROUP_THREAD requires thread_id")
    else:
        if kind not in CHAT_BINDING_KINDS:
            raise ValueError("unsupported chat binding kind")

    return {
        "telegram_user_id": user,
        "chat_id": chat,
        "thread_id": thread,
        "chat_binding_kind": kind,
    }
