from __future__ import annotations

import json
import urllib.request
from typing import Any, Callable


def build_telegram_operator_message(surface: dict[str, Any]) -> str:
    summaries = dict(surface.get("summaries") or {})
    alerts = list(surface.get("alerts") or [])
    channel_snapshots = list(surface.get("channel_snapshots") or [])
    release_snapshots = list(surface.get("release_video_snapshots") or [])
    recs = list(surface.get("recommendation_summaries") or [])
    plans = list(surface.get("planning_summaries") or [])
    actions = list(surface.get("linked_actions") or [])
    lines = [
        "📊 <b>Analyzer Telegram Operator Surface</b>",
        f"Overview: {summaries.get('overview', 'n/a')}",
        f"Alerts: {len(alerts)} | Recommendations: {len(recs)} | Planning: {len(plans)}",
    ]
    if channel_snapshots:
        lines.append(f"Channel snapshot: {channel_snapshots[0].get('deep_link', '-')}")
    if release_snapshots:
        lines.append(f"Release snapshot: {release_snapshots[0].get('deep_link', '-')}")
    if actions:
        lines.append("Linked actions:")
        for action in actions[:5]:
            lines.append(f"- {action.get('label', 'action')}: {action.get('deep_link') or action.get('path')}")
    return "\n".join(lines)


def _telegram_send_message_http(
    *,
    bot_token: str,
    chat_id: int,
    text: str,
) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({"chat_id": int(chat_id), "text": text, "parse_mode": "HTML"}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:  # nosec B310
        body = resp.read().decode("utf-8")
    return json.loads(body)


def deliver_telegram_operator_surface(
    *,
    surface: dict[str, Any],
    bot_token: str,
    chat_id: int,
    dry_run: bool = True,
    transport: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    message = build_telegram_operator_message(surface)
    if dry_run:
        return {
            "delivery_mode": "DRY_RUN",
            "delivered": False,
            "target_chat_id": int(chat_id),
            "message_preview": message,
        }
    if not str(bot_token or "").strip():
        raise ValueError("telegram bot token required for live delivery")
    if int(chat_id) == 0:
        raise ValueError("telegram chat id required for live delivery")
    sender = transport or _telegram_send_message_http
    delivery = sender(bot_token=bot_token, chat_id=int(chat_id), text=message)
    return {
        "delivery_mode": "LIVE",
        "delivered": bool(delivery.get("ok", False)),
        "target_chat_id": int(chat_id),
        "provider_response": delivery,
        "message_preview": message,
    }
