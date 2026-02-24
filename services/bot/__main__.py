from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from services.common.profile import load_profile_env
from services.common.env import Env
from services.common.logging_setup import setup_logging, get_logger
from services.bot.handlers import router, start_background_notifier


async def main() -> None:
    load_profile_env()
    env = Env.load()

    if env.telegram_enabled != 1:
        print("TELEGRAM_ENABLED=0, bot is disabled.")
        return
    if not env.tg_bot_token or not env.tg_admin_chat_id:
        print("Telegram is enabled but TG_BOT_TOKEN / TG_ADMIN_CHAT_ID is not set.")
        return

    setup_logging(env, service="bot")
    log = get_logger("bot")
    log.info("starting telegram bot")

    bot = Bot(token=env.tg_bot_token, parse_mode=ParseMode.HTML)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await start_background_notifier(dp, bot, env)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
