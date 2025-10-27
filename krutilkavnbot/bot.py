"""Entry points for the Krutilka VNB Telegram bot."""
from __future__ import annotations

import logging
from typing import Final

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

LOGGER = logging.getLogger(__name__)

_GREETING_KEY: Final[str] = "krutilkavnbot:greeting"
DEFAULT_GREETING: Final[str] = "Привет!"


async def _send_greeting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Reply with the configured greeting message."""
    message = update.effective_message
    if message is None:
        LOGGER.debug("No message to reply to in update %s", update.update_id)
        return

    greeting: str = context.application.bot_data.get(_GREETING_KEY, DEFAULT_GREETING)
    await message.reply_text(greeting)


def create_application(token: str, greeting: str = DEFAULT_GREETING) -> Application:
    """Create an Application instance that always replies with a greeting."""
    if not token:
        raise ValueError("Telegram bot token must be provided")

    application = Application.builder().token(token).build()
    application.bot_data[_GREETING_KEY] = greeting or DEFAULT_GREETING

    application.add_handler(CommandHandler("start", _send_greeting))
    application.add_handler(MessageHandler(filters.ALL, _send_greeting))

    return application


__all__ = ["create_application", "DEFAULT_GREETING"]

