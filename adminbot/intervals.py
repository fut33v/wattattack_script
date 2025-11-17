"""Intervals.icu token editing for adminbot."""
from __future__ import annotations

import html
import logging
from typing import Any, Dict, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import CallbackContext

from repositories.intervals_link_repository import upsert_link as intervals_upsert_link
from repositories.client_link_repository import get_link_by_client

LOGGER = logging.getLogger(__name__)


def build_intervals_markup(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text="↩️ Назад",
                    callback_data=f"client_info|{client_id}",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data=f"intervals_cancel|{client_id}",
                ),
            ]
        ]
    )


async def start_intervals_edit(update: Update, context: CallbackContext, client_id: int) -> None:
    """Prompt admin to send Intervals.icu API key (and optional athlete_id)."""
    message = update.effective_message or update.callback_query.message
    context.user_data["intervals_pending"] = {"client_id": client_id, "chat_id": message.chat_id}
    await message.reply_text(
        (
            "🔑 Введите API key Intervals.icu и (опционально) athlete_id через пробел.\n"
            "Пример: <code>your_api_key i51963</code>\n"
            "Если athlete_id не указан, будет использовано значение 0."
        ),
        parse_mode=ParseMode.HTML,
        reply_markup=build_intervals_markup(client_id),
    )


async def handle_intervals_text(update: Update, context: CallbackContext) -> bool:
    """Process text message if we are waiting for Intervals.icu key."""
    message = update.effective_message
    if message is None:
        return False
    pending = context.user_data.get("intervals_pending")
    if not pending or pending.get("chat_id") != message.chat_id:
        return False

    tokens = (message.text or "").strip().split()
    if not tokens:
        await message.reply_text("⚠️ Укажите API ключ (и опционально athlete_id).")
        return True

    api_key = tokens[0]
    athlete_id = tokens[1] if len(tokens) > 1 else "0"

    client_id = pending.get("client_id")
    if not client_id:
        await message.reply_text("⚠️ Не удалось определить клиента.")
        context.user_data.pop("intervals_pending", None)
        return True

    # Map client -> tg_user_id via client_links
    link = get_link_by_client(client_id)
    if not link or not link.get("tg_user_id"):
        await message.reply_text("⚠️ У клиента нет связки с Telegram. Сначала привяжите в клиентском боте.")
        context.user_data.pop("intervals_pending", None)
        return True

    tg_user_id = link["tg_user_id"]
    try:
        intervals_upsert_link(
            tg_user_id=tg_user_id,
            api_key=api_key,
            athlete_id=athlete_id,
        )
        await message.reply_text(
            f"✅ Intervals.icu ключ сохранён.\nTelegram user: {tg_user_id}\nAthlete ID: {html.escape(str(athlete_id))}",
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to save Intervals.icu key for client %s / tg %s", client_id, tg_user_id)
        await message.reply_text(f"❌ Ошибка сохранения ключа: {exc}")
    finally:
        context.user_data.pop("intervals_pending", None)

    return True


async def cancel_intervals(update: Update, context: CallbackContext, client_id: int) -> None:
    context.user_data.pop("intervals_pending", None)
    message = update.effective_message or update.callback_query.message
    await message.reply_text("❌ Настройка Intervals.icu отменена.")
