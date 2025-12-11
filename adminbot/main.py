#!/usr/bin/env python3
"""Telegram bot for managing WattAttack profiles, clients, and inventory."""
from __future__ import annotations

import asyncio
import html
import json
import logging
import os
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
import re
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from repositories.client_repository import (
    count_clients,
    create_client,
    get_client,
    get_clients_stats,
    list_clients,
    search_clients,
    update_client_fields,
)
from repositories.client_link_repository import link_user_to_client
from repositories.link_requests_repository import get_link_request, delete_link_request
from repositories.admin_repository import (
    ensure_admin_table,
    seed_admins_from_env,
    is_admin as db_is_admin,
)
from repositories.bikes_repository import (
    ensure_bikes_table,
    list_bikes,
    search_bikes,
    bikes_count,
    find_bikes_for_height,
    get_bike,
    update_bike_fields,
)
from repositories.trainers_repository import (
    ensure_trainers_table,
    list_trainers,
    search_trainers,
    trainers_count,
    get_trainer,
    update_trainer_fields,
)
from repositories.pedals_repository import (
    ensure_pedals_table,
    list_pedals,
)
from repositories.schedule_repository import (
    book_available_reservation,
    get_reservation,
    get_slot_with_reservations,
    list_available_slots,
    list_future_reservations_for_client,
    update_reservation,
)
from repositories.layout_repository import (
    ensure_layout_table,
    list_layout_details,
    get_assignment_for_bike,
    set_bike_assignment,
    clear_bike_assignment_for_bike,
)
from wattattack_activities import WattAttackClient
from wattattack_profiles import apply_client_profile as apply_wattattack_profile
from wattattack_workouts import (
    build_workout_payload,
    calculate_workout_metrics,
    parse_zwo_workout,
    zwo_to_chart_data,
)
from adminbot import events as events_admin
from adminbot import intervals as intervals_admin
from adminbot import wizard as wizard_admin
from adminbot import menu as menu_admin
from adminbot import clients_view
from adminbot import accounts_view
from adminbot import admins_view
from adminbot import uploads as uploads_admin
from adminbot.accounts import (
    AccountConfig,
    format_account_list as format_account_list_from_registry,
    load_accounts,
    normalize_account_id as normalize_account_id_value,
    resolve_account_identifier as resolve_account_identifier_value,
    resolve_account_tokens as resolve_account_tokens_value,
)

LOGGER = logging.getLogger(__name__)

BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
CLIENT_BOT_TOKEN_ENV = "KRUTILKAVN_BOT_TOKEN"
ACCOUNTS_ENV = "WATTATTACK_ACCOUNTS_FILE"
DEFAULT_ACCOUNTS_PATH = Path("accounts.json")
DEFAULT_TIMEOUT = float(os.environ.get("WATTATTACK_HTTP_TIMEOUT", "30"))
CLIENTS_PAGE_SIZE = int(os.environ.get("CLIENTS_PAGE_SIZE", "6"))
DEFAULT_CLIENT_FTP = int(os.environ.get("WATTATTACK_DEFAULT_FTP", "150"))
CLIENT_BIKE_PICK_PAGE_SIZE = int(os.environ.get("CLIENT_BIKE_PAGE_SIZE", "6"))

PEDAL_OPTIONS: List[Tuple[str, str]] = [
    ("топталки (под кроссовки)", "platform"),
    ("контакты шоссе Look", "road_look"),
    ("контакты шоссе Shimano", "road_shimano"),
    ("контакты MTB Shimano", "mtb_shimano"),
    ("принесу свои", "own"),
]
PEDAL_OPTION_LABEL_BY_CODE: Dict[str, str] = {code: label for label, code in PEDAL_OPTIONS}

LOCAL_TIMEZONE = ZoneInfo(os.environ.get("WATTATTACK_LOCAL_TZ", "Europe/Moscow"))
BOOKING_LOOKAHEAD_DAYS = int(os.environ.get("ADMINBOT_BOOKING_LOOKAHEAD_DAYS", "21"))
BOOKING_LOOKAHEAD = timedelta(days=max(1, BOOKING_LOOKAHEAD_DAYS))
BOOKING_CUTOFF_MINUTES = int(os.environ.get("ADMINBOT_BOOKING_CUTOFF_MINUTES", "90"))
BOOKING_CUTOFF = timedelta(minutes=max(1, BOOKING_CUTOFF_MINUTES))
BOOKING_SLOTS_LIMIT = max(10, int(os.environ.get("ADMINBOT_BOOKING_SLOTS_LIMIT", "80")))
WEEKDAY_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
BOOKING_DAY_LIMIT = max(1, int(os.environ.get("ADMINBOT_BOOKING_DAY_LIMIT", "7")))
BOOKING_REASSIGN_LIMIT = max(3, int(os.environ.get("ADMINBOT_REASSIGN_OPTIONS", "12")))
CLIENT_BOOKINGS_LIMIT = max(5, int(os.environ.get("ADMINBOT_CLIENT_BOOKINGS_LIMIT", "10")))
_CLIENT_BOT = None
_CLIENT_BOT_WARNED = False
START_MESSAGE = (
    "👋 Это главное меню админа Крутилки.\n"
    "Выберите действие в меню ниже или используйте команды напрямую.\n"
    "Чтобы найти клиента, просто напишите его имя и фамилию."
)


def build_menu_return_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("↩️ В меню", callback_data="menu|start")]]
    )


def _get_client_bot() -> Bot | None:
    global _CLIENT_BOT, _CLIENT_BOT_WARNED
    if _CLIENT_BOT is not None:
        return _CLIENT_BOT
    token = os.environ.get(CLIENT_BOT_TOKEN_ENV)
    if not token:
        if not _CLIENT_BOT_WARNED:
            LOGGER.warning("KRUTILKAVN_BOT_TOKEN is not set; users will not be notified about link approvals")
            _CLIENT_BOT_WARNED = True
        return None
    try:
        _CLIENT_BOT = Bot(token=token)
        return _CLIENT_BOT
    except Exception:
        LOGGER.exception("Failed to init client bot for link notifications", exc_info=True)
        return None


def _format_link_client_label(client: Dict[str, Any]) -> str:
    last_name = (client.get("last_name") or "").strip()
    first_name = (client.get("first_name") or "").strip()
    full_name = (client.get("full_name") or "").strip()
    if last_name and first_name:
        display = f"{last_name} {first_name}".strip()
    elif full_name:
        display = full_name
    else:
        display = last_name or first_name or "Без имени"
    return f"{display} (ID {client.get('id')})"


async def _handle_link_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    admin_user = update.effective_user
    if query is None or admin_user is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != "link":
        return
    action = parts[1]
    request_id = parts[2]

    if not db_is_admin(tg_id=admin_user.id, username=admin_user.username):
        await query.answer("Недостаточно прав.", show_alert=True)
        return

    request = get_link_request(request_id)
    if not request:
        await query.answer("Запрос не найден или уже обработан.", show_alert=True)
        try:
            await query.edit_message_text("Запрос не найден или уже обработан.")
        except Exception:
            pass
        return

    client = get_client(request["client_id"])
    if not client:
        await query.answer("Анкета не найдена.", show_alert=True)
        delete_link_request(request_id)
        try:
            await query.edit_message_text("Анкета не найдена. Запрос закрыт.")
        except Exception:
            pass
        return

    client_label = _format_link_client_label(client)
    tg_user_id = request["tg_user_id"]
    user_chat_id = request.get("user_chat_id") or tg_user_id

    if action == "approve":
        try:
            link_user_to_client(
                tg_user_id=tg_user_id,
                client_id=client["id"],
                tg_username=request.get("tg_username"),
                tg_full_name=request.get("tg_full_name"),
            )
        except Exception:
            LOGGER.exception("Failed to link user %s to client %s on approve", tg_user_id, client["id"])
            await query.answer("Не удалось привязать клиента. Попробуйте позже.", show_alert=True)
            return
        admin_text = f"✅ Привязка подтверждена.\nКлиент: {client_label}\nПользователь: id {tg_user_id}"
        user_text = (
            f"Администратор подтвердил привязку к клиенту {client_label}. "
            "Теперь вы можете пользоваться сервисом."
        )
    else:
        admin_text = f"❌ Привязка отклонена.\nКлиент: {client_label}\nПользователь: id {tg_user_id}"
        user_text = (
            f"Администратор отклонил запрос на привязку к клиенту {client_label}. "
            "Связь осталась без изменений."
        )

    delete_link_request(request_id)

    try:
        await query.edit_message_text(admin_text)
    except Exception:
        LOGGER.debug("Failed to edit approval message %s", request_id, exc_info=True)

    client_bot = _get_client_bot()
    if client_bot:
        try:
            await client_bot.send_message(chat_id=user_chat_id, text=user_text)
        except Exception:
            LOGGER.exception("Failed to notify user %s about link decision %s", tg_user_id, request_id)
    else:
        LOGGER.info("Skipping user notification for %s: client bot not configured", tg_user_id)

    await query.answer("Готово.")


async def noop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    if not await clients_view.ensure_admin_callback(query):
        return
    await query.answer("Нет доступных файлов")


async def on_error(update: Optional[Update], context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Unhandled exception during update", exc_info=context.error)


def build_application(token: str) -> Application:
    application = Application.builder().token(token).build()

    application.add_handler(CommandHandler("start", clients_view.start_handler))
    application.add_handler(CommandHandler("events", clients_view.events_handler))
    application.add_handler(CommandHandler("wizard", clients_view.wizard_handler))
    newclient_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("newclient", clients_view.newclient_start),
            CallbackQueryHandler(clients_view.newclient_start, pattern=r"^menu\|newclient$"),
        ],
        states={
            clients_view.NEWCLIENT_FIRST_NAME: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_first_name)
            ],
            clients_view.NEWCLIENT_LAST_NAME: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_last_name)
            ],
            clients_view.NEWCLIENT_WEIGHT: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_weight)
            ],
            clients_view.NEWCLIENT_HEIGHT: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_height)
            ],
            clients_view.NEWCLIENT_GENDER: [
                CallbackQueryHandler(
                    clients_view.newclient_gender_selection, pattern=r"^newclient:gender:(male|female)$"
                ),
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_gender_prompt),
            ],
            clients_view.NEWCLIENT_FTP: [
                CallbackQueryHandler(clients_view.newclient_skip_ftp, pattern=r"^newclient:ftp:skip$"),
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_ftp),
            ],
            clients_view.NEWCLIENT_PEDALS: [
                CallbackQueryHandler(
                    clients_view.newclient_pedals_selection, pattern=r"^newclient:pedals:[^:]+$"
                ),
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_pedals_prompt),
            ],
            clients_view.NEWCLIENT_GOAL: [
                CallbackQueryHandler(clients_view.newclient_skip_goal, pattern=r"^newclient:goal:skip$"),
                MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.newclient_goal),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", clients_view.newclient_cancel),
            CommandHandler("stop", clients_view.newclient_cancel),
        ],
        allow_reentry=True,
    )
    application.add_handler(newclient_conversation)
    application.add_handler(
        MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.wizard_message_handler, block=False)
    )
    accounts_view.register_account_handlers(application)
    application.add_handler(CommandHandler("bikes", clients_view.bikes_handler))
    application.add_handler(CommandHandler("layout", clients_view.layout_handler))
    application.add_handler(CommandHandler("stands", clients_view.stands_handler))
    admins_view.register_admin_handlers(application)
    uploads_admin.register_upload_handlers(application)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), clients_view.text_search_handler))
    application.add_handler(CallbackQueryHandler(_handle_link_request_callback, pattern=r"^link:(approve|reject):[0-9a-fA-F]+$"))
    application.add_handler(CallbackQueryHandler(noop_handler, pattern="^noop$"))
    application.add_handler(CallbackQueryHandler(clients_view.callback_handler))
    application.add_error_handler(on_error)

    return application


def main(argv: Iterable[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    token = os.environ.get(BOT_TOKEN_ENV)
    if not token:
        raise SystemExit(
            "TELEGRAM_BOT_TOKEN не задан. Установите переменную окружения и повторите запуск."
        )

    ensure_admin_table()
    seed_admins_from_env()

    accounts_path = Path(os.environ.get(ACCOUNTS_ENV, DEFAULT_ACCOUNTS_PATH))
    account_registry = load_accounts(accounts_path)
    clients_view.ACCOUNT_REGISTRY = account_registry

    LOGGER.info("Loaded %d WattAttack accounts", len(account_registry))

    admins_view.configure_admins_view(ensure_admin_message=clients_view.ensure_admin_message)
    accounts_view.configure_accounts_view(
        account_registry=account_registry,
        ensure_admin_message=clients_view.ensure_admin_message,
        resolve_account_identifier=clients_view.resolve_account_identifier,
        format_account_list=clients_view.format_account_list,
        default_timeout=DEFAULT_TIMEOUT,
    )
    uploads_admin.configure_uploads(
        ensure_admin_message=clients_view.ensure_admin_message,
        show_account_selection=accounts_view.show_account_selection,
        account_registry=account_registry,
        default_timeout=DEFAULT_TIMEOUT,
    )

    application = build_application(token)
    application.run_polling()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
