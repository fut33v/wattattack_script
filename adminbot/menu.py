"""Start menu helpers for adminbot."""
from __future__ import annotations

import logging
from typing import Awaitable, Callable, Mapping, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from adminbot import events as events_admin
from adminbot import wizard as wizard_admin
from adminbot.accounts import AccountConfig

LOGGER = logging.getLogger(__name__)

BuildViewFunc = Callable[[str], Awaitable[Tuple[str, Optional[InlineKeyboardMarkup]]]]
BuildLayoutFunc = Callable[[], Awaitable[str]]
BuildPedalsFunc = Callable[[], Awaitable[str]]


def build_start_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🧙‍♂️ Волшебник", callback_data="menu|wizard"),
                InlineKeyboardButton("🆕 Создать клиента", callback_data="menu|newclient"),
            ],
            [
                InlineKeyboardButton("👤 Аккаунты", callback_data="menu|accounts"),
                InlineKeyboardButton("📅 События", callback_data="menu|events"),
            ],
            [
                InlineKeyboardButton("🚲 Велосипеды", callback_data="menu|bikes"),
                InlineKeyboardButton("🛠 Станки", callback_data="menu|stands"),
            ],
            [
                InlineKeyboardButton("📐 Расстановка", callback_data="menu|layout"),
                InlineKeyboardButton("🚴 Педали", callback_data="menu|pedals"),
            ],
            [
                InlineKeyboardButton("👑 Админы", callback_data="menu|admins"),
            ],
        ]
    )


def build_menu_return_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("↩️ В меню", callback_data="menu|start")]])


def with_menu_return(markup: Optional[InlineKeyboardMarkup]) -> InlineKeyboardMarkup:
    if markup is None:
        return build_menu_return_markup()
    keyboard = list(markup.inline_keyboard or [])
    keyboard.append([InlineKeyboardButton("↩️ В меню", callback_data="menu|start")])
    return InlineKeyboardMarkup(keyboard)


async def _send_bikes_overview_from_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    build_bikes_view: BuildViewFunc,
) -> None:
    try:
        text, markup = await build_bikes_view("")
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch bikes from start menu")
        if query.message:
            await query.message.reply_text(f"❌ Ошибка получения списка велосипедов: {exc}")
        else:
            await query.edit_message_text(f"❌ Ошибка получения списка велосипедов: {exc}")
        return

    message = query.message
    if message is None:
        return
    if markup is None:
        await message.reply_text(text, reply_markup=build_menu_return_markup())
    else:
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=with_menu_return(markup),
        )


async def _send_stands_overview_from_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    build_stands_view: BuildViewFunc,
) -> None:
    try:
        text, markup = await build_stands_view("")
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch stands from start menu")
        if query.message:
            await query.message.reply_text(f"❌ Ошибка получения списка станков: {exc}")
        else:
            await query.edit_message_text(f"❌ Ошибка получения списка станков: {exc}")
        return

    message = query.message
    if message is None:
        return
    if markup is None:
        await message.reply_text(text, reply_markup=build_menu_return_markup())
    else:
        await message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=with_menu_return(markup),
        )


async def _send_layout_overview_from_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    build_layout_overview: BuildLayoutFunc,
) -> None:
    try:
        text = await build_layout_overview()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch layout from start menu")
        if query.message:
            await query.message.reply_text(f"❌ Ошибка получения расстановки: {exc}")
        else:
            await query.edit_message_text(f"❌ Ошибка получения расстановки: {exc}")
        return

    message = query.message
    if message is None:
        return
    await message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_return_markup(),
    )


async def _send_pedals_overview_from_menu(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    build_pedals_overview: BuildPedalsFunc,
) -> None:
    try:
        text = await build_pedals_overview()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch pedals from start menu")
        if query.message:
            await query.message.reply_text(f"❌ Ошибка получения списка педалей: {exc}")
        else:
            await query.edit_message_text(f"❌ Ошибка получения списка педалей: {exc}")
        return

    message = query.message
    if message is None:
        return
    await message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=build_menu_return_markup(),
    )


async def handle_menu_action(
    menu_action: str,
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    account_registry: Mapping[str, AccountConfig],
    timezone,
    *,
    show_account_selection,
    build_bikes_view: BuildViewFunc,
    build_stands_view: BuildViewFunc,
    build_layout_overview: BuildLayoutFunc,
    build_pedals_overview: BuildPedalsFunc,
    show_admin_menu,
    start_text: str,
) -> bool:
    if menu_action == "wizard":
        await wizard_admin.start(update, context, account_registry, timezone)
        return True
    if menu_action == "accounts":
        query = update.callback_query
        if query and query.message:
            await show_account_selection(message=query.message, kind="account")
        else:
            await show_account_selection(query=query, kind="account")
        return True
    if menu_action == "bikes":
        query = update.callback_query
        if query:
            await _send_bikes_overview_from_menu(query, context, build_bikes_view)
        return True
    if menu_action == "stands":
        query = update.callback_query
        if query:
            await _send_stands_overview_from_menu(query, context, build_stands_view)
        return True
    if menu_action == "layout":
        query = update.callback_query
        if query:
            await _send_layout_overview_from_menu(query, context, build_layout_overview)
        return True
    if menu_action == "pedals":
        query = update.callback_query
        if query:
            await _send_pedals_overview_from_menu(query, context, build_pedals_overview)
        return True
    if menu_action == "events":
        await events_admin.start_events_flow(update, context, account_registry)
        return True
    if menu_action == "admins":
        query = update.callback_query
        if query:
            await show_admin_menu(query=query)
        return True
    if menu_action == "start":
        query = update.callback_query
        if query and query.message:
            await query.message.edit_text(start_text, reply_markup=build_start_menu_keyboard())
        return True
    return False
