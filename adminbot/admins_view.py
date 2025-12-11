"""Handlers and helpers for managing admins."""
from __future__ import annotations

import asyncio
import logging
from typing import List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.ext import Application, CommandHandler, ContextTypes

from repositories.admin_repository import (
    add_admin as db_add_admin,
    list_admins as db_list_admins,
    remove_admin as db_remove_admin,
)

LOGGER = logging.getLogger(__name__)

PENDING_ADDADMIN_KEY = "pending_addadmin"
PENDING_REMOVEADMIN_KEY = "pending_removeadmin"

_ensure_admin_message = None


def configure_admins_view(*, ensure_admin_message) -> None:
    """Configure dependencies for admin handlers."""
    global _ensure_admin_message
    _ensure_admin_message = ensure_admin_message


def register_admin_handlers(application: Application) -> None:
    application.add_handler(CommandHandler("admins", admins_handler))
    application.add_handler(CommandHandler("addadmin", addadmin_handler))
    application.add_handler(CommandHandler("removeadmin", removeadmin_handler))


def _parse_admin_identifier(value: str) -> Tuple[Optional[int], Optional[str]]:
    value = value.strip()
    if not value:
        return None, None
    if value.startswith("@"):
        value = value[1:]
    if value.isdigit():
        return int(value), None
    return None, value


def _format_admin_record(record) -> str:
    display_name = record.get("display_name")
    username = record.get("username")
    tg_id = record.get("tg_id")

    parts = []
    if display_name:
        parts.append(str(display_name))
    if username:
        handle = username if username.startswith("@") else f"@{username}"
        parts.append(handle)
    if tg_id:
        parts.append(f"id={tg_id}")
    return " ".join(parts) if parts else f"id={tg_id}" if tg_id else str(record.get("id"))


def _format_admin_list(admins) -> str:
    if not admins:
        return "Администраторы не настроены."
    return "\n".join(_format_admin_record(admin) for admin in admins)


def _admins_menu_keyboard(include_back: bool = True) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("👥 Список админов", callback_data="admins|list")],
        [InlineKeyboardButton("➕ Добавить", callback_data="admins|add")],
        [InlineKeyboardButton("🗑️ Удалить", callback_data="admins|remove")],
    ]
    if include_back:
        buttons.append([InlineKeyboardButton("↩️ В меню", callback_data="menu|start")])
    return InlineKeyboardMarkup(buttons)


async def _send_admins_list_via_message(message: Message) -> None:
    try:
        admins = await asyncio.to_thread(db_list_admins)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load admins")
        await message.reply_text(f"❌ Ошибка получения списка администраторов: {exc}")
        return

    text = "👥 Администраторы:\n" + _format_admin_list(admins) if admins else "⚠️ Администраторы не настроены."
    await message.reply_text(text)


async def _send_admins_list_via_callback(query) -> None:
    try:
        admins = await asyncio.to_thread(db_list_admins)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load admins")
        await query.edit_message_text(f"❌ Ошибка получения списка администраторов: {exc}")
        return

    text = "👥 Администраторы:\n" + _format_admin_list(admins) if admins else "⚠️ Администраторы не настроены."
    await query.edit_message_text(text, reply_markup=_admins_menu_keyboard())


def _build_remove_keyboard(admins: List[dict]) -> InlineKeyboardMarkup:
    buttons: List[List[InlineKeyboardButton]] = []
    for admin in admins:
        label = _format_admin_record(admin)
        tg_id = admin.get("tg_id")
        username = admin.get("username")
        if tg_id:
            callback = f"admins|remove|tg|{tg_id}"
        elif username:
            callback = f"admins|remove|user|{username}"
        else:
            continue
        buttons.append([InlineKeyboardButton(label, callback_data=callback)])
    buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="admins|menu")])
    return InlineKeyboardMarkup(buttons)


async def _send_remove_prompt(query) -> None:
    try:
        admins = await asyncio.to_thread(db_list_admins)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to load admins")
        await query.edit_message_text(f"❌ Ошибка получения списка администраторов: {exc}")
        return

    if not admins:
        await query.edit_message_text("⚠️ Администраторы не настроены.", reply_markup=_admins_menu_keyboard())
        return

    await query.edit_message_text(
        "🗑️ Выберите администратора для удаления:",
        reply_markup=_build_remove_keyboard(admins),
    )


async def show_admin_menu(*, query=None, message: Optional[Message] = None) -> None:
    text = "👑 Управление администраторами."
    markup = _admins_menu_keyboard()
    if query:
        await query.edit_message_text(text, reply_markup=markup)
    elif message:
        await message.reply_text(text, reply_markup=markup)


async def _perform_addadmin(
    message: Message,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    identifier: str,
    display_name: Optional[str] = None,
    target_user=None,
) -> None:
    if target_user is not None:
        tg_id = target_user.id
        username = target_user.username
        display_name = display_name or target_user.full_name
    else:
        tg_id, username = _parse_admin_identifier(identifier)

    if tg_id is None and not username:
        await message.reply_text("⚠️ Не удалось распознать ID или username. Попробуйте ещё раз.")
        return

    try:
        created, record = await asyncio.to_thread(
            db_add_admin,
            tg_id=tg_id,
            username=username,
            display_name=display_name,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to add admin")
        await message.reply_text(f"❌ Ошибка добавления администратора: {exc}")
        return

    status = "Добавлен" if created else "Обновлён"
    summary = _format_admin_record(record)
    await message.reply_text(f"✅ {status} администратор: {summary}")


async def _perform_removeadmin(
    message: Message,
    identifier: str,
    *,
    target_user=None,
) -> None:
    if target_user is not None:
        tg_id = target_user.id
        username = target_user.username
    else:
        tg_id, username = _parse_admin_identifier(identifier)

    if tg_id is None and (not username):
        await message.reply_text("⚠️ Некорректный идентификатор администратора.")
        return

    try:
        removed = await asyncio.to_thread(
            db_remove_admin,
            tg_id=tg_id,
            username=username,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to remove admin")
        await message.reply_text(f"❌ Ошибка удаления администратора: {exc}")
        return

    if removed:
        await message.reply_text("🗑️ Администратор удалён.")
    else:
        await message.reply_text("🔍 Администратор не найден.")


async def admins_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if _ensure_admin_message and not _ensure_admin_message(update):
        return
    await _send_admins_list_via_message(update.message)


async def addadmin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if _ensure_admin_message and not _ensure_admin_message(update):
        return
    context.user_data.pop(PENDING_ADDADMIN_KEY, None)

    identifier: Optional[str] = None
    display_name: Optional[str] = None
    target_user = None

    if context.args:
        identifier = context.args[0]
        if len(context.args) > 1:
            display_name = " ".join(context.args[1:])

    if not identifier and update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        identifier = str(target_user.id)
        display_name = display_name or target_user.full_name

    if not identifier:
        context.user_data[PENDING_ADDADMIN_KEY] = {
            "chat_id": update.message.chat_id,
        }
        await update.message.reply_text(
            "ℹ️ Укажите ID или @username (можно ответить на сообщение пользователя)."
        )
        return

    await _perform_addadmin(
        update.message,
        context,
        identifier=identifier,
        display_name=display_name,
        target_user=target_user,
    )


async def removeadmin_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    if _ensure_admin_message and not _ensure_admin_message(update):
        return
    context.user_data.pop(PENDING_REMOVEADMIN_KEY, None)

    identifier: Optional[str] = None
    target_user = None

    if context.args:
        identifier = context.args[0]

    if not identifier and update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        identifier = str(target_user.id)

    if not identifier:
        context.user_data[PENDING_REMOVEADMIN_KEY] = {"chat_id": update.message.chat_id}
        await update.message.reply_text(
            "ℹ️ Укажите ID или @username (можно ответить на сообщение администратора)."
        )
        return

    await _perform_removeadmin(update.message, identifier, target_user=target_user)


async def handle_admin_callback(action: str, parts: list[str], query, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if action != "admins":
        return False

    subaction = parts[1] if len(parts) >= 2 else "menu"
    if subaction in ("menu", ""):
        await show_admin_menu(query=query)
        return True
    if subaction == "list":
        await _send_admins_list_via_callback(query)
        return True
    if subaction == "add":
        context.user_data[PENDING_ADDADMIN_KEY] = {"chat_id": query.message.chat_id}
        await query.message.reply_text(
            "➕ Укажите ID или @username для добавления (можно ответить на сообщение пользователя)."
        )
        return True
    if subaction == "remove":
        if len(parts) >= 4:
            _, _, kind, value = parts[:4]
            tg_id = None
            username = None
            if kind == "tg":
                try:
                    tg_id = int(value)
                except ValueError:
                    tg_id = None
            elif kind == "user":
                username = value
            try:
                removed = await asyncio.to_thread(
                    db_remove_admin,
                    tg_id=tg_id,
                    username=username,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to remove admin")
                await query.edit_message_text(f"❌ Ошибка удаления администратора: {exc}")
                return True
            if removed:
                await query.answer("Администратор удалён.", show_alert=False)
            else:
                await query.answer("Администратор не найден.", show_alert=True)
            await _send_remove_prompt(query)
            return True

        context.user_data[PENDING_REMOVEADMIN_KEY] = {"chat_id": query.message.chat_id}
        await _send_remove_prompt(query)
        return True
    return False


async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message or not update.message.text:
        return False

    identifier_text = update.message.text.strip()
    for pending_key, action in (
        (PENDING_ADDADMIN_KEY, "add"),
        (PENDING_REMOVEADMIN_KEY, "remove"),
    ):
        pending = context.user_data.get(pending_key)
        if pending and pending.get("chat_id") == update.message.chat_id:
            context.user_data.pop(pending_key, None)
            if not identifier_text:
                await update.message.reply_text("⚠️ Укажите ID или @username.")
                return True
            if action == "add":
                await _perform_addadmin(update.message, context, identifier=identifier_text)
            else:
                await _perform_removeadmin(update.message, identifier_text)
            return True
    return False
