"""Entry points for the Krutilka VNB Telegram bot."""
from __future__ import annotations

import logging
from typing import Any, Dict, Final, List, Optional
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, User
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from repositories.client_repository import get_client, search_clients
from repositories.client_link_repository import (
    get_link_by_client,
    get_link_by_user,
    link_user_to_client,
)
from repositories.admin_repository import get_admin_ids, is_admin

LOGGER = logging.getLogger(__name__)

_GREETING_KEY: Final[str] = "krutilkavnbot:greeting"
_CANDIDATES_KEY: Final[str] = "krutilkavnbot:candidates"
_PENDING_APPROVALS_KEY: Final[str] = "krutilkavnbot:pending_approvals"

DEFAULT_GREETING: Final[str] = "Здравствуйте!"
MAX_SUGGESTIONS: Final[int] = 6

ASK_LAST_NAME, CONFIRM_LINK = range(2)


def _normalize_last_name(value: str) -> str:
    return value.strip().lower()


def _format_client_label(client: Dict) -> str:
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


def _find_clients_by_last_name(last_name: str) -> List[Dict]:
    normalized = _normalize_last_name(last_name)
    results = search_clients(last_name, limit=MAX_SUGGESTIONS * 2)
    exact_matches = [
        client for client in results if _normalize_last_name(client.get("last_name") or "") == normalized
    ]
    if exact_matches:
        return exact_matches[:MAX_SUGGESTIONS]
    return results[:MAX_SUGGESTIONS]


def _store_candidates(context: ContextTypes.DEFAULT_TYPE, clients: List[Dict]) -> None:
    context.user_data[_CANDIDATES_KEY] = {str(client["id"]): client for client in clients}


def _get_candidates(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Dict]:
    return context.user_data.get(_CANDIDATES_KEY, {})


def _clear_candidates(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(_CANDIDATES_KEY, None)


def _pending_approvals(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Dict[str, Any]]:
    approvals = context.application.bot_data.setdefault(_PENDING_APPROVALS_KEY, {})
    return approvals


def _store_pending_request(context: ContextTypes.DEFAULT_TYPE, request: Dict[str, Any]) -> None:
    approvals = _pending_approvals(context)
    approvals[request["request_id"]] = request


def _get_pending_request(context: ContextTypes.DEFAULT_TYPE, request_id: str) -> Optional[Dict[str, Any]]:
    approvals = _pending_approvals(context)
    return approvals.get(request_id)


def _pop_pending_request(context: ContextTypes.DEFAULT_TYPE, request_id: str) -> Optional[Dict[str, Any]]:
    approvals = _pending_approvals(context)
    return approvals.pop(request_id, None)


def _format_user_label(user: Optional[User]) -> str:
    if user is None:
        return "неизвестный пользователь"

    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    parts = [part for part in [first, last] if part]
    display = " ".join(parts) if parts else (user.username or f"id {user.id}")
    if user.username:
        handle = user.username if user.username.startswith("@") else f"@{user.username}"
        display = f"{display} ({handle})"
    return display


def _is_admin_user(user: Optional[User]) -> bool:
    tg_id = user.id if user else None
    username = user.username if user else None
    try:
        return is_admin(tg_id=tg_id, username=username)
    except Exception:
        LOGGER.exception("Failed to check admin status for user %s", tg_id)
        return False


async def _start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        LOGGER.debug("Cannot handle /start without message or user in update %s", update.update_id)
        return ConversationHandler.END

    _clear_candidates(context)
    greeting: str = context.application.bot_data.get(_GREETING_KEY, DEFAULT_GREETING)

    linked_client_name: Optional[str] = None
    try:
        existing = get_link_by_user(user.id)
        if existing:
            client = get_client(existing["client_id"])
            if client:
                linked_client_name = _format_client_label(client)
    except Exception:
        LOGGER.exception("Failed to check existing link for user %s", user.id)

    if linked_client_name:
        text = (
            f"{greeting}\n\n"
            f"Вы уже привязаны к клиенту {linked_client_name}.\n"
            "Если нужно изменить связь, отправьте новую фамилию клиента."
        )
    else:
        text = f"{greeting}\n\nДля авторизации отправьте свою фамилию."

    await message.reply_text(text)
    return ASK_LAST_NAME


async def _help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text(
        "Отправьте /start, чтобы пройти авторизацию. "
        "Бот попросит вашу фамилию и предложит выбрать клиента из списка."
    )


async def _unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None:
        return
    await message.reply_text("Команда не поддерживается. Используйте /start для авторизации.")


async def _handle_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    message = update.effective_message
    if message is None or not message.text:
        return ASK_LAST_NAME

    last_name = message.text.strip()
    if not last_name:
        await message.reply_text("Пожалуйста, отправьте фамилию текстом.")
        return ASK_LAST_NAME

    try:
        clients = _find_clients_by_last_name(last_name)
    except Exception:
        LOGGER.exception("Failed to search clients by last name %r", last_name)
        await message.reply_text("Не удалось выполнить поиск. Попробуйте ещё раз позже.")
        return ASK_LAST_NAME

    if not clients:
        await message.reply_text("Клиентов с такой фамилией не нашлось. Проверьте ввод и попробуйте снова.")
        return ASK_LAST_NAME

    _store_candidates(context, clients)

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text=_format_client_label(client),
                    callback_data=f"link:{client['id']}",
                )
            ]
            for client in clients
        ]
    )
    lines = ["Нашлись такие клиенты:"]
    for client in clients:
        lines.append(f"• {_format_client_label(client)}")
    if len(clients) >= MAX_SUGGESTIONS:
        lines.append("Если не нашли нужного, уточните фамилию и отправьте снова.")

    await message.reply_text("\n".join(lines), reply_markup=keyboard)
    return CONFIRM_LINK


async def _handle_link_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        LOGGER.debug("Callback without query or user in update %s", update.update_id)
        return ASK_LAST_NAME

    await query.answer()
    data = query.data or ""
    if not data.startswith("link:"):
        await query.answer("Неизвестное действие.", show_alert=True)
        return ASK_LAST_NAME

    client_id = data.split(":", 1)[1]
    candidates = _get_candidates(context)
    client = candidates.get(client_id)
    if client is None:
        await query.answer("Список устарел. Отправьте фамилию ещё раз.", show_alert=True)
        return ASK_LAST_NAME

    try:
        existing = get_link_by_client(client["id"])
    except Exception:
        existing = None

    _clear_candidates(context)

    admin_ids: List[int] = []
    try:
        admin_ids = [admin_id for admin_id in get_admin_ids() if admin_id]
    except Exception:
        LOGGER.exception("Failed to load admin IDs for approval request")

    if not admin_ids:
        await query.edit_message_text(
            "Не удалось отправить запрос на подтверждение: не найдены администраторы.",
        )
        if query.message:
            await query.message.reply_text(
                "Запрос не обработан. Обратитесь к администратору, чтобы настроить доступ.",
            )
        return ASK_LAST_NAME

    request_id = uuid4().hex
    tg_username = user.username if user.username else None
    tg_full_name = " ".join(filter(None, [user.first_name, user.last_name])).strip() or None

    request: Dict[str, Any] = {
        "request_id": request_id,
        "client": client,
        "user_id": user.id,
        "user_username": tg_username,
        "user_full_name": tg_full_name,
        "user_chat_id": query.message.chat_id if query.message else user.id,
        "existing": existing,
        "admin_messages": [],
    }
    _store_pending_request(context, request)

    await query.edit_message_text(
        f"Запрос на привязку клиента {_format_client_label(client)} отправлен администратору. "
        "Ожидайте подтверждения.",
    )

    await _notify_admins(context, request, admin_ids)
    if not request["admin_messages"]:
        _pop_pending_request(context, request_id)
        await query.edit_message_text(
            "Не удалось отправить запрос администраторам. Попробуйте позже или свяжитесь с поддержкой.",
        )
        if query.message:
            await query.message.reply_text(
                "Запрос не обработан. Сообщите администратору о проблеме или повторите попытку позже.",
            )

    return ASK_LAST_NAME


async def _cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _clear_candidates(context)
    message = update.effective_message
    if message is not None:
        await message.reply_text("Авторизация прервана. Чтобы начать заново, используйте /start.")
    return ConversationHandler.END


async def _notify_admins(
    context: ContextTypes.DEFAULT_TYPE,
    request: Dict[str, Any],
    admin_ids: List[int],
) -> None:
    client = request["client"]
    user_id = request["user_id"]
    user_username = request.get("user_username")
    user_full_name = request.get("user_full_name")
    existing = request.get("existing")

    client_label = _format_client_label(client)
    user_label_parts: List[str] = []
    if user_full_name:
        user_label_parts.append(user_full_name)
    if user_username:
        handle = user_username if user_username.startswith("@") else f"@{user_username}"
        user_label_parts.append(handle)
    user_label_parts.append(f"id {user_id}")
    user_label = ", ".join(user_label_parts)

    lines = [
        "Новый запрос на привязку клиента.",
        f"Клиент: {client_label}",
        f"Пользователь: {user_label}",
    ]

    if existing and existing.get("tg_user_id"):
        existing_user_id = existing.get("tg_user_id")
        existing_username = existing.get("tg_username")
        existing_full_name = existing.get("tg_full_name")
        existing_parts = []
        if existing_full_name:
            existing_parts.append(existing_full_name)
        if existing_username:
            handle = existing_username if existing_username.startswith("@") else f"@{existing_username}"
            existing_parts.append(handle)
        if existing_user_id:
            existing_parts.append(f"id {existing_user_id}")
        lines.append("Предыдущая связь: " + ", ".join(existing_parts))

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Подтвердить", callback_data=f"approve:{request['request_id']}"),
                InlineKeyboardButton("Отменить", callback_data=f"reject:{request['request_id']}"),
            ]
        ]
    )

    for admin_id in admin_ids:
        try:
            message = await context.bot.send_message(
                admin_id,
                "\n".join(lines),
                reply_markup=keyboard,
            )
            request["admin_messages"].append(
                {"chat_id": message.chat_id, "message_id": message.message_id}
            )
        except Exception:
            LOGGER.exception("Failed to send approval request %s to admin %s", request["request_id"], admin_id)


async def _handle_admin_decision(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    admin_user = update.effective_user
    if query is None or admin_user is None:
        return

    await query.answer()
    data = query.data or ""
    if ":" not in data:
        return
    action, request_id = data.split(":", 1)

    request = _get_pending_request(context, request_id)
    if request is None:
        await query.answer("Запрос уже обработан.", show_alert=True)
        try:
            await query.edit_message_text("Запрос уже обработан.")
        except Exception:
            pass
        return

    if not _is_admin_user(admin_user):
        await query.answer("Недостаточно прав.", show_alert=True)
        return

    client = request["client"]
    client_label = _format_client_label(client)
    user_chat_id = request["user_chat_id"]
    user_id = request["user_id"]

    if action == "approve":
        try:
            link_user_to_client(
                tg_user_id=user_id,
                client_id=client["id"],
                tg_username=request.get("user_username"),
                tg_full_name=request.get("user_full_name"),
            )
        except Exception:
            LOGGER.exception(
                "Failed to apply approved link for request %s (user %s, client %s)",
                request_id,
                user_id,
                client["id"],
            )
            await query.answer("Не удалось привязать клиента. Попробуйте позже.", show_alert=True)
            return

        admin_text = (
            f"✅ Запрос {request_id} подтверждён администратором {_format_user_label(admin_user)}.\n"
            f"Клиент: {client_label}"
        )
        user_text = (
            f"Администратор подтвердил привязку к клиенту {client_label}. "
            "Теперь вы можете пользоваться сервисом."
        )
    else:
        admin_text = (
            f"❌ Запрос {request_id} отклонён администратором {_format_user_label(admin_user)}.\n"
            f"Клиент: {client_label}"
        )
        user_text = (
            f"Администратор отклонил запрос на привязку к клиенту {client_label}. "
            "Связь осталась без изменений."
        )

    _pop_pending_request(context, request_id)

    try:
        await query.edit_message_text(admin_text)
    except Exception:
        LOGGER.debug("Failed to edit admin decision message for request %s", request_id, exc_info=True)

    for message_ref in request.get("admin_messages", []):
        if (
            message_ref.get("chat_id") == query.message.chat_id
            and message_ref.get("message_id") == query.message.message_id
        ):
            continue
        try:
            await context.bot.edit_message_text(
                admin_text,
                chat_id=message_ref["chat_id"],
                message_id=message_ref["message_id"],
            )
        except Exception:
            LOGGER.debug("Failed to update admin message for request %s", request_id, exc_info=True)

    try:
        await context.bot.send_message(user_chat_id, user_text)
    except Exception:
        LOGGER.exception(
            "Failed to notify user %s about decision %s for request %s",
            user_id,
            action,
            request_id,
        )


def create_application(token: str, greeting: str = DEFAULT_GREETING) -> Application:
    """Create a Telegram application with surname-based client linking."""
    if not token:
        raise ValueError("Telegram bot token must be provided")

    application = Application.builder().token(token).build()
    application.bot_data[_GREETING_KEY] = greeting or DEFAULT_GREETING

    conversation = ConversationHandler(
        entry_points=[CommandHandler("start", _start_handler)],
        states={
            ASK_LAST_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_last_name),
            ],
            CONFIRM_LINK: [
                CallbackQueryHandler(_handle_link_selection, pattern=r"^link:\d+$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_last_name),
            ],
        },
        fallbacks=[CommandHandler("cancel", _cancel_handler)],
        name="client_authorization",
        persistent=False,
    )

    application.add_handler(conversation)
    application.add_handler(CommandHandler("help", _help_handler))
    application.add_handler(CallbackQueryHandler(_handle_admin_decision, pattern=r"^(approve|reject):"))
    application.add_handler(MessageHandler(filters.COMMAND, _unknown_command_handler))

    return application


__all__ = ["create_application", "DEFAULT_GREETING"]
