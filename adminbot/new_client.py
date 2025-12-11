"""Conversation flow for creating a new client."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

from repositories.client_repository import create_client
from wattattack_profiles import DEFAULT_CLIENT_FTP

LOGGER = logging.getLogger(__name__)

PEDAL_OPTIONS: List[Tuple[str, str]] = [
    ("топталки (под кроссовки)", "platform"),
    ("контакты шоссе Look", "road_look"),
    ("контакты шоссе Shimano", "road_shimano"),
    ("контакты MTB Shimano", "mtb_shimano"),
    ("принесу свои", "own"),
]
PEDAL_OPTION_LABEL_BY_CODE: Dict[str, str] = {code: label for label, code in PEDAL_OPTIONS}

(
    NEWCLIENT_FIRST_NAME,
    NEWCLIENT_LAST_NAME,
    NEWCLIENT_WEIGHT,
    NEWCLIENT_HEIGHT,
    NEWCLIENT_GENDER,
    NEWCLIENT_FTP,
    NEWCLIENT_PEDALS,
    NEWCLIENT_GOAL,
) = range(8)

NEWCLIENT_FORM_KEY = "adminbot:newclient_form"

_ensure_admin_message: Optional[Callable[[Update], bool]] = None
_ensure_admin_callback: Optional[Callable[[Any], Awaitable[bool]]] = None


def configure_new_client(
    *,
    ensure_admin_message: Callable[[Update], bool],
    ensure_admin_callback: Callable[[Any], Awaitable[bool]],
) -> None:
    global _ensure_admin_message, _ensure_admin_callback
    _ensure_admin_message = ensure_admin_message
    _ensure_admin_callback = ensure_admin_callback


def _require_configured() -> None:
    if _ensure_admin_message is None or _ensure_admin_callback is None:
        raise RuntimeError("new_client module is not configured. Call configure_new_client first.")


def _newclient_get_form(context: ContextTypes.DEFAULT_TYPE) -> Dict[str, Any]:
    form = context.user_data.get(NEWCLIENT_FORM_KEY)
    if form is None:
        form = {}
        context.user_data[NEWCLIENT_FORM_KEY] = form
    return form


def _newclient_reset_form(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(NEWCLIENT_FORM_KEY, None)


def _parse_optional_float_value(text: str) -> Tuple[bool, Optional[float]]:
    normalized = text.replace(",", ".").strip()
    if not normalized:
        return True, None
    try:
        return True, float(normalized)
    except ValueError:
        return False, None


def _parse_positive_float_value(text: str) -> Tuple[bool, Optional[float]]:
    ok, value = _parse_optional_float_value(text)
    if not ok or value is None:
        return False, None
    if value <= 0:
        return False, None
    return True, value


def _format_optional_number(value: Optional[float]) -> str:
    if value is None:
        return "—"
    return f"{value:g}"


def _format_client_display_name(record: Dict[str, Any]) -> str:
    first = record.get("first_name")
    last = record.get("last_name")
    if first or last:
        return " ".join(part for part in [first, last] if part).strip()
    return record.get("full_name") or f"id={record.get('id')}"


async def _newclient_send_gender_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    rows = [
        [
            InlineKeyboardButton("М", callback_data="newclient:gender:male"),
            InlineKeyboardButton("Ж", callback_data="newclient:gender:female"),
        ]
    ]
    await context.bot.send_message(
        chat_id=chat_id,
        text="👤 Выберите пол клиента:",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _newclient_send_ftp_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    await context.bot.send_message(
        chat_id,
        (
            f"⚡️ Укажите FTP клиента.\n"
            f"Значение по умолчанию — {DEFAULT_CLIENT_FTP}.\n"
            "Введите число или нажмите кнопку оставить без изменений."
        ),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Оставить 150", callback_data="newclient:ftp:skip")]]
        ),
    )


async def _newclient_send_pedals_prompt(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
) -> None:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"newclient:pedals:{code}")]
        for label, code in PEDAL_OPTIONS
    ]
    await context.bot.send_message(
        chat_id,
        "🚴 Выберите тип педалей из доступных вариантов:",
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def _newclient_send_goal_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Пропустить", callback_data="newclient:goal:skip")]]
    )
    await context.bot.send_message(
        chat_id,
        "🎯 Укажите цель клиента (например, подготовка к старту).\n"
        "Если цель не нужна, отправьте пустое сообщение или нажмите «Пропустить».",
        reply_markup=keyboard,
    )


async def _newclient_finalize_creation(
    *,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    send_message: Callable[[str], Awaitable[Any]],
) -> int:
    form = _newclient_get_form(context)

    first_name = (form.get("first_name") or "").strip()
    last_name = (form.get("last_name") or "").strip()
    weight = form.get("weight")
    height = form.get("height")
    gender = form.get("gender")
    ftp = form.get("ftp", float(DEFAULT_CLIENT_FTP))
    pedals = form.get("pedals")
    goal = form.get("goal")

    missing: List[str] = []
    if not first_name:
        missing.append("имя")
    if not last_name:
        missing.append("фамилия")
    if weight is None:
        missing.append("вес")
    if height is None:
        missing.append("рост")
    if gender not in {"male", "female"}:
        missing.append("пол")
    if not pedals:
        missing.append("педали")

    if missing:
        await send_message(
            "⚠️ Анкета заполнена не полностью. Завершите команду и начните заново: "
            + ", ".join(missing)
        )
        _newclient_reset_form(context)
        return ConversationHandler.END

    try:
        client = await asyncio.to_thread(
            create_client,
            first_name=first_name,
            last_name=last_name,
            weight=weight,
            height=height,
            gender=gender,
            ftp=ftp,
            pedals=pedals,
            goal=goal,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to create client %s %s", first_name, last_name)
        await send_message(f"❌ Не удалось создать запись: {exc}")
        _newclient_reset_form(context)
        return ConversationHandler.END

    summary_lines = [
        "🆕 Создан клиент:",
        f"• Имя: {first_name}",
        f"• Фамилия: {last_name}",
        f"• Пол: {'М' if gender == 'male' else 'Ж'}",
        f"• Вес: {_format_optional_number(weight)} кг",
        f"• Рост: {_format_optional_number(height)} см",
        f"• FTP: {_format_optional_number(ftp)} Вт",
        f"• Педали: {pedals}",
        f"• Цель: {goal or '—'}",
    ]
    await send_message("\n".join(summary_lines))

    client_label = _format_client_display_name(client)
    await context.bot.send_message(
        chat_id,
        f"✅ Запись сохранена: {client_label} (ID {client['id']}).",
    )

    _newclient_reset_form(context)
    return ConversationHandler.END


async def newclient_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    query = update.callback_query
    message = update.effective_message
    if query:
        await query.answer()
        if not await _ensure_admin_callback(query):
            return ConversationHandler.END
    else:
        if not update.message:
            return ConversationHandler.END
        if not _ensure_admin_message(update):
            return ConversationHandler.END

    if not message:
        return ConversationHandler.END

    _newclient_reset_form(context)
    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP)

    await message.reply_text(
        "🆕 Создание новой анкеты клиента.\n"
        "🖊️ Введите имя клиента (команда /cancel для отмены)."
    )
    return NEWCLIENT_FIRST_NAME


async def newclient_first_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    if not update.message:
        return NEWCLIENT_FIRST_NAME
    if not _ensure_admin_message(update):
        return ConversationHandler.END

    value = update.message.text.strip()
    if not value:
        await update.message.reply_text("⚠️ Имя не должно быть пустым. Введите имя клиента.")
        return NEWCLIENT_FIRST_NAME

    form = _newclient_get_form(context)
    form["first_name"] = value
    await update.message.reply_text("🧾 Введите фамилию клиента.")
    return NEWCLIENT_LAST_NAME


async def newclient_last_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    if not update.message:
        return NEWCLIENT_LAST_NAME
    if not _ensure_admin_message(update):
        return ConversationHandler.END

    value = update.message.text.strip()
    if not value:
        await update.message.reply_text("⚠️ Фамилия не должна быть пустой. Введите фамилию клиента.")
        return NEWCLIENT_LAST_NAME

    form = _newclient_get_form(context)
    form["last_name"] = value
    await update.message.reply_text(
        "⚖️ Введите вес клиента в килограммах (например, 72.5)."
    )
    return NEWCLIENT_WEIGHT


async def newclient_weight(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    if not update.message:
        return NEWCLIENT_WEIGHT
    if not _ensure_admin_message(update):
        return ConversationHandler.END

    ok, weight = _parse_positive_float_value(update.message.text or "")
    if not ok or weight is None:
        await update.message.reply_text(
            "⚠️ Вес должен быть положительным числом (например, 72.5). Повторите ввод."
        )
        return NEWCLIENT_WEIGHT

    form = _newclient_get_form(context)
    form["weight"] = weight
    await update.message.reply_text(
        "📏 Введите рост клиента в сантиметрах (например, 178)."
    )
    return NEWCLIENT_HEIGHT


async def newclient_height(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    if not update.message:
        return NEWCLIENT_HEIGHT
    if not _ensure_admin_message(update):
        return ConversationHandler.END

    ok, height = _parse_positive_float_value(update.message.text or "")
    if not ok or height is None:
        await update.message.reply_text(
            "⚠️ Рост должен быть положительным числом (например, 178). Повторите ввод."
        )
        return NEWCLIENT_HEIGHT

    form = _newclient_get_form(context)
    form["height"] = height
    await _newclient_send_gender_prompt(context, update.message.chat_id)
    return NEWCLIENT_GENDER


async def newclient_gender_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_GENDER
    if not await _ensure_admin_callback(query):
        return NEWCLIENT_GENDER

    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_GENDER
    gender = parts[2]
    if gender not in {"male", "female"}:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_GENDER

    form = _newclient_get_form(context)
    form["gender"] = gender

    label = "М" if gender == "male" else "Ж"
    try:
        await query.edit_message_text(f"👤 Пол: {label}")
    except Exception:
        LOGGER.debug("Failed to edit gender selection message", exc_info=True)

    await _newclient_send_ftp_prompt(context, query.message.chat_id)
    return NEWCLIENT_FTP


async def newclient_gender_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    message = update.effective_message
    if message is not None:
        await message.reply_text("👤 Пожалуйста, выберите пол с помощью кнопок.")
    return NEWCLIENT_GENDER


async def newclient_ftp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    if not update.message:
        return NEWCLIENT_FTP
    if not _ensure_admin_message(update):
        return ConversationHandler.END

    ok, ftp = _parse_optional_float_value(update.message.text or "")
    if not ok or (ftp is not None and ftp <= 0):
        await update.message.reply_text(
            "⚠️ Введите положительное число (например, 215) или нажмите кнопку, чтобы оставить значение по умолчанию."
        )
        return NEWCLIENT_FTP

    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP) if ftp is None else ftp
    await _newclient_send_pedals_prompt(context, update.message.chat_id)
    return NEWCLIENT_PEDALS


async def newclient_skip_ftp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_FTP
    if not await _ensure_admin_callback(query):
        return NEWCLIENT_FTP

    form = _newclient_get_form(context)
    form["ftp"] = float(DEFAULT_CLIENT_FTP)
    try:
        await query.edit_message_text("ℹ️ FTP оставлен по умолчанию (150).")
    except Exception:
        LOGGER.debug("Failed to edit FTP skip message", exc_info=True)

    await _newclient_send_pedals_prompt(context, query.message.chat_id)
    return NEWCLIENT_PEDALS


async def newclient_pedals_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_PEDALS
    if not await _ensure_admin_callback(query):
        return NEWCLIENT_PEDALS

    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_PEDALS
    code = parts[2]
    label = PEDAL_OPTION_LABEL_BY_CODE.get(code)
    if label is None:
        await query.answer("Неизвестный выбор.", show_alert=True)
        return NEWCLIENT_PEDALS

    form = _newclient_get_form(context)
    form["pedals"] = label
    try:
        await query.edit_message_text(f"🚴 Педали: {label}")
    except Exception:
        LOGGER.debug("Failed to edit pedals selection message", exc_info=True)

    await _newclient_send_goal_prompt(context, query.message.chat_id)
    return NEWCLIENT_GOAL


async def newclient_pedals_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    message = update.effective_message
    if message is not None:
        await message.reply_text("🚴 Выберите вариант педалей при помощи кнопок.")
    return NEWCLIENT_PEDALS


async def newclient_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    if not update.message:
        return NEWCLIENT_GOAL
    if not _ensure_admin_message(update):
        return ConversationHandler.END

    value = (update.message.text or "").strip()
    form = _newclient_get_form(context)
    form["goal"] = value or None

    return await _newclient_finalize_creation(
        context=context,
        chat_id=update.message.chat_id,
        send_message=update.message.reply_text,
    )


async def newclient_skip_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    query = update.callback_query
    if query is None or query.message is None:
        return NEWCLIENT_GOAL
    if not await _ensure_admin_callback(query):
        return NEWCLIENT_GOAL

    form = _newclient_get_form(context)
    form["goal"] = None
    try:
        await query.edit_message_text("ℹ️ Цель не указана.")
    except Exception:
        LOGGER.debug("Failed to edit goal skip message", exc_info=True)

    return await _newclient_finalize_creation(
        context=context,
        chat_id=query.message.chat_id,
        send_message=lambda text: context.bot.send_message(query.message.chat_id, text),
    )


async def newclient_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _require_configured()
    _newclient_reset_form(context)
    message = update.effective_message
    if message is not None:
        await message.reply_text("Создание клиента отменено.")
    return ConversationHandler.END


__all__ = [
    "DEFAULT_CLIENT_FTP",
    "PEDAL_OPTIONS",
    "PEDAL_OPTION_LABEL_BY_CODE",
    "configure_new_client",
    "NEWCLIENT_FIRST_NAME",
    "NEWCLIENT_LAST_NAME",
    "NEWCLIENT_WEIGHT",
    "NEWCLIENT_HEIGHT",
    "NEWCLIENT_GENDER",
    "NEWCLIENT_FTP",
    "NEWCLIENT_PEDALS",
    "NEWCLIENT_GOAL",
    "newclient_start",
    "newclient_first_name",
    "newclient_last_name",
    "newclient_weight",
    "newclient_height",
    "newclient_gender_selection",
    "newclient_gender_prompt",
    "newclient_ftp",
    "newclient_skip_ftp",
    "newclient_pedals_selection",
    "newclient_pedals_prompt",
    "newclient_goal",
    "newclient_skip_goal",
    "newclient_cancel",
]
