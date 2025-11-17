"""Intervals.icu integration helpers for the client bot."""
from __future__ import annotations

import logging
import base64
from datetime import date, timedelta
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from repositories.client_link_repository import get_link_by_user
from repositories.client_repository import get_client
from repositories.intervals_link_repository import (
    get_link as get_intervals_link,
    remove_link as remove_intervals_link,
    upsert_link as upsert_intervals_link,
)
from intervals_client import IntervalsClient

LOGGER = logging.getLogger(__name__)

PENDING_INTERVALS_KEY = "pending_intervals"
INTERVALS_EVENTS_KEY = "intervals_events"


def _fetch_linked_client(user_id: int):
    """Return (link, client) for the user or (None, None)."""
    link = get_link_by_user(user_id)
    client = None
    if link:
        client = get_client(link["client_id"])
    return link, client


def _format_intervals_status(tg_user_id: int) -> str:
    link = get_intervals_link(tg_user_id)
    if not link:
        return "❌ Intervals.icu не подключен.\n\nОтправьте команду:\n/intervals <API_KEY> [athlete_id]"
    athlete_id = link.get("intervals_athlete_id") or "0"
    return f"✅ Intervals.icu подключен.\nAthlete ID: {athlete_id}"


async def intervals_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /intervals command for linking Intervals.icu API key."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return

    # Ensure user is linked to a client
    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        await message.reply_text("Сначала привяжите свою анкету через /start.")
        return

    existing = get_intervals_link(user.id)
    if existing and existing.get("intervals_api_key"):
        # Already connected: show status + plan in one message with disconnect button
        status_text = _format_intervals_status(user.id)
        plan_text, events = await _build_plan_text(user.id, status_text)
        context.user_data[INTERVALS_EVENTS_KEY] = events or []
        markup = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("💾 Скачать .zwo (неделя)", callback_data="intervals_download_menu")],
                [InlineKeyboardButton("🔌 Отключить", callback_data="intervals_disconnect")],
            ]
        )
        await message.reply_text(plan_text, reply_markup=markup)
        return

    # Start interactive flow: ask for API key
    context.user_data[PENDING_INTERVALS_KEY] = {"step": "api_key"}
    await message.reply_text(
        "🔑 Отправьте API ключ Intervals.icu (из настроек → Developer Settings).\n"
        "После этого я спрошу athlete_id (опционально, по умолчанию 0).",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("❌ Отмена", callback_data="intervals_cancel")]]
        ),
    )


def _format_event_lines(events: list[dict]) -> list[str]:
    lines = []
    for ev in events:
        start_date = (ev.get("start_date_local") or "")[:10] or "?"
        name = ev.get("name") or "Без названия"
        moving = ev.get("moving_time")
        duration = ""
        try:
            if moving:
                mins = int(moving) // 60
                hours = mins // 60
                mins = mins % 60
                duration = f" • {hours}ч {mins:02d}м" if hours else f" • {mins}м"
        except Exception:
            duration = ""
        lines.append(f"{start_date} — {name}{duration}")
    return lines


async def _send_plan(user_id: int, message: Update.effective_message, status_text: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fetch and send 7-day plan if possible."""
    plan_text, events = await _build_plan_text(user_id, status_text)
    if plan_text:
        context.user_data[INTERVALS_EVENTS_KEY] = events or []
        markup = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("💾 Скачать .zwo (неделя)", callback_data="intervals_download_menu")],
                [InlineKeyboardButton("🔌 Отключить", callback_data="intervals_disconnect")],
            ]
        )
        await message.reply_text(plan_text, reply_markup=markup)


async def _build_plan_text(user_id: int, status_text: str) -> tuple[str, list[dict]]:
    """Return combined status + plan text and events list."""
    link = get_intervals_link(user_id)
    if not link:
        return status_text, []

    api_key = link.get("intervals_api_key")
    athlete_id = link.get("intervals_athlete_id") or "0"
    if not api_key:
        return status_text, []

    oldest = date.today().isoformat()
    newest = (date.today() + timedelta(days=7)).isoformat()
    try:
        client = IntervalsClient(api_key=api_key, athlete_id=athlete_id)
        events = client.fetch_events(category="WORKOUT", oldest=oldest, newest=newest, resolve=True)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to fetch Intervals plan for user %s", user_id)
        return status_text + f"\n\n⚠️ Не удалось загрузить план: {exc}", []

    if not events:
        return status_text + "\n\nПлан на неделю: нет записей.", []

    lines = [status_text, "\n📅 План на неделю:"]
    lines.extend(_format_event_lines(events))
    return "\n".join(lines), events


async def intervals_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline buttons for Intervals (cancel/disconnect)."""
    query = update.callback_query
    if query is None:
        return
    user = query.from_user
    if user is None:
        return
    if query.data == "intervals_cancel":
        context.user_data.pop(PENDING_INTERVALS_KEY, None)
        context.user_data.pop(INTERVALS_EVENTS_KEY, None)
        await query.answer()
        await query.edit_message_reply_markup(reply_markup=None)
        await query.edit_message_text("❌ Настройка Intervals.icu отменена.")
        return
    if query.data == "intervals_disconnect":
        removed = remove_intervals_link(user.id)
        context.user_data.pop(PENDING_INTERVALS_KEY, None)
        context.user_data.pop(INTERVALS_EVENTS_KEY, None)
        await query.answer()
        await query.edit_message_reply_markup(reply_markup=None)
        await query.edit_message_text("✅ Intervals.icu отключён." if removed else "Intervals.icu уже был отключён.")
        return
    if query.data == "intervals_download_menu":
        await query.answer()
        events = context.user_data.get(INTERVALS_EVENTS_KEY) or []
        if not events:
            await query.edit_message_text("Нет сохранённых тренировок на неделю. Отправьте /intervals ещё раз.")
            return
        buttons = []
        for idx, ev in enumerate(events):
            start_date = (ev.get("start_date_local") or "")[:10] or "?"
            name = ev.get("name") or "Без названия"
            label = f"{start_date} • {name}"
            buttons.append([InlineKeyboardButton(label[:64], callback_data=f"intervals_zwo|{idx}")])
        buttons.append([InlineKeyboardButton("❌ Отмена", callback_data="intervals_cancel")])
        await query.edit_message_text(
            "Выберите тренировку для скачивания .zwo:",
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def intervals_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle text input for API key / athlete id in a step-by-step flow."""
    message = update.effective_message
    user = update.effective_user
    if message is None or user is None:
        return
    pending = context.user_data.get(PENDING_INTERVALS_KEY)
    if not pending:
        return

    # Ensure user is linked
    link, client = _fetch_linked_client(user.id)
    if not link or not client:
        context.user_data.pop(PENDING_INTERVALS_KEY, None)
        await message.reply_text("Сначала привяжите свою анкету через /start.")
        return

    step = pending.get("step")
    text = (message.text or "").strip()
    if step == "api_key":
        if not text:
            await message.reply_text("⚠️ Укажите API ключ.")
            return
        pending["api_key"] = text
        pending["step"] = "athlete_id"
        await message.reply_text(
            "Укажите athlete_id (или отправьте 0, если используем связанный с ключом).",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton("⬅️ Пропустить (0)", callback_data="intervals_skip_athlete"),
                        InlineKeyboardButton("❌ Отмена", callback_data="intervals_cancel"),
                    ]
                ]
            ),
        )
        return

    if step == "athlete_id":
        athlete_id = text or "0"
        try:
            record = upsert_intervals_link(
                tg_user_id=user.id,
                api_key=pending.get("api_key", ""),
                athlete_id=athlete_id,
            )
            context.user_data.pop(PENDING_INTERVALS_KEY, None)
            status_text = (
                f"✅ Intervals.icu подключён.\nAthlete ID: {record.get('intervals_athlete_id') or '0'}"
            )
            # Add inline disconnect button for convenience
            markup = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("💾 Скачать .zwo (неделя)", callback_data="intervals_download_menu")],
                    [InlineKeyboardButton("🔌 Отключить", callback_data="intervals_disconnect")],
                ]
            )
            # Send status + plan
            plan_text, events = await _build_plan_text(user.id, status_text)
            context.user_data[INTERVALS_EVENTS_KEY] = events or []
            await message.reply_text(plan_text, reply_markup=markup)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to save Intervals.icu key for user %s", user.id)
            await message.reply_text(f"⚠️ Не удалось сохранить ключ: {exc}")
        return

    # Unknown step, reset
    context.user_data.pop(PENDING_INTERVALS_KEY, None)
    context.user_data.pop(INTERVALS_EVENTS_KEY, None)


async def handle_intervals_zwo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle ZWO download callback."""
    query = update.callback_query
    if query is None:
        return
    user = query.from_user
    if user is None:
        return
    data = query.data or ""
    if not data.startswith("intervals_zwo|"):
        return
    parts = data.split("|", 1)
    if len(parts) != 2:
        await query.answer("Некорректные данные.", show_alert=True)
        return
    try:
        idx = int(parts[1])
    except ValueError:
        await query.answer("Некорректный индекс.", show_alert=True)
        return

    events = context.user_data.get(INTERVALS_EVENTS_KEY) or []
    if idx < 0 or idx >= len(events):
        await query.answer("Тренировка не найдена.", show_alert=True)
        return
    event = events[idx]

    link = get_intervals_link(user.id)
    if not link or not link.get("intervals_api_key"):
        await query.answer("Интеграция не настроена.", show_alert=True)
        return

    api_key = link["intervals_api_key"]
    athlete_id = link.get("intervals_athlete_id") or "0"

    try:
        client = IntervalsClient(api_key=api_key, athlete_id=athlete_id)
        # Try fetching workout_file_base64 via events?ext=zwo for the specific date
        oldest = newest = (event.get("start_date_local") or "")[:10] or None
        zwo_bytes = None
        if oldest:
            zwo_bytes = client.download_event_zwo(oldest=oldest, newest=newest)
        if not zwo_bytes:
            # Fallback to converting workout_doc to zwo
            workout = event.get("workout_doc") or event
            if not workout:
                await query.answer("Нет данных тренировки.", show_alert=True)
                return
            zwo_bytes = client.download_workout_as_zwo(workout)
        name = event.get("name") or "workout"
        date_str = (event.get("start_date_local") or "")[:10] or "plan"
        filename = f"{date_str}_{name}".replace(" ", "_") + ".zwo"
        await query.answer()
        await query.message.reply_document(
            document=zwo_bytes,
            filename=filename,
            caption=f"💾 {name} ({date_str})",
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download ZWO for user %s", user.id)
        await query.answer("Не удалось скачать .zwo", show_alert=True)
