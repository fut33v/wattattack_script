"""Abstractions for fanning out admin notifications without leaking transports."""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Iterable, List, Optional, Sequence

from telegram import Bot, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from repositories.admin_repository import list_admins_for_notifications

log = logging.getLogger(__name__)

ADMIN_BOT_TOKEN_ENV = "TELEGRAM_BOT_TOKEN"

_ADMIN_BOT: Optional[Bot] = None
_WARNED_NO_TOKEN = False


def _get_admin_bot() -> Optional[Bot]:
    """Lazily instantiate the dedicated admin bot if configured."""
    global _ADMIN_BOT, _WARNED_NO_TOKEN
    token = os.environ.get(ADMIN_BOT_TOKEN_ENV)
    if not token:
        if not _WARNED_NO_TOKEN:
            log.warning("%s is not configured; admin notifications will use caller bots only", ADMIN_BOT_TOKEN_ENV)
            _WARNED_NO_TOKEN = True
        return None
    if _ADMIN_BOT is None:
        _ADMIN_BOT = Bot(token=token)
    return _ADMIN_BOT


def _filter_admins_by_instructor(admins: Sequence[dict], instructor_id: Optional[int]) -> List[int]:
    result: List[int] = []
    for admin in admins:
        tg_id = admin.get("tg_id")
        if tg_id is None:
            continue
        if admin.get("notify_instructor_only"):
            if instructor_id is None:
                continue
            try:
                admin_instr = int(admin.get("instructor_id")) if admin.get("instructor_id") is not None else None
            except Exception:
                admin_instr = None
            if admin_instr is None or admin_instr != instructor_id:
                continue
        try:
            result.append(int(tg_id))
        except Exception:
            continue
    return result


def _normalize_admin_ids(admin_ids: Iterable[int]) -> List[int]:
    normalized: List[int] = []
    for admin_id in admin_ids:
        try:
            normalized.append(int(admin_id))
        except Exception:
            continue
    return normalized


def resolve_admin_chat_ids(
    *,
    instructor_id: Optional[int] = None,
    admin_ids: Optional[Iterable[int]] = None,
) -> List[int]:
    """Return admin chat IDs respecting instructor filtering when available."""
    if admin_ids is not None:
        return _normalize_admin_ids(admin_ids)
    try:
        admins = list_admins_for_notifications()
    except Exception:
        log.exception("Failed to list admins for notifications")
        return []
    return _filter_admins_by_instructor(admins, instructor_id)


async def _send_single(
    admin_id: int,
    text: str,
    *,
    reply_markup: InlineKeyboardMarkup | None,
    context: ContextTypes.DEFAULT_TYPE | None,
) -> bool:
    admin_bot = _get_admin_bot()
    if admin_bot is not None:
        try:
            await admin_bot.send_message(chat_id=admin_id, text=text, reply_markup=reply_markup)
            return True
        except Exception:
            log.exception("Failed to send admin notification via dedicated bot", exc_info=True)
    if context is not None:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, reply_markup=reply_markup)
            return True
        except Exception:
            log.exception("Failed to send admin notification via caller bot context", exc_info=True)
    return False


async def notify_admins(
    text: str,
    *,
    instructor_id: Optional[int] = None,
    admin_ids: Optional[Iterable[int]] = None,
    reply_markup: InlineKeyboardMarkup | None = None,
    context: ContextTypes.DEFAULT_TYPE | None = None,
) -> List[int]:
    """Send a notification to admins and return the list of delivered chat IDs."""
    target_admins = resolve_admin_chat_ids(instructor_id=instructor_id, admin_ids=admin_ids)
    delivered: List[int] = []
    for admin_id in target_admins:
        sent = await _send_single(admin_id, text, reply_markup=reply_markup, context=context)
        if sent:
            delivered.append(admin_id)
    return delivered


def notify_admins_blocking(
    text: str,
    *,
    instructor_id: Optional[int] = None,
    admin_ids: Optional[Iterable[int]] = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> List[int]:
    """Blocking wrapper to use from synchronous code paths (e.g. VK bot)."""
    return asyncio.run(
        notify_admins(
            text,
            instructor_id=instructor_id,
            admin_ids=admin_ids,
            reply_markup=reply_markup,
            context=None,
        )
    )
