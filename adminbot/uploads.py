"""Upload-related command handlers for adminbot."""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import CommandHandler, ContextTypes, MessageHandler, filters

from adminbot.accounts import (
    AccountConfig,
    resolve_account_tokens as resolve_account_tokens_value,
    resolve_account_identifier as resolve_account_identifier_value,
)
from repositories.client_repository import create_client
from repositories.bikes_repository import ensure_bikes_table
from repositories.trainers_repository import ensure_trainers_table
from repositories.layout_repository import ensure_layout_table
from scripts.load_clients import load_clients_from_csv_bytes, export_clients_to_csv_bytes
from scripts.load_bikes import load_bikes_from_csv_bytes
from scripts.load_trainers import load_trainers_from_csv_bytes
from scripts.import_schedule_from_xlsx import (
    run_schedule_import_from_bytes,
    format_import_report as format_schedule_import_report,
)
from wattattack_workouts import (
    build_workout_payload,
    calculate_workout_metrics,
    parse_zwo_workout,
    zwo_to_chart_data,
)

LOGGER = logging.getLogger(__name__)

PENDING_UPLOAD_KEY = "pending_inventory_upload"
PENDING_WORKOUT_UPLOAD_KEY = "pending_workout_upload"
PENDING_WORKOUT_FILE_KEY = "pending_workout_file"

UPLOAD_COMMAND_TYPES = {
    "/uploadclients": "clients",
    "/uploadbikes": "bikes",
    "/uploadstands": "stands",
    "/uploadschedule": "schedule",
}
WORKOUT_UPLOAD_COMMAND = "/uploadworkout"
UPLOADCLIENTS_PROMPT = (
    "📄 Выберите режим импорта клиентов.\n"
    "• «Обновление» — добавить новых и обновить существующих.\n"
    "• «Перезаписать» — очистить таблицу и загрузить заново.\n"
    "• «Dry run» — проверить файл без изменений в базе.\n"
    "После выбора нажмите «Готово» и отправьте CSV документ."
)
UPLOADSCHEDULE_PROMPT = (
    "📅 Выберите режим импорта расписания.\n"
    "• «Keep» — не очищать существующие недели (добавление/обновление).\n"
    "• Без keep — недели будут полностью заменены.\n"
    "• «Dry run» — проверить файл без записи.\n"
    "После выбора нажмите «Готово» и отправьте XLSX документ."
)


EnsureAdminFunc = Callable[[Update], bool]
ShowAccountSelectionFunc = Callable[..., Awaitable[None]]

_ensure_admin_message: Optional[EnsureAdminFunc] = None
_show_account_selection: Optional[ShowAccountSelectionFunc] = None
_account_registry: Mapping[str, AccountConfig] = {}
_default_timeout: float = 30.0


def configure_uploads(
    *,
    ensure_admin_message: EnsureAdminFunc,
    show_account_selection: ShowAccountSelectionFunc,
    account_registry: Mapping[str, AccountConfig],
    default_timeout: float,
) -> None:
    global _ensure_admin_message, _show_account_selection, _account_registry, _default_timeout
    _ensure_admin_message = ensure_admin_message
    _show_account_selection = show_account_selection
    _account_registry = account_registry
    _default_timeout = default_timeout


def _require_setup() -> None:
    if _ensure_admin_message is None or _show_account_selection is None:
        raise RuntimeError("Uploads module is not configured. Call configure_uploads first.")


def resolve_account_tokens(tokens: Iterable[str]) -> Tuple[List[str], List[str]]:
    return resolve_account_tokens_value(_account_registry, tokens)


def _make_reply_func(
    bot,
    chat_id: int,
    reply_to_message_id: Optional[int] = None,
) -> Callable[[str], Awaitable[Any]]:
    async def _reply(text: str) -> Any:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_to_message_id=reply_to_message_id,
        )

    return _reply


def _set_pending_upload(
    user_data: Dict,
    upload_type: str,
    *,
    truncate: bool,
    update: bool = False,
    **extra: Any,
) -> None:
    payload: Dict[str, Any] = {"type": upload_type, "truncate": truncate, "update": update}
    if extra:
        payload.update(extra)
    user_data[PENDING_UPLOAD_KEY] = payload


def _pop_pending_upload(user_data: Dict) -> Optional[Dict[str, Any]]:
    value = user_data.get(PENDING_UPLOAD_KEY)
    if value is not None:
        user_data.pop(PENDING_UPLOAD_KEY, None)
    return value


def _get_pending_upload(user_data: Dict) -> Optional[Dict[str, Any]]:
    value = user_data.get(PENDING_UPLOAD_KEY)
    if isinstance(value, dict):
        return value
    return None


def _set_pending_workout_upload(user_data: Dict[str, Any], account_ids: List[str]) -> None:
    unique_ids = list(dict.fromkeys(account_ids))
    user_data[PENDING_WORKOUT_UPLOAD_KEY] = {"account_ids": unique_ids}


def _pop_pending_workout_upload(user_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    value = user_data.get(PENDING_WORKOUT_UPLOAD_KEY)
    if value is not None:
        user_data.pop(PENDING_WORKOUT_UPLOAD_KEY, None)
    return value


def _set_pending_workout_file(
    user_data: Dict[str, Any], *, data: bytes, file_name: str, chat_id: int, reply_to_message_id: Optional[int]
) -> None:
    user_data[PENDING_WORKOUT_FILE_KEY] = {
        "data": data,
        "file_name": file_name,
        "chat_id": chat_id,
        "reply_to_message_id": reply_to_message_id,
    }


def _get_pending_workout_file(user_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    value = user_data.get(PENDING_WORKOUT_FILE_KEY)
    if not value:
        return None
    if not isinstance(value, dict):
        return None
    return value


def _clear_pending_workout_file(user_data: Dict[str, Any]) -> None:
    user_data.pop(PENDING_WORKOUT_FILE_KEY, None)


def build_uploadclients_keyboard(state: Dict[str, Any]) -> InlineKeyboardMarkup:
    update_on = state.get("update", True)
    truncate_on = state.get("truncate", False)
    dry_run_on = state.get("dry_run", False)

    update_label = "🔁 Обновление (вкл)" if update_on else "🔁 Обновление (выкл)"
    truncate_label = "🧹 Перезаписать (вкл)" if truncate_on else "🧹 Перезаписать (выкл)"
    dry_label = "🧪 Dry run (вкл)" if dry_run_on else "🧪 Dry run (выкл)"

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(text=update_label, callback_data="uploadclients_mode|update"),
                InlineKeyboardButton(text=truncate_label, callback_data="uploadclients_mode|truncate"),
            ],
            [
                InlineKeyboardButton(text=dry_label, callback_data="uploadclients_mode|dry_toggle"),
                InlineKeyboardButton(
                    text="✅ Готово, жду файл",
                    callback_data="uploadclients_mode|confirm",
                ),
            ],
        ]
    )


async def handle_uploadclients_mode(query, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    state = _get_pending_upload(context.user_data)
    if not state or state.get("type") != "clients":
        await query.answer("Сначала вызовите /uploadclients.", show_alert=True)
        return

    if mode == "update":
        state["update"] = True
        state["truncate"] = False
    elif mode == "truncate":
        state["truncate"] = True
        state["update"] = False
    elif mode == "dry_toggle":
        state["dry_run"] = not state.get("dry_run", False)
    elif mode == "confirm":
        mode_label = "перезапись" if state.get("truncate") else "обновление"
        dry_label = "dry run" if state.get("dry_run") else "боевой режим"
        await query.edit_message_text(
            f"Режим выбран: {mode_label}, {dry_label}.\n"
            "Пришлите CSV файл (как документ), чтобы выполнить импорт."
        )
        return
    else:
        await query.answer("Неизвестный вариант.", show_alert=True)
        return

    text = UPLOADCLIENTS_PROMPT
    markup = build_uploadclients_keyboard(state)
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except Exception:
        await query.message.reply_text(text, reply_markup=markup)


def build_uploadschedule_keyboard(state: Dict[str, Any]) -> InlineKeyboardMarkup:
    keep_on = state.get("keep_existing", False)
    dry_on = state.get("dry_run", False)
    keep_label = "📌 Keep (вкл)" if keep_on else "📌 Keep (выкл)"
    dry_label = "🧪 Dry run (вкл)" if dry_on else "🧪 Dry run (выкл)"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(text=keep_label, callback_data="uploadschedule_mode|keep_toggle"),
                InlineKeyboardButton(text=dry_label, callback_data="uploadschedule_mode|dry_toggle"),
            ],
            [
                InlineKeyboardButton(
                    text="✅ Готово, жду файл",
                    callback_data="uploadschedule_mode|confirm",
                )
            ],
        ]
    )


async def handle_uploadschedule_mode(query, context: ContextTypes.DEFAULT_TYPE, mode: str) -> None:
    state = _get_pending_upload(context.user_data)
    if not state or state.get("type") != "schedule":
        await query.answer("Сначала вызовите /uploadschedule.", show_alert=True)
        return

    if mode == "keep_toggle":
        state["keep_existing"] = not state.get("keep_existing", False)
    elif mode == "dry_toggle":
        state["dry_run"] = not state.get("dry_run", False)
    elif mode == "confirm":
        keep_label = "keep" if state.get("keep_existing") else "replace"
        dry_label = "dry run" if state.get("dry_run") else "боевой режим"
        await query.edit_message_text(
            f"Режим выбран: {keep_label}, {dry_label}.\n"
            "Пришлите XLSX файл расписания (как документ), чтобы выполнить импорт."
        )
        return
    else:
        await query.answer("Неизвестный вариант.", show_alert=True)
        return

    text = UPLOADSCHEDULE_PROMPT
    markup = build_uploadschedule_keyboard(state)
    try:
        await query.edit_message_text(text, reply_markup=markup)
    except Exception:
        await query.message.reply_text(text, reply_markup=markup)


async def process_clients_document(
    document,
    message: Message,
    truncate: bool = False,
    update_existing: bool = False,
    dry_run: bool = False,
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download CSV file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        total, inserted, updated = await asyncio.to_thread(
            load_clients_from_csv_bytes,
            bytes(data),
            truncate=truncate,
            update_existing=update_existing,
            dry_run=dry_run,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import clients CSV")
        await message.reply_text(f"❌ Ошибка импорта: {exc}")
        return

    mode_label = "Перезаписал" if truncate else "Загрузил"
    if dry_run:
        mode_label = "Проверил (dry run)"
    await message.reply_text(
        f"✅ {mode_label} клиентов.\nВсего строк: {total}.\nДобавлено: {inserted}.\nОбновлено: {updated}."
    )


async def process_bikes_document(
    document,
    message: Message,
    truncate: bool = False,
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download bikes CSV file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        total, inserted, updated = await asyncio.to_thread(
            load_bikes_from_csv_bytes,
            bytes(data),
            truncate=truncate,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import bikes CSV")
        await message.reply_text(f"❌ Ошибка импорта велосипедов: {exc}")
        return

    mode_label = "Перезаписал" if truncate else "Загрузил"
    await message.reply_text(
        f"✅ {mode_label} велосипеды.\nВсего строк: {total}.\nДобавлено: {inserted}.\nОбновлено: {updated}."
    )


async def process_trainers_document(
    document,
    message: Message,
    truncate: bool = False,
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download trainers CSV file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        total, inserted, updated = await asyncio.to_thread(
            load_trainers_from_csv_bytes,
            bytes(data),
            truncate=truncate,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import trainers CSV")
        await message.reply_text(f"❌ Ошибка импорта станков: {exc}")
        return

    mode_label = "Перезаписал" if truncate else "Загрузил"
    await message.reply_text(
        f"✅ {mode_label} станки.\nВсего строк: {total}.\nДобавлено: {inserted}.\nОбновлено: {updated}."
    )


async def process_schedule_document(
    document,
    message: Message,
    keep_existing: bool = False,
    dry_run: bool = False,
) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download schedule XLSX file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    try:
        report = await asyncio.to_thread(
            run_schedule_import_from_bytes,
            bytes(data),
            keep_existing=keep_existing,
            dry_run=dry_run,
        )
        formatted = await asyncio.to_thread(format_import_report_report_wrapper, report)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to import schedule XLSX")
        await message.reply_text(f"❌ Ошибка импорта расписания: {exc}")
        return

    prefix = "🧪 Проверка (без записи):\n" if dry_run else ""
    await message.reply_text(prefix + formatted, parse_mode=ParseMode.HTML)


def format_import_report_report_wrapper(report) -> str:
    return format_schedule_import_report(report)


async def process_workout_bytes(
    raw_bytes: bytes,
    file_name: str,
    account_ids: List[str],
    reply_func: Callable[[str], Awaitable[Any]],
) -> Tuple[bool, str]:
    def worker() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        xml_text: str
        if isinstance(raw_bytes, bytes):
            try:
                xml_text = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                xml_text = raw_bytes.decode("utf-8", errors="ignore")
        else:
            xml_text = str(raw_bytes)
        try:
            workout = parse_zwo_workout(xml_text)
        except Exception:
            LOGGER.exception("Failed to parse workout file %s", file_name)
            return None, "⚠️ Не удалось разобрать ZWO файл."

        chart_data = zwo_to_chart_data(workout)
        ftp = workout.get("ftp")
        if ftp is None:
            ftp = None
        metrics = calculate_workout_metrics(workout, ftp)
        payload = build_workout_payload(workout, chart_data, metrics)
        return payload, None

    payload, error = await asyncio.to_thread(worker)
    if error:
        await reply_func(error)
        return False, "parse-error"
    if not payload:
        await reply_func("⚠️ Не удалось обработать файл.")
        return False, "parse-error"

    results: List[str] = []
    success_any = False
    for account_id in account_ids:
        ok, message = await upload_workout_for_account(account_id, payload)
        success_any = success_any or ok
        status = "✅" if ok else "⚠️"
        results.append(f"{status} {account_id}: {message}")

    await reply_func("\n".join(results))
    return success_any, "\n".join(results)


async def upload_workout_for_account(account_id: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    from wattattack_activities import WattAttackClient  # local import to avoid cycles

    account = _account_registry.get(account_id)
    if not account:
        LOGGER.warning("Workout upload requested for unknown account %s", account_id)
        return False, "Аккаунт не найден"

    def worker() -> Tuple[bool, str]:
        try:
            client = WattAttackClient()
            client.login(account.email, account.password, timeout=_default_timeout)
            response = client.upload_workout(payload, timeout=_default_timeout)
            if isinstance(response, dict):
                message = response.get("message") or "Загружено"
                workout_info = response.get("workout")
                if isinstance(workout_info, dict) and workout_info.get("id"):
                    message = f"{message} (ID {workout_info['id']})"
            else:
                message = "Загружено"
            return True, message
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to upload workout for %s", account_id)
            return False, str(exc)

    return await asyncio.to_thread(worker)


async def process_workout_document(document, message: Message, account_ids: List[str]) -> None:
    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download workout file")
        await message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    file_name = document.file_name or "workout.zwo"
    await process_workout_bytes(
        raw_bytes=bytes(data),
        file_name=file_name,
        account_ids=account_ids,
        reply_func=message.reply_text,
    )


async def uploadclients_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    args_lower = [arg.lower() for arg in context.args or []]
    truncate = any(arg in {"truncate", "--truncate"} for arg in args_lower)
    update_mode = not truncate
    if any(arg in {"update", "--update"} for arg in args_lower):
        update_mode = True
    dry_run = any(arg in {"dry-run", "dry", "--dry-run"} for arg in args_lower)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_clients_document(
            update.message.reply_to_message.document,
            update.message,
            truncate=truncate,
            update_existing=update_mode,
            dry_run=dry_run,
        )
        return

    _set_pending_upload(
        context.user_data,
        "clients",
        truncate=truncate,
        update=update_mode,
        dry_run=dry_run,
    )
    state = _get_pending_upload(context.user_data) or {}
    await update.message.reply_text(
        UPLOADCLIENTS_PROMPT,
        reply_markup=build_uploadclients_keyboard(state),
    )


async def uploadbikes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    truncate = False
    if context.args:
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in context.args)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_bikes_document(
            update.message.reply_to_message.document,
            update.message,
            truncate=truncate,
        )
        return

    _set_pending_upload(context.user_data, "bikes", truncate=truncate)
    await update.message.reply_text(
        "📄 Пришлите CSV файл (как документ). Можно указать /uploadbikes truncate для полной перезагрузки."
    )


async def uploadstands_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    truncate = False
    if context.args:
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in context.args)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_trainers_document(
            update.message.reply_to_message.document,
            update.message,
            truncate=truncate,
        )
        return

    _set_pending_upload(context.user_data, "stands", truncate=truncate)
    await update.message.reply_text(
        "📄 Пришлите CSV файл (как документ). Можно указать /uploadstands truncate для полной перезагрузки."
    )


async def uploadschedule_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    args = [arg.lower() for arg in (context.args or [])]
    dry_run = any(arg in {"dry-run", "dry", "--dry-run"} for arg in args)
    keep_existing = any(arg in {"keep", "append", "--keep"} for arg in args)

    if update.message.reply_to_message and update.message.reply_to_message.document:
        _pop_pending_upload(context.user_data)
        await process_schedule_document(
            update.message.reply_to_message.document,
            update.message,
            keep_existing=keep_existing,
            dry_run=dry_run,
        )
        return

    _set_pending_upload(
        context.user_data,
        "schedule",
        truncate=False,
        keep_existing=keep_existing,
        dry_run=dry_run,
    )
    state = _get_pending_upload(context.user_data) or {}
    await update.message.reply_text(
        UPLOADSCHEDULE_PROMPT,
        reply_markup=build_uploadschedule_keyboard(state),
    )


async def downloadclients_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    await update.message.reply_text("Готовлю выгрузку клиентов…")
    try:
        csv_bytes = await asyncio.to_thread(export_clients_to_csv_bytes)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to export clients CSV")
        await update.message.reply_text(f"❌ Ошибка выгрузки: {exc}")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"clients_{timestamp}.csv"
    await update.message.reply_document(
        document=csv_bytes,
        filename=file_name,
        caption="📥 Клиенты выгружены в формате загрузки.",
    )


async def uploadworkout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message:
        return
    if not _ensure_admin_message(update):
        return

    tokens = context.args or []
    account_ids, missing = resolve_account_tokens(tokens)
    if missing:
        await update.message.reply_text(
            "⚠️ Не удалось распознать аккаунты: {0}".format(", ".join(missing))
        )
        return

    reply_document = None
    if update.message.reply_to_message and update.message.reply_to_message.document:
        reply_document = update.message.reply_to_message.document

    if reply_document and (reply_document.file_name or "").lower().endswith(".zwo"):
        if not account_ids:
            await update.message.reply_text(
                "ℹ️ Укажите аккаунт или all, например: /uploadworkout all (в ответ на файл)."
            )
            return
        _pop_pending_workout_upload(context.user_data)
        await process_workout_document(reply_document, update.message, account_ids)
        _clear_pending_workout_file(context.user_data)
        return

    if account_ids:
        if len(account_ids) == len(_account_registry):
            target = "все аккаунты"
        elif len(account_ids) == 1:
            target = _account_registry[account_ids[0]].name
        else:
            target = ", ".join(
                _account_registry[acc].name for acc in account_ids if acc in _account_registry
            )
        pending_file = _get_pending_workout_file(context.user_data)
        if pending_file and pending_file.get("data"):
            try:
                await process_workout_bytes(
                    raw_bytes=bytes(pending_file.get("data")),
                    file_name=pending_file.get("file_name") or "",
                    account_ids=account_ids,
                    reply_func=update.message.reply_text,
                )
            finally:
                _clear_pending_workout_file(context.user_data)
                _pop_pending_workout_upload(context.user_data)
            return
        _pop_pending_workout_upload(context.user_data)
        _set_pending_workout_upload(context.user_data, account_ids)
        await update.message.reply_text(
            f"📄 Пришлите файл тренировки ZWO для загрузки в {target}."
        )
        return

    await _show_account_selection(message=update.message, kind="workout")


async def workout_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return
    if not _ensure_admin_message(update):
        return

    document = update.message.document
    file_name = (document.file_name or "").lower()
    if not file_name.endswith(".zwo"):
        return

    caption = update.message.caption or ""
    command = None
    args: List[str] = []
    stripped = caption.strip()
    if stripped.startswith("/"):
        parts = stripped.split()
        command = parts[0].lower()
        args = parts[1:]

    account_ids: List[str] = []
    if command == WORKOUT_UPLOAD_COMMAND:
        account_ids, missing = resolve_account_tokens(args)
        if missing:
            await update.message.reply_text(
                "⚠️ Не удалось распознать аккаунты: {0}".format(", ".join(missing))
            )
            return
        _pop_pending_workout_upload(context.user_data)
    else:
        pending = _pop_pending_workout_upload(context.user_data)
        if pending:
            account_ids = list(pending.get("account_ids") or [])

    if account_ids:
        await process_workout_document(document, update.message, account_ids)
        _clear_pending_workout_file(context.user_data)
        return

    try:
        file = await document.get_file()
        data = await file.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Failed to download workout file")
        await update.message.reply_text(f"⚠️ Не удалось скачать файл: {exc}")
        return

    file_label = document.file_name or document.file_unique_id or "тренировка.zwo"
    _set_pending_workout_file(
        context.user_data,
        data=bytes(data),
        file_name=file_label,
        chat_id=update.message.chat_id,
        reply_to_message_id=update.message.message_id,
    )

    await _show_account_selection(message=update.message, kind="workout")


async def document_upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _require_setup()
    if not update.message or not update.message.document:
        return
    if not _ensure_admin_message(update):
        return

    document = update.message.document
    caption = update.message.caption or ""
    command = None
    args: List[str] = []

    stripped = caption.strip()
    if stripped.startswith("/"):
        parts = stripped.split()
        command = parts[0].lower()
        args = parts[1:]

    upload_type: Optional[str] = None
    truncate = False
    update_mode = True
    keep_existing_schedule = False
    dry_run_schedule = False
    dry_run_clients = False

    if command and command in UPLOAD_COMMAND_TYPES:
        upload_type = UPLOAD_COMMAND_TYPES[command]
        truncate = any(arg.lower() in {"truncate", "--truncate"} for arg in args)
        if truncate:
            update_mode = False
        if any(arg.lower() in {"update", "--update"} for arg in args):
            update_mode = True
        if upload_type == "schedule":
            keep_existing_schedule = any(
                arg.lower() in {"keep", "--keep", "append"} for arg in args
            )
            dry_run_schedule = any(
                arg.lower() in {"dry-run", "dry", "--dry-run"} for arg in args
            )
        if upload_type == "clients":
            dry_run_clients = any(arg.lower() in {"dry-run", "dry", "--dry-run"} for arg in args)
        _pop_pending_upload(context.user_data)
    else:
        pending = _pop_pending_upload(context.user_data)
        if pending:
            upload_type = pending.get("type")
            truncate = pending.get("truncate", False)
            update_mode = pending.get("update", True)
            keep_existing_schedule = pending.get("keep_existing", False)
            dry_run_schedule = pending.get("dry_run", False)
            dry_run_clients = pending.get("dry_run", False)

    if upload_type == "clients":
        await process_clients_document(
            document,
            update.message,
            truncate,
            update_mode,
            dry_run=dry_run_clients,
        )
    elif upload_type == "bikes":
        await process_bikes_document(document, update.message, truncate)
    elif upload_type == "stands":
        await process_trainers_document(document, update.message, truncate)
    elif upload_type == "schedule":
        await process_schedule_document(
            document,
            update.message,
            keep_existing=keep_existing_schedule,
            dry_run=dry_run_schedule,
        )
    else:
        await update.message.reply_text(
            "ℹ️ Чтобы импортировать данные, используйте /uploadclients, /uploadbikes, /uploadstands, "
            "/uploadschedule, /uploadworkout или добавьте команду в подпись к файлу."
        )


async def handle_upload_callback(action: str, parts: List[str], query, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if action == "uploadclients_mode" and len(parts) >= 2:
        await handle_uploadclients_mode(query, context, parts[1])
        return True
    if action == "uploadschedule_mode" and len(parts) >= 2:
        await handle_uploadschedule_mode(query, context, parts[1])
        return True
    if action == "workout_select" and len(parts) >= 2:
        target = parts[1]
        if target.upper() == "ALL":
            account_ids = list(_account_registry.keys())
            if not account_ids:
                await query.edit_message_text("⚠️ Аккаунты не настроены.")
                return True
            label = "все аккаунты"
        else:
            account_id = resolve_account_identifier_value(_account_registry, target)
            if account_id is None:
                await query.edit_message_text("⚠️ Аккаунт не найден.")
                return True
            account_ids = [account_id]
            label = _account_registry[account_id].name
        pending_file = _get_pending_workout_file(context.user_data)
        if pending_file and pending_file.get("data"):
            await query.edit_message_text(f"📤 Загружаю тренировку в {label}…")
            reply_func = _make_reply_func(
                context.bot,
                pending_file.get("chat_id") or query.message.chat_id,
                pending_file.get("reply_to_message_id"),
            )
            try:
                await process_workout_bytes(
                    raw_bytes=bytes(pending_file.get("data")),
                    file_name=pending_file.get("file_name") or "",
                    account_ids=account_ids,
                    reply_func=reply_func,
                )
            finally:
                _clear_pending_workout_file(context.user_data)
                _pop_pending_workout_upload(context.user_data)
            return True
        _set_pending_workout_upload(context.user_data, account_ids)
        await query.edit_message_text(
            f"📄 Пришлите файл тренировки ZWO для загрузки в {label}."
        )
        return True
    return False


def register_upload_handlers(application) -> None:
    application.add_handler(CommandHandler("uploadclients", uploadclients_handler))
    application.add_handler(CommandHandler("downloadclients", downloadclients_handler))
    application.add_handler(CommandHandler("uploadbikes", uploadbikes_handler))
    application.add_handler(CommandHandler("uploadstands", uploadstands_handler))
    application.add_handler(CommandHandler("uploadschedule", uploadschedule_handler))
    application.add_handler(CommandHandler("uploadworkout", uploadworkout_handler))
    workout_filter = filters.Document.FileExtension("zwo") | filters.Document.FileExtension("ZWO")
    application.add_handler(MessageHandler(workout_filter, workout_document_handler))
    csv_filter = (
        filters.Document.MimeType("text/csv")
        | filters.Document.FileExtension("csv")
        | filters.Document.FileExtension("CSV")
    )
    xlsx_filter = (
        filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        | filters.Document.FileExtension("xlsx")
        | filters.Document.FileExtension("XLSX")
    )
    document_filter = csv_filter | xlsx_filter
    application.add_handler(MessageHandler(document_filter, document_upload_handler))
