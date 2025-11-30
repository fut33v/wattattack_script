from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable, Dict, Iterable, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from adminbot.accounts import AccountConfig, load_accounts
from repositories import client_groups_repository
from wattattack_profiles import apply_client_profile as apply_wattattack_profile

SELF_SERVICE_GROUP_NAME = "САМОКРУТЧИКИ"
ACCOUNTS_CONFIG_PATH = Path(Path.cwd() / "accounts.json")

_EMOJI_DIGITS: Dict[int, str] = {
    1: "1️⃣",
    2: "2️⃣",
    3: "3️⃣",
    4: "4️⃣",
    5: "5️⃣",
    6: "6️⃣",
    7: "7️⃣",
    8: "8️⃣",
    9: "9️⃣",
    10: "🔟",
}


def _format_account_label(account: AccountConfig, index: Optional[int] = None) -> str:
    base_label = account.identifier or account.name
    prefix = _EMOJI_DIGITS.get(index, "") if index is not None else ""
    return f"{prefix} {base_label}".strip()


def _build_self_service_keyboard(accounts: Dict[str, AccountConfig]) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                _format_account_label(accounts[account_id], idx),
                callback_data=f"self_service:account:{accounts[account_id].identifier}",
            )
        ]
        for idx, account_id in enumerate(sorted(accounts), start=1)
    ]
    rows.append([InlineKeyboardButton("⚡️ Применить на все аккаунты", callback_data="self_service:all")])
    rows.append([InlineKeyboardButton("🔙 В главное меню", callback_data="self_service:back")])
    return InlineKeyboardMarkup(rows)


class SelfServiceFlow:
    def __init__(
        self,
        fetch_linked_client: Callable[[int], tuple[Optional[dict], Optional[dict]]],
        send_main_menu: Callable[[ContextTypes.DEFAULT_TYPE, int, str], asyncio.Future],
        *,
        accounts_path: Path = ACCOUNTS_CONFIG_PATH,
    ):
        self._fetch_linked_client = fetch_linked_client
        self._send_main_menu = send_main_menu
        self._accounts_path = accounts_path
        self._accounts_cache: Dict[str, AccountConfig] = {}

    def has_access(self, user_id: Optional[int], client: Optional[dict]) -> bool:
        client_id = client.get("id") if isinstance(client, dict) else None
        if client_id is None and user_id is not None:
            _, linked_client = self._fetch_linked_client(user_id)
            client_id = linked_client.get("id") if isinstance(linked_client, dict) else None
        if not isinstance(client_id, int):
            return False
        try:
            return client_groups_repository.is_client_in_group(client_id, SELF_SERVICE_GROUP_NAME)
        except Exception:
            return False

    def _load_accounts_registry(self, context: ContextTypes.DEFAULT_TYPE) -> Dict[str, AccountConfig]:
        if self._accounts_cache:
            return self._accounts_cache
        try:
            accounts = load_accounts(self._accounts_path)
        except Exception:
            return {}
        self._accounts_cache = accounts
        context.application.bot_data["clientbot:accounts"] = accounts
        return accounts

    async def show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        message = update.effective_message
        user = update.effective_user
        if message is None or user is None:
            return

        link, client = self._fetch_linked_client(user.id)
        if not link or not client:
            await message.reply_text("Сначала привяжите свою анкету через /start.")
            return
        if not self.has_access(user.id, client):
            if query is not None:
                await query.answer("Доступно только участникам группы «САМОКРУТЧИКИ».", show_alert=True)
            else:
                await message.reply_text("Доступно только участникам группы «САМОКРУТЧИКИ».")
            return

        accounts = self._load_accounts_registry(context)
        if not accounts:
            await message.reply_text("Аккаунты WattAttack не настроены. Сообщите администратору.")
            return

        text = (
            "⚠️ Внимание: устанавливайте свои данные на станок только когда вы уже находитесь в Крутилке.\n"
            "Нажмите «Хорошо, я понял», чтобы выбрать аккаунт."
        )
        keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Хорошо, я понял", callback_data="self_service:ack")],
                [InlineKeyboardButton("🔙 В главное меню", callback_data="self_service:back")],
            ]
        )
        try:
            if query is not None and query.message is not None:
                await query.edit_message_text(text, reply_markup=keyboard)
            else:
                await message.reply_text(text, reply_markup=keyboard)
        except Exception:
            if message is not None:
                await context.bot.send_message(message.chat_id, text, reply_markup=keyboard)

    async def handle_ack(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        user = update.effective_user
        if user is None:
            return

        link, client = self._fetch_linked_client(user.id)
        if not link or not client:
            await query.edit_message_text("Сначала привяжите свою анкету через /start.")
            return
        if not self.has_access(user.id, client):
            await query.answer("Самокрутка доступна только участникам группы «САМОКРУТЧИКИ».", show_alert=True)
            return

        accounts = self._load_accounts_registry(context)
        if not accounts:
            await query.edit_message_text("Аккаунты WattAttack не настроены. Сообщите администратору.")
            return

        text = (
            "🔄 Самокрутка\n"
            "Выберите аккаунт WattAttack, на который вас посадить.\n"
            "Не забудьте перезагрузить WattAttack после применения аккаунта!"
        )
        keyboard = _build_self_service_keyboard(accounts)
        try:
            await query.edit_message_text(text, reply_markup=keyboard)
        except Exception:
            chat_id = query.message.chat_id if query.message else user.id
            if chat_id is not None:
                await context.bot.send_message(chat_id, text, reply_markup=keyboard)

    async def handle_account_choice(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        parts = (query.data or "").split(":", 2)
        if len(parts) != 3:
            await query.answer("Неизвестный аккаунт.", show_alert=True)
            return

        account_id = parts[2]
        user = update.effective_user
        if user is None:
            return

        link, client = self._fetch_linked_client(user.id)
        if not link or not client:
            await query.edit_message_text("Сначала привяжите свою анкету через /start.")
            return
        if not self.has_access(user.id, client):
            await query.answer("Самокрутка доступна только участникам группы «САМОКРУТЧИКИ».", show_alert=True)
            return

        accounts = self._load_accounts_registry(context)
        account = accounts.get(account_id)
        if account is None:
            await query.edit_message_text(
                "Аккаунт недоступен. Попробуйте выбрать другой.",
                reply_markup=_build_self_service_keyboard(accounts),
            )
            return

        progress_text = f"⏳ Сажаем на {_format_account_label(account)}..."
        try:
            await query.edit_message_text(progress_text)
        except Exception:
            pass

        target_chat = query.message.chat_id if query.message else user.id
        try:
            await asyncio.to_thread(
                apply_wattattack_profile,
                account_id=account.identifier,
                account_label=account.name,
                email=account.email,
                password=account.password,
                base_url=account.base_url,
                client_record=client,
            )
            success_text = (
                f"✅ Данные {self._format_client_display_name(client)} применены к аккаунту {_format_account_label(account)}."
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В главное меню", callback_data="self_service:back")]])
            try:
                await query.edit_message_text(success_text, reply_markup=markup)
            except Exception:
                if target_chat is not None:
                    await context.bot.send_message(target_chat, success_text, reply_markup=markup)
        except Exception:
            error_text = (
                f"❌ Не удалось посадить на аккаунт {_format_account_label(account)}. "
                "Попробуйте ещё раз позже или обратитесь к администратору."
            )
            markup = _build_self_service_keyboard(accounts)
            try:
                await query.edit_message_text(error_text, reply_markup=markup)
            except Exception:
                if target_chat is not None:
                    await context.bot.send_message(target_chat, error_text, reply_markup=markup)

    async def handle_apply_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        user = update.effective_user
        if user is None:
            return

        link, client = self._fetch_linked_client(user.id)
        if not link or not client:
            await query.edit_message_text("Сначала привяжите свою анкету через /start.")
            return
        if not self.has_access(user.id, client):
            await query.answer("Самокрутка доступна только участникам группы «САМОКРУТЧИКИ».", show_alert=True)
            return

        accounts = self._load_accounts_registry(context)
        if not accounts:
            await query.edit_message_text("Аккаунты WattAttack не настроены. Сообщите администратору.")
            return

        progress_text = f"⏳ Сажаем на все аккаунты ({len(accounts)} шт.)..."
        try:
            await query.edit_message_text(progress_text)
        except Exception:
            pass

        successes: list[str] = []
        failures: list[str] = []
        for account in accounts.values():
            try:
                await asyncio.to_thread(
                    apply_wattattack_profile,
                    account_id=account.identifier,
                    account_label=account.name,
                    email=account.email,
                    password=account.password,
                    base_url=account.base_url,
                    client_record=client,
                )
                successes.append(_format_account_label(account))
            except Exception:
                failures.append(_format_account_label(account))

        lines = ["✅ Данные применены ко всем аккаунтам."]
        if successes:
            lines.append("Обновлены: " + ", ".join(successes))
        if failures:
            lines.append("Не удалось: " + ", ".join(failures))

        markup = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 В главное меню", callback_data="self_service:back")]])
        text = "\n".join(lines)
        try:
            await query.edit_message_text(text, reply_markup=markup)
        except Exception:
            chat_id = query.message.chat_id if query.message else user.id
            if chat_id is not None:
                await context.bot.send_message(chat_id, text, reply_markup=markup)

    async def handle_back(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        chat_id = query.message.chat_id if query.message else query.from_user.id if query.from_user else None
        user_id = query.from_user.id if query.from_user else None
        if chat_id is not None:
            await self._send_main_menu(context, chat_id, "Выберите действие:")

    @staticmethod
    def _format_client_display_name(client: dict) -> str:
        first_name = (client.get("first_name") or "").strip()
        last_name = (client.get("last_name") or "").strip()
        full_name = (client.get("full_name") or "").strip()
        if first_name and last_name:
            return f"{first_name} {last_name}".strip()
        if full_name:
            return full_name
        return last_name or first_name or "Клиент"

    def register_handlers(self, application: Application) -> None:
        application.add_handler(CallbackQueryHandler(self.handle_ack, pattern=r"^self_service:ack$"), group=-1)
        application.add_handler(CallbackQueryHandler(self.handle_account_choice, pattern=r"^self_service:account:[^:]+$"), group=-1)
        application.add_handler(CallbackQueryHandler(self.handle_apply_all, pattern=r"^self_service:all$"), group=-1)
        application.add_handler(CallbackQueryHandler(self.handle_back, pattern=r"^self_service:back$"), group=-1)
