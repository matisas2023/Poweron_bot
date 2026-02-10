import asyncio
import logging
import os
import time

import telebot
from telebot import types

from poweron_bot.wizard import PowerOnWizard


def load_token_from_file(path="bot_token.txt"):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as token_file:
        content = token_file.read().strip()
        return content or None


def parse_allowed_ids(raw_value: str):
    ids = set()
    for part in (raw_value or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids


def parse_admin_id(raw_value: str):
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def setup_user_logger() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    user_logger = logging.getLogger("poweron_user_entries")
    user_logger.setLevel(logging.INFO)
    user_logger.propagate = False
    if not user_logger.handlers:
        handler = logging.FileHandler("logs/user_entries.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        user_logger.addHandler(handler)
    return user_logger


def admin_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("📊 /stats", callback_data="admin:stats"))
    kb.add(types.InlineKeyboardButton("🩺 /health", callback_data="admin:health"))
    kb.add(types.InlineKeyboardButton("📣 /broadcast", callback_data="admin:broadcast"))
    return kb


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    token = os.getenv("POWERON_BOT_TOKEN") or load_token_from_file("poweron_bot_token.txt")
    if not token:
        raise RuntimeError("Set POWERON_BOT_TOKEN or create poweron_bot_token.txt")

    admin_id_raw = os.getenv("POWERON_ADMIN_USER_ID") or load_token_from_file("poweron_admin_user_id.txt")
    admin_user_id = parse_admin_id(admin_id_raw)

    allowed_ids = parse_allowed_ids(os.getenv("POWERON_ALLOWED_IDS", ""))
    bot = telebot.TeleBot(token)
    wizard = PowerOnWizard(bot)
    user_logger = setup_user_logger()
    admin_broadcast_pending = set()

    def is_allowed(message):
        user_id = getattr(message.from_user, "id", None)
        if allowed_ids:
            return user_id in allowed_ids
        return True

    def is_admin(user_id: int) -> bool:
        return admin_user_id is not None and user_id == admin_user_id

    def build_stats_text() -> str:
        wizard._load_users_payload()
        users_total = len(wizard._users_payload)
        active_auto = sum(1 for item in wizard.auto_update.values() if item.get("enabled"))
        return (
            "📊 Статистика бота:\n"
            f"• Користувачів у базі: {users_total}\n"
            f"• Активних автооновлень: {active_auto}\n"
            f"• Поточних in-memory станів: {len(wizard.state)}"
        )

    def build_health_text() -> str:
        api_ok = False
        api_error = None
        try:
            items = asyncio.run(wizard.client.search_settlements("а", limit=1))
            api_ok = isinstance(items, list)
        except Exception as exc:
            api_error = str(exc)

        cache_ok = os.path.isdir(wizard.client.cache_dir)
        return (
            "🩺 Health check:\n"
            f"• API: {'✅ OK' if api_ok else '❌ FAIL'}\n"
            f"• Cache dir: {'✅ OK' if cache_ok else '❌ FAIL'} ({wizard.client.cache_dir})\n"
            f"• Polling restart loop: ✅ enabled\n"
            + (f"• API error: {api_error}" if api_error else "")
        )

    @bot.message_handler(commands=["start"])
    def cmd_start(message):
        user = message.from_user
        user_logger.info(
            "user_start chat_id=%s user_id=%s username=%s first_name=%s",
            message.chat.id,
            getattr(user, "id", None),
            getattr(user, "username", None),
            getattr(user, "first_name", None),
        )
        if not is_allowed(message):
            bot.send_message(message.chat.id, "⛔️ Доступ заборонено")
            return
        wizard.send_home(message.chat.id)
        if is_admin(message.from_user.id):
            bot.send_message(message.chat.id, "🛠 Адмін-меню:", reply_markup=admin_keyboard())

    @bot.message_handler(commands=["admin"])
    def cmd_admin(message):
        if not is_admin(message.from_user.id):
            return
        bot.send_message(message.chat.id, "🛠 Адмін-меню:", reply_markup=admin_keyboard())

    @bot.message_handler(commands=["stats"])
    def cmd_stats(message):
        if not is_admin(message.from_user.id):
            return
        bot.send_message(message.chat.id, build_stats_text())

    @bot.message_handler(commands=["health"])
    def cmd_health(message):
        if not is_admin(message.from_user.id):
            return
        bot.send_message(message.chat.id, build_health_text())

    @bot.message_handler(commands=["broadcast"])
    def cmd_broadcast(message):
        if not is_admin(message.from_user.id):
            return
        admin_broadcast_pending.add(message.chat.id)
        bot.send_message(message.chat.id, "📣 Введіть текст для розсилки всім користувачам:")

    @bot.message_handler(func=lambda m: True)
    def on_message(message):
        if not is_allowed(message):
            bot.send_message(message.chat.id, "⛔️ Доступ заборонено")
            return

        if is_admin(message.from_user.id) and message.chat.id in admin_broadcast_pending:
            text = (message.text or "").strip()
            if not text:
                bot.send_message(message.chat.id, "Повідомлення порожнє. Введіть текст знову:")
                return
            admin_broadcast_pending.discard(message.chat.id)

            wizard._load_users_payload()
            sent = 0
            for chat_id_str in wizard._users_payload.keys():
                try:
                    chat_id = int(chat_id_str)
                except ValueError:
                    continue
                try:
                    bot.send_message(chat_id, f"📣 Повідомлення від адміністратора:\n\n{text}")
                    sent += 1
                except Exception:
                    continue
            bot.send_message(message.chat.id, f"✅ Розсилку завершено. Надіслано: {sent}")
            return

        if wizard.handle_message(message):
            return
        if (message.text or "").strip().lower() in {"/start", "start", "старт", "🚀 старт"}:
            wizard.send_home(message.chat.id)

    @bot.callback_query_handler(func=lambda call: True)
    def on_callback(call):
        if allowed_ids and call.from_user.id not in allowed_ids:
            return

        if call.data == "admin:stats" and is_admin(call.from_user.id):
            bot.send_message(call.message.chat.id, build_stats_text())
            return
        if call.data == "admin:health" and is_admin(call.from_user.id):
            bot.send_message(call.message.chat.id, build_health_text())
            return
        if call.data == "admin:broadcast" and is_admin(call.from_user.id):
            admin_broadcast_pending.add(call.message.chat.id)
            bot.send_message(call.message.chat.id, "📣 Введіть текст для розсилки всім користувачам:")
            return

        if wizard.handle_callback(call):
            return

    retry_delay_seconds = 5
    while True:
        try:
            bot.infinity_polling(skip_pending=True)
        except Exception as exc:
            logging.exception("Bot polling crashed, restarting in %s seconds. error=%s", retry_delay_seconds, exc)
            time.sleep(retry_delay_seconds)


if __name__ == "__main__":
    main()
