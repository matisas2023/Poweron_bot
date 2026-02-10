import logging
import os

import telebot

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


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    token = os.getenv("POWERON_BOT_TOKEN") or load_token_from_file("../poweron_bot_token.txt")
    if not token:
        raise RuntimeError("Set POWERON_BOT_TOKEN or create poweron_bot_token.txt")

    allowed_ids = parse_allowed_ids(os.getenv("POWERON_ALLOWED_IDS", ""))
    bot = telebot.TeleBot(token)
    wizard = PowerOnWizard(bot)

    def is_allowed(message):
        if not allowed_ids:
            return True
        user_id = getattr(message.from_user, "id", None)
        return user_id in allowed_ids

    @bot.message_handler(commands=["start"])
    def cmd_start(message):
        if not is_allowed(message):
            bot.send_message(message.chat.id, "⛔️ Доступ заборонено")
            return
        wizard.send_home(message.chat.id)

    @bot.message_handler(func=lambda m: True)
    def on_message(message):
        if not is_allowed(message):
            return
        if wizard.handle_message(message):
            return
        if (message.text or "").strip().lower() in {"/start", "start", "старт", "🚀 старт"}:
            wizard.send_home(message.chat.id)

    @bot.callback_query_handler(func=lambda call: True)
    def on_callback(call):
        if allowed_ids and call.from_user.id not in allowed_ids:
            return
        if wizard.handle_callback(call):
            return

    bot.infinity_polling(skip_pending=True)


if __name__ == "__main__":
    main()
