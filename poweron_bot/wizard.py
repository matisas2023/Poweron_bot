import asyncio
import json
import logging
import os
from typing import Dict, Optional

from telebot import types

from poweron_bot.client import PowerOnClient, PowerOnClientError


class PowerOnWizard:
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("poweron_standalone")
        self.client = PowerOnClient()
        self.state: Dict[int, dict] = {}
        self.history: Dict[int, list] = {}
        self.pinned: Dict[int, list] = {}
        self.seen_users = set()
        self.user_data_file = "data/users.json"
        os.makedirs("data", exist_ok=True)
        self._users_payload = {}

    def _load_users_payload(self):
        if self._users_payload:
            return
        if not os.path.exists(self.user_data_file):
            self._users_payload = {}
            return
        try:
            with open(self.user_data_file, "r", encoding="utf-8") as users_file:
                payload = json.load(users_file)
            self._users_payload = payload if isinstance(payload, dict) else {}
        except Exception as exc:
            self.logger.exception("poweron.user_data_load_failed error=%s", exc)
            self._users_payload = {}

    def _save_users_payload(self):
        try:
            with open(self.user_data_file, "w", encoding="utf-8") as users_file:
                json.dump(self._users_payload, users_file, ensure_ascii=False, indent=2)
        except Exception as exc:
            self.logger.exception("poweron.user_data_save_failed error=%s", exc)

    def _save_user_data(self, chat_id: int):
        self._load_users_payload()
        payload_key = str(chat_id)
        self._users_payload[payload_key] = {
            "seen": chat_id in self.seen_users,
            "history": self.history.get(chat_id, [])[:3],
            "pinned": self.pinned.get(chat_id, [])[:3],
        }
        self._save_users_payload()

    def _ensure_user_loaded(self, chat_id: int):
        if chat_id in self.history and chat_id in self.pinned:
            return
        self._load_users_payload()
        user_payload = self._users_payload.get(str(chat_id), {})
        self.history[chat_id] = user_payload.get("history", [])[:3]
        self.pinned[chat_id] = user_payload.get("pinned", [])[:3]
        if user_payload.get("seen"):
            self.seen_users.add(chat_id)

    def _nav_keyboard(self) -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("◀️ Назад", callback_data="poweron:back"),
            types.InlineKeyboardButton("🔄 Почати заново", callback_data="poweron:reset"),
            types.InlineKeyboardButton("🏠 Головна", callback_data="poweron:home"),
        )
        return kb

    def _home_keyboard(self):
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
        kb.add(types.KeyboardButton("💡 Графік світла (за адресою)"))
        kb.add(types.KeyboardButton("🏠 Головна"))
        return kb

    @staticmethod
    def _address_caption(item: dict) -> str:
        settlement_name = item.get("settlement_display") or item.get("settlement_name", "")
        return f"{settlement_name}, {item['street_name']}, {item['house_name']}"

    def _quick_access_keyboard(self, chat_id: int) -> Optional[types.InlineKeyboardMarkup]:
        self._ensure_user_loaded(chat_id)
        pinned = self.pinned.get(chat_id, [])
        history = self.history.get(chat_id, [])
        if not pinned and not history:
            return None

        kb = types.InlineKeyboardMarkup(row_width=1)
        for idx, item in enumerate(pinned[:3]):
            kb.add(types.InlineKeyboardButton(f"📌 {self._address_caption(item)}", callback_data=f"poweron:pin_open:{idx}"))
        if history:
            kb.add(types.InlineKeyboardButton("🕘 Історія (останні 3)", callback_data="poweron:history"))
        return kb

    def _history_keyboard(self, chat_id: int) -> Optional[types.InlineKeyboardMarkup]:
        self._ensure_user_loaded(chat_id)
        history = self.history.get(chat_id, [])
        if not history:
            return None

        pinned_keys = {item["cache_key"] for item in self.pinned.get(chat_id, [])}
        kb = types.InlineKeyboardMarkup(row_width=1)
        for idx, item in enumerate(history[:3]):
            caption = self._address_caption(item)
            pin_title = "❌ Відкріпити" if item["cache_key"] in pinned_keys else "📌 Закріпити"
            kb.add(types.InlineKeyboardButton(f"🏠 {caption}", callback_data=f"poweron:hist_open:{idx}"))
            kb.add(types.InlineKeyboardButton(pin_title, callback_data=f"poweron:hist_pin:{idx}"))

        nav = self._nav_keyboard()
        for row in nav.keyboard:
            kb.keyboard.append(row)
        return kb

    def _options_keyboard(self, prefix: str, options: list):
        kb = types.InlineKeyboardMarkup(row_width=1)
        for option in options[:10]:
            kb.add(types.InlineKeyboardButton(option["name"], callback_data=f"poweron:{prefix}:{option['id']}"))
        nav = self._nav_keyboard()
        for row in nav.keyboard:
            kb.keyboard.append(row)
        return kb

    def _upsert_history(self, chat_id: int, item: dict):
        self._ensure_user_loaded(chat_id)
        history = self.history.setdefault(chat_id, [])
        history = [entry for entry in history if entry["cache_key"] != item["cache_key"]]
        history.insert(0, item)
        self.history[chat_id] = history[:3]
        self._save_user_data(chat_id)

    def _toggle_pin(self, chat_id: int, item: dict) -> str:
        self._ensure_user_loaded(chat_id)
        pinned = self.pinned.setdefault(chat_id, [])
        pinned_keys = {entry["cache_key"] for entry in pinned}
        if item["cache_key"] in pinned_keys:
            self.pinned[chat_id] = [entry for entry in pinned if entry["cache_key"] != item["cache_key"]]
            self._save_user_data(chat_id)
            return "❌ Адресу відкріплено."
        pinned = [entry for entry in pinned if entry["cache_key"] != item["cache_key"]]
        pinned.insert(0, item)
        self.pinned[chat_id] = pinned[:3]
        self._save_user_data(chat_id)
        return "📌 Адресу закріплено."

    def send_home(self, chat_id: int):
        self._ensure_user_loaded(chat_id)
        if chat_id not in self.seen_users:
            self.seen_users.add(chat_id)
            self._save_user_data(chat_id)
            self.bot.send_message(
                chat_id,
                """👋 Вітаю! Це бот для перегляду графіків відключень електроенергії за вашою адресою.

Натисніть кнопку нижче, щоб почати пошук.""",
                reply_markup=self._home_keyboard(),
            )
            return

        self.bot.send_message(chat_id, "Окремий бот для графіків відключень.", reply_markup=self._home_keyboard())

    def start(self, chat_id: int):
        self._ensure_user_loaded(chat_id)
        self.state[chat_id] = {"step": "settlement_query"}
        extra_kb = self._quick_access_keyboard(chat_id)
        if extra_kb:
            self.bot.send_message(chat_id, "⚡ Швидкий доступ: закріплені та нещодавні адреси.", reply_markup=extra_kb)
        self.bot.send_message(chat_id, "🔎 Крок 1/3: Введіть 2–5 символів населеного пункту.", reply_markup=self._nav_keyboard())

    def handle_message(self, message) -> bool:
        chat_id = message.chat.id
        session = self.state.get(chat_id)
        text = (message.text or "").strip()

        if text == "💡 Графік світла (за адресою)":
            self.start(chat_id)
            return True

        if text == "🏠 Головна":
            self.state.pop(chat_id, None)
            self.send_home(chat_id)
            return True

        if not session:
            return False

        min_len = 1 if session.get("step") == "house_query" else 2
        if len(text) < min_len:
            hint = "1–5" if min_len == 1 else "2–5"
            self.bot.send_message(chat_id, f"Нічого не знайшов. Введіть {hint} символів і спробуйте ще раз.")
            return True

        try:
            if session["step"] == "settlement_query":
                options = asyncio.run(self.client.search_settlements(text))
                if not options:
                    self.bot.send_message(chat_id, "Нічого не знайшов. Введіть 2–5 символів і спробуйте ще раз.")
                    return True
                session["settlements"] = {str(item["id"]): item for item in options}
                session["step"] = "settlement_pick"
                self.bot.send_message(chat_id, "Оберіть населений пункт зі списку:", reply_markup=self._options_keyboard("set", options))
                return True

            if session["step"] == "street_query":
                options = asyncio.run(self.client.search_streets(session["settlement"]["id"], text))
                if not options:
                    self.bot.send_message(chat_id, "Нічого не знайшов. Введіть 2–5 символів і спробуйте ще раз.")
                    return True
                session["streets"] = {str(item["id"]): item for item in options}
                session["step"] = "street_pick"
                self.bot.send_message(chat_id, "Оберіть вулицю зі списку:", reply_markup=self._options_keyboard("str", options))
                return True

            if session["step"] == "house_query":
                options = asyncio.run(self.client.search_houses(session["settlement"]["id"], session["street"]["id"], text))
                if not options:
                    self.bot.send_message(chat_id, "Нічого не знайшов. Введіть 1–5 символів і спробуйте ще раз.")
                    return True
                session["houses"] = {str(item["id"]): item for item in options}
                session["step"] = "house_pick"
                self.bot.send_message(chat_id, "Оберіть будинок/корпус зі списку:", reply_markup=self._options_keyboard("hou", options))
                return True
        except Exception as exc:
            self.logger.exception("poweron.search_failed chat_id=%s error=%s", chat_id, exc)
            self.bot.send_message(chat_id, "Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/")
            return True

        return False

    def handle_callback(self, call) -> bool:
        data = call.data or ""
        if not data.startswith("poweron:"):
            return False

        chat_id = call.message.chat.id
        session = self.state.setdefault(chat_id, {"step": "settlement_query"})

        if data == "poweron:home":
            self.state.pop(chat_id, None)
            self.send_home(chat_id)
            return True
        if data in {"poweron:start", "poweron:reset"}:
            self.start(chat_id)
            return True
        if data == "poweron:back":
            self._go_back(chat_id)
            return True
        if data == "poweron:history":
            history_kb = self._history_keyboard(chat_id)
            if not history_kb:
                self.bot.send_message(chat_id, "Історія порожня. Спочатку перегляньте графік хоча б для однієї адреси.")
                return True
            self.bot.send_message(chat_id, "🕘 Останні 3 адреси. Можна відкрити або закріпити:", reply_markup=history_kb)
            return True

        try:
            if data.startswith("poweron:set:"):
                settlement = (session.get("settlements") or {}).get(data.split(":", 2)[2])
                if not settlement:
                    return True
                session["settlement"] = settlement
                session["step"] = "street_query"
                self.bot.send_message(chat_id, f"✅ Населений пункт: {settlement['name']}\n\n🔎 Крок 2/3: Введіть 2–5 символів вулиці.", reply_markup=self._nav_keyboard())
                return True

            if data.startswith("poweron:str:"):
                street = (session.get("streets") or {}).get(data.split(":", 2)[2])
                if not street:
                    return True
                session["street"] = street
                session["step"] = "house_query"
                self.bot.send_message(chat_id, f"✅ Вулиця: {street['name']}\n\n🔎 Крок 3/3: Введіть номер будинку/корпусу (1–5 символів).", reply_markup=self._nav_keyboard())
                return True

            if data.startswith("poweron:hou:"):
                house = (session.get("houses") or {}).get(data.split(":", 2)[2])
                if not house:
                    return True
                session["house"] = house
                self._send_schedule(chat_id)
                return True

            if data.startswith("poweron:hist_open:"):
                idx = int(data.rsplit(":", 1)[1])
                history = self.history.get(chat_id, [])
                if idx < len(history):
                    self._send_schedule(chat_id, history[idx])
                return True

            if data.startswith("poweron:pin_open:"):
                idx = int(data.rsplit(":", 1)[1])
                pinned = self.pinned.get(chat_id, [])
                if idx < len(pinned):
                    self._send_schedule(chat_id, pinned[idx])
                return True

            if data.startswith("poweron:hist_pin:"):
                idx = int(data.rsplit(":", 1)[1])
                history = self.history.get(chat_id, [])
                if idx < len(history):
                    status = self._toggle_pin(chat_id, history[idx])
                    self.bot.send_message(chat_id, status, reply_markup=self._history_keyboard(chat_id) or self._nav_keyboard())
                return True
        except Exception as exc:
            self.logger.exception("poweron.callback_failed chat_id=%s error=%s", chat_id, exc)
            self.bot.send_message(chat_id, "Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/")
            return True

        return True

    def _go_back(self, chat_id: int):
        session = self.state.get(chat_id)
        if not session:
            self.start(chat_id)
            return
        step = session.get("step")
        if step in {"settlement_query", "settlement_pick"}:
            self.start(chat_id)
            return
        if step in {"street_query", "street_pick"}:
            session["step"] = "settlement_query"
            self.bot.send_message(chat_id, "🔎 Крок 1/3: Введіть 2–5 символів населеного пункту.", reply_markup=self._nav_keyboard())
            return
        if step in {"house_query", "house_pick"}:
            session["step"] = "street_query"
            self.bot.send_message(chat_id, "🔎 Крок 2/3: Введіть 2–5 символів вулиці.", reply_markup=self._nav_keyboard())
            return
        self.start(chat_id)

    def _send_schedule(self, chat_id: int, address_item: Optional[dict] = None):
        session = self.state.get(chat_id)
        if not session and not address_item:
            return

        if address_item:
            settlement_render = address_item.get("settlement_render") or address_item.get("settlement_name")
            settlement_display = address_item.get("settlement_display") or address_item.get("settlement_name")
            street_name = address_item["street_name"]
            house_name = address_item["house_name"]
            cache_key = address_item["cache_key"]
            schedule = address_item.get("schedule") or {}
        else:
            settlement = session.get("settlement")
            street = session.get("street")
            house = session.get("house")
            if not settlement or not street or not house:
                return
            settlement_render = settlement.get("raw_name", settlement["name"])
            settlement_display = settlement["name"]
            street_name = street["name"]
            house_name = house["name"]
            cache_key = f"{settlement['id']}:{street['id']}:{house['id']}"
            schedule = house.get("schedule", {})

        try:
            self.bot.send_message(chat_id, "⏳ Очікуйте, формую та завантажую графік...")
            image_path = asyncio.run(self.client.render_schedule_screenshot(settlement_render, street_name, house_name, cache_key))
            with open(image_path, "rb") as image_file:
                self.bot.send_photo(chat_id, image_file, caption=f"Графік відключень для: {settlement_display}, {street_name}, {house_name} (джерело: poweron.toe.com.ua)")
            entry = {
                "cache_key": cache_key,
                "settlement_name": settlement_display,
                "settlement_display": settlement_display,
                "settlement_render": settlement_render,
                "street_name": street_name,
                "house_name": house_name,
                "schedule": schedule,
            }
            self._upsert_history(chat_id, entry)
            self.bot.send_message(
                chat_id,
                "Черги з API:\n"
                f"• ГПВ: {schedule.get('gpv', '—')}\n"
                f"• ГАВ: {schedule.get('gav', '—')}\n"
                f"• АЧР: {schedule.get('achr', '—')}\n"
                f"• ГВСП: {schedule.get('gvsp', '—')}\n"
                f"• СГАВ: {schedule.get('sgav', '—')}",
                reply_markup=self._quick_access_keyboard(chat_id) or self._nav_keyboard(),
            )
        except PowerOnClientError:
            self.bot.send_message(chat_id, "Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/")
        except Exception as exc:
            self.logger.exception("poweron.render_failed chat_id=%s error=%s", chat_id, exc)
            self.bot.send_message(chat_id, "Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/")
