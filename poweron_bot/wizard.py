import asyncio
import hashlib
import heapq
import json
import logging
import os
import threading
import time
from typing import Dict, List, Optional, Tuple

from telebot import types

from poweron_bot.client import PowerOnClient, PowerOnClientError
from poweron_bot.paths import DATA_DIR
from poweron_bot.storage import UserStateStore

MAX_HISTORY_ITEMS = 6
MAX_PINNED_ITEMS = 6
AUTO_UPDATE_FAILURE_THRESHOLD = 3
AUTO_UPDATE_COOLDOWN_SECONDS = 15 * 60


class PowerOnWizard:
    def __init__(self, bot):
        self.bot = bot
        self.logger = logging.getLogger("poweron_standalone")
        self.client = PowerOnClient()
        self.state: Dict[int, dict] = {}
        self.history: Dict[int, list] = {}
        self.pinned: Dict[int, list] = {}
        self.seen_users = set()

        self.auto_update: Dict[int, dict] = {}
        self.rate_limit: Dict[int, float] = {}

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.user_data_file = DATA_DIR / "users.json"
        self.user_data_backup_file = DATA_DIR / "users.json.bak"
        self.user_state_db_file = DATA_DIR / "users.sqlite"
        self.store = UserStateStore(self.user_state_db_file)
        self._users_payload = {}
        self._users_payload_lock = threading.Lock()

        self.feedback_file = DATA_DIR / "feedback.json"
        self._feedback_lock = threading.Lock()
        self._feedback_payload = {"entries": [], "ratings": {}}
        self._load_feedback_payload()

        self.feature_flags = {
            "analytics_enabled": True,
            "multi_address_auto": True,
            "quiet_hours_enabled": True,
            "text_mode_cooldown": True,
            "compare_enabled": True,
        }

        self.metrics = {
            "schedule_requests": 0,
            "schedule_success": 0,
            "schedule_failures": 0,
            "text_fallbacks": 0,
            "auto_update_runs": 0,
            "auto_update_notifications": 0,
            "last_render_ms": 0,
        }
        self._auto_update_heap = []

        self._auto_update_worker_started = False
        self._start_auto_update_worker()

    @staticmethod
    def _default_auto_update_settings() -> dict:
        return {
            "enabled": False,
            "interval": 60,
            "silent": True,
            "last_signature": "",
            "last_signatures": {},
            "selected_keys": [],
            "next_run_ts": 0,
            "quiet_hours": {"enabled": True, "start": 23, "end": 7},
            "max_per_hour": 4,
            "notify_timestamps": [],
            "failures": 0,
            "text_mode_until": 0,
        }

    # ---------------------- feedback/rating ----------------------
    def _load_feedback_payload(self):
        with self._feedback_lock:
            if self.feedback_file.exists():
                try:
                    with self.feedback_file.open("r", encoding="utf-8") as feedback_file:
                        payload = json.load(feedback_file)
                    if isinstance(payload, dict):
                        self._feedback_payload = {
                            "entries": payload.get("entries") or [],
                            "ratings": payload.get("ratings") or {},
                        }
                except Exception as exc:
                    self.logger.exception("poweron.feedback_load_failed error=%s", exc)

    def _save_feedback_payload(self):
        tmp_path = self.feedback_file.with_suffix(".json.tmp")
        with self._feedback_lock:
            try:
                with tmp_path.open("w", encoding="utf-8") as feedback_file:
                    json.dump(self._feedback_payload, feedback_file, ensure_ascii=False, indent=2)
                os.replace(tmp_path, self.feedback_file)
            except Exception as exc:
                self.logger.exception("poweron.feedback_save_failed error=%s", exc)

    def add_feedback_entry(self, chat_id: int, text: str, username: str = "", first_name: str = ""):
        clean_text = (text or "").strip()
        if not clean_text:
            return
        with self._feedback_lock:
            entries = self._feedback_payload.setdefault("entries", [])
            entries.append({
                "chat_id": int(chat_id),
                "username": username or "",
                "first_name": first_name or "",
                "text": clean_text[:1500],
                "created_at": int(time.time()),
            })
            self._feedback_payload["entries"] = entries[-500:]
        self._save_feedback_payload()

    def set_user_rating(self, chat_id: int, rating: int):
        rating = max(1, min(5, int(rating)))
        with self._feedback_lock:
            ratings = self._feedback_payload.setdefault("ratings", {})
            ratings[str(chat_id)] = {"rating": rating, "updated_at": int(time.time())}
        self._save_feedback_payload()

    def get_feedback_entries(self) -> List[dict]:
        with self._feedback_lock:
            entries = list(self._feedback_payload.get("entries") or [])
        return entries

    def get_rating_summary(self) -> dict:
        with self._feedback_lock:
            ratings = self._feedback_payload.get("ratings") or {}
            values = [int((item or {}).get("rating", 0) or 0) for item in ratings.values()]
        values = [v for v in values if 1 <= v <= 5]
        if not values:
            return {"count": 0, "average": 0.0, "distribution": {str(i): 0 for i in range(1, 6)}}
        distribution = {str(i): 0 for i in range(1, 6)}
        for v in values:
            distribution[str(v)] += 1
        return {"count": len(values), "average": round(sum(values) / len(values), 2), "distribution": distribution}

    # ---------------------- persistence ----------------------
    def _load_users_payload(self):
        with self._users_payload_lock:
            if self._users_payload:
                return

            if not self.user_data_file.exists():
                self._users_payload = {}
                return

            try:
                with self.user_data_file.open("r", encoding="utf-8") as users_file:
                    payload = json.load(users_file)
                self._users_payload = payload if isinstance(payload, dict) else {}
                if self._users_payload:
                    self.store.replace_all(self._users_payload)
            except Exception as exc:
                self.logger.exception("poweron.user_data_load_failed error=%s", exc)
                # restore from backup if possible
                if self.user_data_backup_file.exists():
                    try:
                        with self.user_data_backup_file.open("r", encoding="utf-8") as users_file:
                            payload = json.load(users_file)
                        self._users_payload = payload if isinstance(payload, dict) else {}
                        if self._users_payload:
                            self.store.replace_all(self._users_payload)
                        return
                    except Exception as backup_exc:
                        self.logger.exception("poweron.user_data_backup_load_failed error=%s", backup_exc)
                self._users_payload = self.store.load_all()

    def _save_users_payload(self):
        tmp_path = self.user_data_file.with_suffix(".json.tmp")
        with self._users_payload_lock:
            try:
                with tmp_path.open("w", encoding="utf-8") as users_file:
                    json.dump(self._users_payload, users_file, ensure_ascii=False, indent=2)

                if self.user_data_file.exists():
                    with self.user_data_file.open("r", encoding="utf-8") as src, self.user_data_backup_file.open("w", encoding="utf-8") as dst:
                        dst.write(src.read())

                os.replace(tmp_path, self.user_data_file)
                self.store.replace_all(self._users_payload)
            except Exception as exc:
                self.logger.exception("poweron.user_data_save_failed error=%s", exc)
                try:
                    if tmp_path.exists():
                        os.remove(tmp_path)
                except OSError:
                    pass

    def _save_user_data(self, chat_id: int):
        self._load_users_payload()
        payload_key = str(chat_id)
        self._users_payload[payload_key] = {
            "seen": chat_id in self.seen_users,
            "history": self.history.get(chat_id, [])[:MAX_HISTORY_ITEMS],
            "pinned": self.pinned.get(chat_id, [])[:MAX_PINNED_ITEMS],
            "auto_update": self.auto_update.get(chat_id, self._default_auto_update_settings()),
        }
        self.store.upsert_chat(chat_id, self._users_payload[payload_key])
        self._save_users_payload()

    def _hydrate_users_cache_from_payload(self):
        self._load_users_payload()
        for chat_key, user_payload in self._users_payload.items():
            try:
                chat_id = int(chat_key)
            except (TypeError, ValueError):
                continue

            self.history[chat_id] = user_payload.get("history", [])[:MAX_HISTORY_ITEMS]
            self.pinned[chat_id] = user_payload.get("pinned", [])[:MAX_PINNED_ITEMS]
            auto_update = user_payload.get("auto_update") or {}
            interval = int(auto_update.get("interval", 60) or 60)
            self.auto_update[chat_id] = {
                "enabled": bool(auto_update.get("enabled", False)),
                "interval": max(10, interval),
                "silent": bool(auto_update.get("silent", True)),
                "last_signature": auto_update.get("last_signature", ""),
                "last_signatures": auto_update.get("last_signatures", {}),
                "selected_keys": auto_update.get("selected_keys", []),
                "next_run_ts": float(auto_update.get("next_run_ts", 0) or 0),
                "quiet_hours": auto_update.get("quiet_hours", {"enabled": True, "start": 23, "end": 7}),
                "max_per_hour": int(auto_update.get("max_per_hour", 4) or 4),
                "notify_timestamps": auto_update.get("notify_timestamps", []),
                "failures": int(auto_update.get("failures", 0) or 0),
                "text_mode_until": float(auto_update.get("text_mode_until", 0) or 0),
            }

            if user_payload.get("seen"):
                self.seen_users.add(chat_id)

    def _ensure_user_loaded(self, chat_id: int):
        if chat_id in self.history and chat_id in self.pinned and chat_id in self.auto_update:
            return

        self._load_users_payload()
        user_payload = self._users_payload.get(str(chat_id), {})
        self.history[chat_id] = user_payload.get("history", [])[:MAX_HISTORY_ITEMS]
        self.pinned[chat_id] = user_payload.get("pinned", [])[:MAX_PINNED_ITEMS]

        auto_update = user_payload.get("auto_update") or {}
        interval = int(auto_update.get("interval", 60) or 60)
        self.auto_update[chat_id] = {
            "enabled": bool(auto_update.get("enabled", False)),
            "interval": max(10, interval),
            "silent": bool(auto_update.get("silent", True)),
            "last_signature": auto_update.get("last_signature", ""),
            "last_signatures": auto_update.get("last_signatures", {}),
            "selected_keys": auto_update.get("selected_keys", []),
            "next_run_ts": float(auto_update.get("next_run_ts", 0) or 0),
            "quiet_hours": auto_update.get("quiet_hours", {"enabled": True, "start": 23, "end": 7}),
            "max_per_hour": int(auto_update.get("max_per_hour", 4) or 4),
            "notify_timestamps": auto_update.get("notify_timestamps", []),
            "failures": int(auto_update.get("failures", 0) or 0),
            "text_mode_until": float(auto_update.get("text_mode_until", 0) or 0),
        }

        if user_payload.get("seen"):
            self.seen_users.add(chat_id)

    # ---------------------- UI ----------------------
    def _nav_keyboard(self) -> types.InlineKeyboardMarkup:
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("◀️ Назад", callback_data="poweron:back"),
            types.InlineKeyboardButton("🔄 Почати заново", callback_data="poweron:reset"),
            types.InlineKeyboardButton("🏠 Головна", callback_data="poweron:home"),
        )
        return kb

    def _home_keyboard(self):
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        kb.add(
            types.KeyboardButton("⚡ Перевірити графік"),
            types.KeyboardButton("📌 Мої адреси"),
        )
        kb.add(
            types.KeyboardButton("🕘 Недавні"),
            types.KeyboardButton("🎛 Налаштування"),
        )
        kb.add(
            types.KeyboardButton("📡 Статус"),
            types.KeyboardButton("❓ FAQ"),
        )
        kb.add(
            types.KeyboardButton("⭐ Оцінити бота"),
            types.KeyboardButton("📝 Зворотній зв'язок"),
        )
        kb.add(types.KeyboardButton("🏠 Додому"))
        return kb

    @staticmethod
    def _address_caption(item: dict) -> str:
        settlement_name = item.get("settlement_display") or item.get("settlement_name", "")
        return f"{settlement_name}, {item['street_name']}, {item['house_name']}"

    def _quick_access_keyboard(self, chat_id: int) -> Optional[types.InlineKeyboardMarkup]:
        self._ensure_user_loaded(chat_id)
        pinned = self.pinned.get(chat_id, [])
        history = self.history.get(chat_id, [])

        kb = types.InlineKeyboardMarkup(row_width=1)
        has_any = False
        for idx, item in enumerate(pinned[:MAX_PINNED_ITEMS]):
            kb.add(types.InlineKeyboardButton(f"📌 {self._address_caption(item)}", callback_data=f"poweron:pin_open:{idx}"))
            has_any = True
        if history:
            kb.add(types.InlineKeyboardButton("🕘 Історія (останні 6)", callback_data="poweron:history"))
            has_any = True

        kb.add(types.InlineKeyboardButton("⚙️ Автооновлення", callback_data="poweron:auto_settings"))
        return kb if has_any else kb

    def _pinned_keyboard(self, chat_id: int) -> Optional[types.InlineKeyboardMarkup]:
        self._ensure_user_loaded(chat_id)
        pinned = self.pinned.get(chat_id, [])
        if not pinned:
            return None

        kb = types.InlineKeyboardMarkup(row_width=1)
        for idx, item in enumerate(pinned[:MAX_PINNED_ITEMS]):
            kb.add(types.InlineKeyboardButton(f"📌 {self._address_caption(item)}", callback_data=f"poweron:pin_open:{idx}"))
        nav = self._nav_keyboard()
        for row in nav.keyboard:
            kb.keyboard.append(row)
        return kb

    def _history_keyboard(self, chat_id: int) -> Optional[types.InlineKeyboardMarkup]:
        self._ensure_user_loaded(chat_id)
        history = self.history.get(chat_id, [])
        if not history:
            return None

        pinned_keys = {item["cache_key"] for item in self.pinned.get(chat_id, [])}
        kb = types.InlineKeyboardMarkup(row_width=1)
        for idx, item in enumerate(history[:MAX_HISTORY_ITEMS]):
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

    def _settings_keyboard(self, chat_id: int) -> types.InlineKeyboardMarkup:
        self._ensure_user_loaded(chat_id)
        auto = self.auto_update.get(chat_id, self._default_auto_update_settings())
        status = "✅ ON" if auto.get("enabled") else "⛔️ OFF"
        interval = int(auto.get("interval", 60) or 60)
        silent = "🤫 Тихий" if auto.get("silent", True) else "🔔 Повідомляти завжди"

        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(types.InlineKeyboardButton(f"Автооновлення: {status}", callback_data="poweron:auto_status"))
        kb.add(types.InlineKeyboardButton(f"Інтервал: {interval}с", callback_data="poweron:auto_settings"))
        kb.add(types.InlineKeyboardButton(f"Режим: {silent}", callback_data="poweron:auto_toggle_silent"))
        kb.add(types.InlineKeyboardButton("⚙️ Налаштувати автооновлення", callback_data="poweron:auto_settings"))
        nav = self._nav_keyboard()
        for row in nav.keyboard:
            kb.keyboard.append(row)
        return kb

    def _auto_update_settings_keyboard(self, chat_id: int) -> types.InlineKeyboardMarkup:
        self._ensure_user_loaded(chat_id)
        settings = self.auto_update.get(chat_id, self._default_auto_update_settings())
        current_interval = int(settings.get("interval", 60) or 60)

        kb = types.InlineKeyboardMarkup(row_width=2)
        status = "✅ Увімкнено" if settings.get("enabled") else "⛔️ Вимкнено"
        mode = "🤫 Тихий" if settings.get("silent", True) else "🔔 Завжди"
        kb.add(types.InlineKeyboardButton(f"Статус: {status}", callback_data="poweron:auto_status"))
        kb.add(types.InlineKeyboardButton(f"Режим: {mode}", callback_data="poweron:auto_toggle_silent"))

        kb.add(types.InlineKeyboardButton("Увімкнути", callback_data=f"poweron:auto_on:{current_interval}"))
        kb.add(types.InlineKeyboardButton("Вимкнути", callback_data="poweron:auto_off"))
        kb.add(
            types.InlineKeyboardButton("30с", callback_data="poweron:auto_on:30"),
            types.InlineKeyboardButton("60с", callback_data="poweron:auto_on:60"),
        )
        kb.add(
            types.InlineKeyboardButton("120с", callback_data="poweron:auto_on:120"),
            types.InlineKeyboardButton("✍️ Свій інтервал", callback_data="poweron:auto_custom"),
        )
        kb.add(types.InlineKeyboardButton("📍 Адреси для автооновлення", callback_data="poweron:auto_pick"))
        nav = self._nav_keyboard()
        for row in nav.keyboard:
            kb.keyboard.append(row)
        return kb

    def _auto_update_candidates(self, chat_id: int) -> list:
        self._ensure_user_loaded(chat_id)
        unique = []
        seen = set()
        for item in (self.pinned.get(chat_id, []) + self.history.get(chat_id, [])):
            key = item.get("cache_key")
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(item)
        return unique[:MAX_HISTORY_ITEMS]

    def _auto_update_address_keyboard(self, chat_id: int) -> types.InlineKeyboardMarkup:
        settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
        selected = set(settings.get("selected_keys") or [])
        candidates = self._auto_update_candidates(chat_id)

        kb = types.InlineKeyboardMarkup(row_width=1)
        if not candidates:
            kb.add(types.InlineKeyboardButton("Немає адрес (спершу відкрийте графік)", callback_data="poweron:auto_settings"))
        else:
            for item in candidates:
                key = item.get("cache_key", "")
                checked = "✅" if key in selected else "▫️"
                kb.add(types.InlineKeyboardButton(f"{checked} {self._address_caption(item)}", callback_data=f"poweron:auto_addr:{key}"))

        kb.add(types.InlineKeyboardButton("⬅️ До автооновлення", callback_data="poweron:auto_settings"))
        nav = self._nav_keyboard()
        for row in nav.keyboard:
            kb.keyboard.append(row)
        return kb

    def _faq_text(self) -> str:
        return (
            "❓ FAQ PowerON\n"
            "────────────\n"
            "• Як почати? Натисніть «⚡ Перевірити графік», оберіть населений пункт, вулицю, будинок.\n"
            "• Що показує бот? Скріншот графіка + значення ГПВ з API.\n"
            "• Історія/закріплені: зберігається до 6 адрес в історії і до 6 закріплених.\n"
            "• Автооновлення: у «🎛 Налаштування» відкрийте автооновлення, увімкніть інтервал і виберіть адреси «📍 Адреси для автооновлення».\n"
            "• Тихий режим: надсилання лише при зміні графіка.\n"
            "• Оцінка та відгук: кнопки «⭐ Оцінити бота» і «📝 Зворотній зв'язок» на головному екрані.\n"
            "• Якщо щось не працює: спробуйте повторити запит або відкрийте https://poweron.toe.com.ua/ вручну."
        )

    # ---------------------- data operations ----------------------
    def _upsert_history(self, chat_id: int, item: dict):
        self._ensure_user_loaded(chat_id)
        history = self.history.setdefault(chat_id, [])
        history = [entry for entry in history if entry["cache_key"] != item["cache_key"]]
        history.insert(0, item)
        self.history[chat_id] = history[:MAX_HISTORY_ITEMS]
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
        self.pinned[chat_id] = pinned[:MAX_PINNED_ITEMS]
        self._save_user_data(chat_id)
        return "📌 Адресу закріплено."

    def _is_rate_limited(self, chat_id: int, min_seconds: float = 1.0) -> bool:
        now = time.time()
        last_ts = self.rate_limit.get(chat_id, 0)
        if now - last_ts < min_seconds:
            return True
        self.rate_limit[chat_id] = now
        return False

    def _status_text(self, chat_id: int) -> str:
        self._ensure_user_loaded(chat_id)
        settings = self.auto_update.get(chat_id, {})
        enabled = "✅ Увімкнено" if settings.get("enabled") else "⛔️ Вимкнено"
        interval = int(settings.get("interval", 60) or 60)
        mode = "🤫 Тихий" if settings.get("silent", True) else "🔔 Завжди"
        history = self.history.get(chat_id, [])
        last_address = "—"
        if history:
            last = history[0]
            last_address = f"{last.get('settlement_display', '')}, {last.get('street_name', '')}, {last.get('house_name', '')}"

        return (
            "📡 Ваш статус\n"
            "────────────\n"
            f"• Автооновлення: {enabled}\n"
            f"• Інтервал: {interval}с\n"
            f"• Режим: {mode}\n"
            f"• Остання адреса: {last_address}\n"
            f"• Адрес в історії: {len(history)}\n"
            f"• Закріплених адрес: {len(self.pinned.get(chat_id, []))}"
        )

    def send_home(self, chat_id: int):
        self._ensure_user_loaded(chat_id)
        if chat_id not in self.seen_users:
            self.seen_users.add(chat_id)
            self._save_user_data(chat_id)
            self.bot.send_message(
                chat_id,
                """⚡ PowerON • Швидкий старт

Це сучасний бот для перевірки графіків відключень за вашою адресою.

Натисніть «⚡ Перевірити графік», щоб почати пошук.""",
                reply_markup=self._home_keyboard(),
            )
            return

        self.bot.send_message(chat_id, "⚡ PowerON готовий. Оберіть дію нижче 👇", reply_markup=self._home_keyboard())

    def send_settings(self, chat_id: int):
        self._ensure_user_loaded(chat_id)
        self.bot.send_message(chat_id, "🎛 Панель налаштувань:", reply_markup=self._settings_keyboard(chat_id))

    def start(self, chat_id: int):
        self._ensure_user_loaded(chat_id)
        self.state[chat_id] = {"step": "settlement_query"}
        extra_kb = self._quick_access_keyboard(chat_id)
        if extra_kb:
            self.bot.send_message(chat_id, "✨ Швидкий доступ: закріплені та недавні адреси.", reply_markup=extra_kb)
        self.bot.send_message(chat_id, "🔎 Крок 1/3 · Введіть 2–5 символів населеного пункту.", reply_markup=self._nav_keyboard())

    # ---------------------- message/callback handlers ----------------------
    def handle_message(self, message) -> bool:
        chat_id = message.chat.id
        session = self.state.get(chat_id)
        text = (message.text or "").strip()

        if text in {"💡 Графік світла (за адресою)", "💡 Графік світла", "⚡ Перевірити графік"}:
            self.start(chat_id)
            return True

        if text in {"📌 Закріплені", "📌 Мої адреси"}:
            pinned_kb = self._pinned_keyboard(chat_id)
            if not pinned_kb:
                self.bot.send_message(chat_id, "Немає закріплених адрес. Закріпіть адресу з історії.")
            else:
                self.bot.send_message(chat_id, "📌 Ваші закріплені адреси:", reply_markup=pinned_kb)
            return True

        if text in {"🕘 Історія", "🕘 Недавні"}:
            history_kb = self._history_keyboard(chat_id)
            if not history_kb:
                self.bot.send_message(chat_id, "Історія порожня. Спочатку перегляньте графік хоча б для однієї адреси.")
            else:
                self.bot.send_message(chat_id, "🕘 Останні 6 адрес. Можна відкрити або закріпити:", reply_markup=history_kb)
            return True

        if text in {"⚙️ Налаштування", "🎛 Налаштування"}:
            self.state.pop(chat_id, None)
            self.send_settings(chat_id)
            return True

        if text in {"ℹ️ Статус", "📡 Статус"} or text.lower() == "/status":
            self.bot.send_message(chat_id, self._status_text(chat_id), reply_markup=self._home_keyboard())
            return True

        if text.lower() in {"/faq", "faq"} or text in {"❓ FAQ"}:
            self.bot.send_message(chat_id, self._faq_text(), reply_markup=self._home_keyboard())
            return True

        if text in {"⭐ Оцінити бота"}:
            self.state[chat_id] = {"step": "rating_input"}
            self.bot.send_message(chat_id, "⭐ Оцініть бота від 1 до 5 (надішліть лише число).")
            return True

        if text in {"📝 Зворотній зв'язок"}:
            self.state[chat_id] = {"step": "feedback_input"}
            self.bot.send_message(chat_id, "📝 Напишіть ваш відгук одним повідомленням. Ми врахуємо його в наступних оновленнях.")
            return True

        if text in {"🏠 Головна", "🏠 Додому"}:
            self.state.pop(chat_id, None)
            self.send_home(chat_id)
            return True

        if session and session.get("step") == "rating_input":
            try:
                rating = int(text)
            except ValueError:
                self.bot.send_message(chat_id, "Введіть число від 1 до 5.")
                return True
            if rating < 1 or rating > 5:
                self.bot.send_message(chat_id, "Оцінка має бути в межах 1..5.")
                return True
            user = getattr(message, "from_user", None)
            self.set_user_rating(chat_id, rating)
            self.state.pop(chat_id, None)
            self.bot.send_message(chat_id, f"✅ Дякуємо! Вашу оцінку {rating}/5 збережено.", reply_markup=self._home_keyboard())
            return True

        if session and session.get("step") == "feedback_input":
            if len(text) < 3:
                self.bot.send_message(chat_id, "Будь ласка, додайте трохи більше деталей (мінімум 3 символи).")
                return True
            user = getattr(message, "from_user", None)
            self.add_feedback_entry(
                chat_id,
                text,
                username=getattr(user, "username", "") or "",
                first_name=getattr(user, "first_name", "") or "",
            )
            self.state.pop(chat_id, None)
            self.bot.send_message(chat_id, "✅ Дякуємо за відгук!", reply_markup=self._home_keyboard())
            return True

        if session and session.get("step") == "auto_interval_input":
            try:
                interval = int(text)
            except ValueError:
                self.bot.send_message(chat_id, "Введіть число секунд (наприклад 45).")
                return True
            if interval < 10:
                self.bot.send_message(chat_id, "Мінімальний інтервал — 10 секунд.")
                return True

            settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
            settings["enabled"] = True
            settings["interval"] = interval
            settings["next_run_ts"] = time.time() + interval
            self._schedule_auto_update(chat_id)
            self._save_user_data(chat_id)
            self.state.pop(chat_id, None)
            self.bot.send_message(chat_id, f"✅ Автооновлення увімкнено: кожні {interval} секунд.", reply_markup=self._auto_update_settings_keyboard(chat_id))
            return True

        if not session:
            return False

        if self._is_rate_limited(chat_id, min_seconds=0.8):
            self.bot.send_message(chat_id, "⏱ Забагато запитів. Спробуйте через 1 секунду.")
            return True

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

        if self._is_rate_limited(chat_id, min_seconds=0.4):
            return True

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

        if data == "poweron:retry_last":
            history = self.history.get(chat_id, [])
            if history:
                self._send_schedule(chat_id, history[0])
            else:
                self.bot.send_message(chat_id, "Немає попередньої адреси для повтору.")
            return True
        if data == "poweron:history":
            history_kb = self._history_keyboard(chat_id)
            if not history_kb:
                self.bot.send_message(chat_id, "Історія порожня. Спочатку перегляньте графік хоча б для однієї адреси.")
                return True
            self.bot.send_message(chat_id, "🕘 Останні 6 адрес. Можна відкрити або закріпити:", reply_markup=history_kb)
            return True

        if data == "poweron:auto_settings":
            self.bot.send_message(chat_id, "⚙️ Налаштування автооновлення графіка:", reply_markup=self._auto_update_settings_keyboard(chat_id))
            return True
        if data == "poweron:auto_status":
            self.bot.send_message(chat_id, "Оберіть режим автооновлення:", reply_markup=self._auto_update_settings_keyboard(chat_id))
            return True
        if data == "poweron:auto_toggle_silent":
            self._ensure_user_loaded(chat_id)
            settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
            settings["silent"] = not settings.get("silent", True)
            self._save_user_data(chat_id)
            mode = "🤫 Тихий" if settings["silent"] else "🔔 Завжди"
            self.bot.send_message(chat_id, f"Режим автооновлення: {mode}", reply_markup=self._auto_update_settings_keyboard(chat_id))
            return True
        if data == "poweron:auto_custom":
            self.state[chat_id] = {"step": "auto_interval_input"}
            self.bot.send_message(chat_id, "✍️ Введіть інтервал у секундах (мінімум 10):", reply_markup=self._nav_keyboard())
            return True
        if data == "poweron:auto_pick":
            self.bot.send_message(chat_id, "📍 Оберіть адреси для автооновлення (можна кілька):", reply_markup=self._auto_update_address_keyboard(chat_id))
            return True
        if data == "poweron:auto_off":
            self._ensure_user_loaded(chat_id)
            settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
            settings["enabled"] = False
            settings["next_run_ts"] = 0
            self._save_user_data(chat_id)
            self.bot.send_message(chat_id, "⛔️ Автооновлення вимкнено.", reply_markup=self._auto_update_settings_keyboard(chat_id))
            return True

        try:
            if data.startswith("poweron:auto_addr:"):
                cache_key = data.replace("poweron:auto_addr:", "", 1)
                settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
                selected = [key for key in (settings.get("selected_keys") or []) if isinstance(key, str)]
                if cache_key in selected:
                    selected = [key for key in selected if key != cache_key]
                else:
                    selected.insert(0, cache_key)
                settings["selected_keys"] = selected[:MAX_HISTORY_ITEMS]
                self._save_user_data(chat_id)
                self.bot.send_message(chat_id, "✅ Список адрес автооновлення оновлено.", reply_markup=self._auto_update_address_keyboard(chat_id))
                return True

            if data.startswith("poweron:auto_on:"):
                interval = int(data.rsplit(":", 1)[1])
                if interval < 10:
                    interval = 10
                self._ensure_user_loaded(chat_id)
                settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
                settings["enabled"] = True
                settings["interval"] = interval
                settings["next_run_ts"] = time.time() + interval
                self._schedule_auto_update(chat_id)
                self._save_user_data(chat_id)
                self.bot.send_message(chat_id, f"✅ Автооновлення увімкнено: кожні {interval} секунд.", reply_markup=self._auto_update_settings_keyboard(chat_id))
                return True

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


    def _is_quiet_hours(self, settings: dict) -> bool:
        quiet = settings.get("quiet_hours") or {}
        if not quiet.get("enabled", True):
            return False
        start = int(quiet.get("start", 23) or 23)
        end = int(quiet.get("end", 7) or 7)
        hour = time.localtime().tm_hour
        if start == end:
            return False
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def _can_notify_now(self, settings: dict) -> bool:
        now = time.time()
        timestamps = [ts for ts in (settings.get("notify_timestamps") or []) if now - ts < 3600]
        settings["notify_timestamps"] = timestamps
        max_per_hour = max(1, int(settings.get("max_per_hour", 4) or 4))
        if len(timestamps) >= max_per_hour:
            return False
        if self._is_quiet_hours(settings):
            return False
        return True

    @staticmethod
    def _entry_signature(entry: dict) -> str:
        return entry.get("cache_key", "")

    @staticmethod
    def _entry_ids(entry: dict) -> Optional[Tuple[int, int, int]]:
        cache_key = (entry or {}).get("cache_key", "")
        if not cache_key:
            return None
        try:
            settlement_id, street_id, house_id = cache_key.split(":")
            return int(settlement_id), int(street_id), int(house_id)
        except (TypeError, ValueError):
            return None

    def _refresh_entry_schedule(self, entry: dict) -> dict:
        entry = dict(entry or {})
        ids = self._entry_ids(entry)
        if not ids:
            return entry

        try:
            schedule = asyncio.run(self.client.fetch_house_schedule(*ids))
            if schedule:
                entry["schedule"] = schedule
        except Exception as exc:
            self.logger.warning("poweron.schedule_refresh_failed cache_key=%s error=%s", entry.get("cache_key", ""), exc)
        return entry

    # ---------------------- auto update worker ----------------------
    def _schedule_auto_update(self, chat_id: int):
        settings = self.auto_update.get(chat_id) or {}
        if not settings.get("enabled"):
            return

        interval = max(10, int(settings.get("interval", 60) or 60))
        next_ts = float(settings.get("next_run_ts", 0) or 0)
        if not next_ts:
            next_ts = time.time() + interval
            settings["next_run_ts"] = next_ts
        heapq.heappush(self._auto_update_heap, (next_ts, chat_id))

    def _schedule_all_auto_updates(self):
        self._auto_update_heap = []
        for chat_id in self.auto_update.keys():
            self._schedule_auto_update(chat_id)

    def _start_auto_update_worker(self):
        if self._auto_update_worker_started:
            return
        self._hydrate_users_cache_from_payload()
        self._schedule_all_auto_updates()
        self._auto_update_worker_started = True
        worker = threading.Thread(target=self._auto_update_loop, name="poweron-auto-update", daemon=True)
        worker.start()

    def _auto_update_loop(self):
        while True:
            time.sleep(0.5)
            now = time.time()
            if not self._auto_update_heap:
                continue

            next_run_ts, chat_id = heapq.heappop(self._auto_update_heap)
            if next_run_ts > now:
                heapq.heappush(self._auto_update_heap, (next_run_ts, chat_id))
                continue

            settings = self.auto_update.get(chat_id, {})
            if not settings.get("enabled"):
                continue

            if float(settings.get("next_run_ts", 0) or 0) > next_run_ts + 0.001:
                continue

            interval = max(10, int(settings.get("interval", 60) or 60))
            settings["next_run_ts"] = now + interval
            self._schedule_auto_update(chat_id)

            history = self.history.get(chat_id, [])
            selected_keys = set(settings.get("selected_keys") or [])
            all_candidates = self._auto_update_candidates(chat_id)
            if selected_keys:
                candidates = [item for item in all_candidates if item.get("cache_key") in selected_keys]
            else:
                candidates = history[:1]
            if self.feature_flags.get("multi_address_auto", True):
                candidates = candidates[:MAX_HISTORY_ITEMS]
            else:
                candidates = candidates[:1]
            if not candidates:
                self._save_user_data(chat_id)
                continue

            self.metrics["auto_update_runs"] += 1
            for item in candidates:
                entry_key = self._entry_signature(item)
                try:
                    if float(settings.get("text_mode_until", 0) or 0) > time.time():
                        continue
                    result = self._render_schedule(chat_id, item)
                    settings["failures"] = 0
                except Exception as exc:
                    settings["failures"] = int(settings.get("failures", 0) or 0) + 1
                    self.logger.exception("poweron.auto_update_render_failed chat_id=%s error=%s", chat_id, exc)
                    if settings["failures"] >= AUTO_UPDATE_FAILURE_THRESHOLD and self.feature_flags.get("text_mode_cooldown", True):
                        settings["text_mode_until"] = time.time() + AUTO_UPDATE_COOLDOWN_SECONDS
                    continue

                if not result:
                    continue

                image_path, entry, signature = result
                signatures = settings.setdefault("last_signatures", {})
                previous_sig = signatures.get(entry_key, settings.get("last_signature", ""))
                changed = signature != previous_sig
                always_notify = not settings.get("silent", True)

                if (changed or always_notify) and self._can_notify_now(settings):
                    self._deliver_schedule(chat_id, image_path, entry, entry.get("schedule", {}), auto=True)
                    self.metrics["auto_update_notifications"] += 1
                    settings["notify_timestamps"] = (settings.get("notify_timestamps") or []) + [time.time()]
                    signatures[entry_key] = signature
                    settings["last_signature"] = signature
                    self._upsert_history(chat_id, entry)
            self._save_user_data(chat_id)

    # ---------------------- helpers ----------------------
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
            self.bot.send_message(chat_id, "🔎 Крок 1/3 · Введіть 2–5 символів населеного пункту.", reply_markup=self._nav_keyboard())
            return
        if step in {"house_query", "house_pick"}:
            session["step"] = "street_query"
            self.bot.send_message(chat_id, "🔎 Крок 2/3: Введіть 2–5 символів вулиці.", reply_markup=self._nav_keyboard())
            return
        self.start(chat_id)

    def _build_entry_from_context(self, chat_id: int, address_item: Optional[dict] = None) -> Optional[dict]:
        session = self.state.get(chat_id)
        if not session and not address_item:
            return None

        if address_item:
            normalized_item = self._refresh_entry_schedule(address_item)
            settlement_render = normalized_item.get("settlement_render") or normalized_item.get("settlement_name")
            settlement_display = normalized_item.get("settlement_display") or normalized_item.get("settlement_name")
            street_name = normalized_item["street_name"]
            house_name = normalized_item["house_name"]
            cache_key = normalized_item["cache_key"]
            schedule = normalized_item.get("schedule") or {}
        else:
            settlement = session.get("settlement")
            street = session.get("street")
            house = session.get("house")
            if not settlement or not street or not house:
                return None
            settlement_render = settlement.get("raw_name", settlement["name"])
            settlement_display = settlement["name"]
            street_name = street["name"]
            house_name = house["name"]
            cache_key = f"{settlement['id']}:{street['id']}:{house['id']}"
            schedule = house.get("schedule", {})

        return {
            "cache_key": cache_key,
            "settlement_name": settlement_display,
            "settlement_display": settlement_display,
            "settlement_render": settlement_render,
            "street_name": street_name,
            "house_name": house_name,
            "schedule": schedule,
        }

    def _render_schedule(self, chat_id: int, address_item: Optional[dict] = None):
        entry = self._build_entry_from_context(chat_id, address_item)
        if not entry:
            return None

        started = time.time()
        image_path = asyncio.run(
            self.client.render_schedule_screenshot(
                entry["settlement_render"],
                entry["street_name"],
                entry["house_name"],
                entry["cache_key"],
                force_refresh=bool(address_item),
            )
        )
        self.metrics["last_render_ms"] = int((time.time() - started) * 1000)
        signature = self._file_signature(image_path)
        return image_path, entry, signature

    @staticmethod
    def _file_signature(image_path: str) -> str:
        hasher = hashlib.sha1()
        with open(image_path, "rb") as image_file:
            for chunk in iter(lambda: image_file.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _send_text_fallback(self, chat_id: int, entry: Optional[dict], schedule: Optional[dict], reason: str = ""):
        self.metrics["text_fallbacks"] += 1
        schedule = schedule or {}
        address_caption = "невідомої адреси"
        if entry:
            address_caption = f"{entry.get('settlement_display', '—')}, {entry.get('street_name', '—')}, {entry.get('house_name', '—')}"

        details = (
            "⚠️ Не вдалося сформувати скріншот графіка. Надсилаю текстовий режим.\n"
            f"Адреса: {address_caption}\n\n"
            "Черги з API:\n"
            f"• ГПВ: {schedule.get('gpv', '—')}\n"
        )
        if reason:
            details += f"\nТехнічна причина: {reason}\n"
        details += "\nВи також можете переглянути графік вручну: https://poweron.toe.com.ua/"
        self.bot.send_message(chat_id, details, reply_markup=self._quick_access_keyboard(chat_id) or self._nav_keyboard())

    def _deliver_schedule(self, chat_id: int, image_path: str, entry: dict, schedule: dict, auto: bool = False):
        with open(image_path, "rb") as image_file:
            prefix = "[AUTO] " if auto else ""
            self.bot.send_photo(
                chat_id,
                image_file,
                caption=(
                    f"{prefix}Графік відключень для: {entry['settlement_display']}, {entry['street_name']}, {entry['house_name']} "
                    "(джерело: poweron.toe.com.ua)"
                ),
            )

        self.bot.send_message(
            chat_id,
            "Черги з API:\n"
            f"• ГПВ: {schedule.get('gpv', '—')}",
            reply_markup=self._quick_access_keyboard(chat_id) or self._nav_keyboard(),
        )

    def _send_schedule(self, chat_id: int, address_item: Optional[dict] = None, show_wait: bool = True):
        self.metrics["schedule_requests"] += 1
        entry = self._build_entry_from_context(chat_id, address_item)
        try:
            if show_wait:
                self.bot.send_message(chat_id, "⏳ Очікуйте, формую та завантажую графік...")

            result = self._render_schedule(chat_id, address_item)
            if not result:
                self.metrics["schedule_failures"] += 1
                self._send_text_fallback(chat_id, entry, (entry or {}).get("schedule", {}), reason="немає даних для рендеру")
                return

            image_path, entry, signature = result
            self._deliver_schedule(chat_id, image_path, entry, entry.get("schedule", {}), auto=False)
            self.metrics["schedule_success"] += 1
            self._upsert_history(chat_id, entry)

            settings = self.auto_update.setdefault(chat_id, self._default_auto_update_settings())
            settings["last_signature"] = signature
            self._save_user_data(chat_id)
        except PowerOnClientError as exc:
            self.metrics["schedule_failures"] += 1
            self.logger.warning("poweron.render_client_error chat_id=%s error=%s", chat_id, exc)
            self._send_text_fallback(chat_id, entry, (entry or {}).get("schedule", {}), reason=str(exc))
        except Exception as exc:
            self.metrics["schedule_failures"] += 1
            self.logger.exception("poweron.render_failed chat_id=%s error=%s", chat_id, exc)
            self._send_text_fallback(chat_id, entry, (entry or {}).get("schedule", {}), reason="непередбачена помилка")

    def health_snapshot(self) -> dict:
        return {
            "wizard": dict(self.metrics),
            "client": dict(self.client.metrics),
            "users_loaded": len(self._users_payload),
            "auto_heap_size": len(self._auto_update_heap),
            "feature_flags": dict(self.feature_flags),
        }
