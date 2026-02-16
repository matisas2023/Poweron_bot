import asyncio
import logging
import os
import sys
import csv
import tempfile
import threading
import time
import statistics
from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import telebot
from telebot import types

from poweron_bot.logging_setup import get_admin_logger, get_user_logger
from poweron_bot.paths import ADMIN_ID_FILE, BASE_DIR, LOGS_DIR, TMP_DIR
from poweron_bot.wizard import PowerOnWizard


def load_token_from_file(path: Path):
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as token_file:
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


def admin_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("📊 Статистика", callback_data="admin:stats"))
    kb.add(types.InlineKeyboardButton("📈 Аналітика", callback_data="admin:analytics"))
    kb.add(types.InlineKeyboardButton("🩺 Стан сервісу", callback_data="admin:health"))
    kb.add(types.InlineKeyboardButton("📣 Розсилка", callback_data="admin:broadcast"))
    kb.add(types.InlineKeyboardButton("🧪 Self-test логів", callback_data="admin:selftest_logs"))
    kb.add(types.InlineKeyboardButton("🖼 Self-test графіка", callback_data="admin:selftest_plot"))
    kb.add(types.InlineKeyboardButton("📥 Завантажити логи", callback_data="admin:download_logs"))
    kb.add(types.InlineKeyboardButton("👥 Експорт користувачів", callback_data="admin:users_export"))
    kb.add(types.InlineKeyboardButton("📝 Відгуки (перегляд)", callback_data="admin:feedback_view"))
    kb.add(types.InlineKeyboardButton("📥 Відгуки CSV", callback_data="admin:feedback_export"))
    kb.add(types.InlineKeyboardButton("⭐ Загальна оцінка", callback_data="admin:ratings"))
    kb.add(types.InlineKeyboardButton("📄 Останні записи логів", callback_data="admin:logs_tail"))
    kb.add(types.InlineKeyboardButton("🎛 Прапорці функцій", callback_data="admin:feature_flags"))
    kb.add(types.InlineKeyboardButton("🛑 Вимкнути сервер", callback_data="admin:shutdown"))
    kb.add(types.InlineKeyboardButton("🔄 Перезапустити сервер", callback_data="admin:restart"))
    return kb


def feature_flags_keyboard(flags: dict) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, value in sorted(flags.items()):
        status_icon = "🟢" if value else "⚪️"
        kb.add(types.InlineKeyboardButton(f"{status_icon} {key}", callback_data=f"admin:feature_toggle:{key}"))
    kb.add(types.InlineKeyboardButton("🔙 До адмін-меню", callback_data="admin:menu"))
    return kb


def broadcast_confirm_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("✅ Підтвердити", callback_data="admin:broadcast_confirm"),
        types.InlineKeyboardButton("❌ Скасувати", callback_data="admin:broadcast_cancel"),
    )
    return kb


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    token = os.getenv("POWERON_BOT_TOKEN") or load_token_from_file(BASE_DIR / "poweron_bot_token.txt")
    if not token:
        raise RuntimeError("Set POWERON_BOT_TOKEN or create poweron_bot_token.txt")

    admin_id_raw = os.getenv("POWERON_ADMIN_USER_ID") or load_token_from_file(ADMIN_ID_FILE)
    admin_user_id = parse_admin_id(admin_id_raw)

    allowed_ids = parse_allowed_ids(os.getenv("POWERON_ALLOWED_IDS", ""))
    bot = telebot.TeleBot(token)
    wizard = PowerOnWizard(bot)
    user_logger = get_user_logger()
    admin_logger = get_admin_logger()
    admin_broadcast_pending = set()
    admin_broadcast_draft = {}

    def metric_percentile(values, percent: float) -> int:
        if not values:
            return 0
        sorted_values = sorted(int(v) for v in values)
        if len(sorted_values) == 1:
            return sorted_values[0]
        pos = int(round((len(sorted_values) - 1) * (percent / 100.0)))
        pos = max(0, min(pos, len(sorted_values) - 1))
        return sorted_values[pos]

    def format_latency_block(label: str, values: list) -> str:
        if not values:
            return f"• {label}: n/a"
        return (
            f"• {label}: avg={int(statistics.mean(values))}ms "
            f"p50={metric_percentile(values, 50)}ms p95={metric_percentile(values, 95)}ms n={len(values)}"
        )

    def is_allowed(message):
        user_id = getattr(message.from_user, "id", None)
        if allowed_ids:
            return user_id in allowed_ids
        return True

    def is_admin(user_id: int) -> bool:
        return admin_user_id is not None and user_id == admin_user_id

    def log_admin_action(user, action: str, details: str = "", chat_id: Optional[int] = None):
        admin_logger.info(
            "admin_action=%s user_id=%s username=%s chat_id=%s details=%s",
            action,
            getattr(user, "id", None),
            getattr(user, "username", None),
            chat_id,
            details,
        )

    def log_user_action(message, action: str, details: str = ""):
        user = getattr(message, "from_user", None)
        chat = getattr(message, "chat", None)
        user_logger.info(
            "user_action=%s chat_id=%s user_id=%s username=%s first_name=%s details=%s",
            action,
            getattr(chat, "id", None),
            getattr(user, "id", None),
            getattr(user, "username", None),
            getattr(user, "first_name", None),
            details,
        )

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

    def build_status_text(chat_id: int) -> str:
        wizard._ensure_user_loaded(chat_id)
        settings = wizard.auto_update.get(chat_id, {})
        enabled = "✅ Увімкнено" if settings.get("enabled") else "⛔️ Вимкнено"
        interval = int(settings.get("interval", 60) or 60)
        mode = "🤫 Тихий" if settings.get("silent", True) else "🔔 Завжди"
        history = wizard.history.get(chat_id, [])
        last_address = "—"
        if history:
            last = history[0]
            last_address = f"{last.get('settlement_display', '')}, {last.get('street_name', '')}, {last.get('house_name', '')}"

        return (
            "ℹ️ Ваш статус:\n"
            f"• Автооновлення: {enabled}\n"
            f"• Інтервал: {interval}с\n"
            f"• Режим: {mode}\n"
            f"• Остання адреса: {last_address}"
        )

    def create_test_plot(chat_id: int) -> Path:
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix="poweron_test_plot_", suffix=".png", dir=TMP_DIR, delete=False) as tmp_file:
            image_path = Path(tmp_file.name)

        x_values = [0, 1, 2, 3, 4]
        y_values = [2, 1, 3, 2, 4]
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.plot(x_values, y_values, marker="o")
        ax.set_title("PowerOn test графік")
        ax.set_xlabel("Крок")
        ax.set_ylabel("Рівень")
        ax.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()
        fig.savefig(image_path, dpi=150)
        plt.close(fig)

        user_logger.info("user_action=test_plot_generated chat_id=%s file=%s", chat_id, image_path)
        return image_path

    def send_logs_to_admin(chat_id: int, user, source: str):
        sent_count = 0
        missing_files = []
        for log_file in (LOGS_DIR / "admin_actions.log", LOGS_DIR / "user_entries.log"):
            if not log_file.exists():
                missing_files.append(log_file.name)
                continue
            with log_file.open("rb") as fh:
                bot.send_document(chat_id, fh, visible_file_name=log_file.name, caption=f"📥 Лог: {log_file.name}")
            sent_count += 1

        details = f"source={source} sent={sent_count} missing={','.join(missing_files) if missing_files else '-'}"
        log_admin_action(user, "download_logs", details, chat_id=chat_id)

        if sent_count == 0:
            bot.send_message(chat_id, "⚠️ Лог-файли поки що відсутні. Спробуйте /selftest_logs і повторіть.")
        elif missing_files:
            bot.send_message(chat_id, f"✅ Надіслано {sent_count} лог(и). Відсутні: {', '.join(missing_files)}")

    def run_selftest_logs(chat_id: int, user, source: str, message=None):
        log_admin_action(user, "selftest_logs", f"source={source}", chat_id=chat_id)
        if message is not None:
            log_user_action(message, "selftest_logs")
        else:
            user_logger.info(
                "user_action=selftest_logs chat_id=%s user_id=%s username=%s details=%s",
                chat_id,
                getattr(user, "id", None),
                getattr(user, "username", None),
                f"source={source}",
            )
        bot.send_message(chat_id, "✅ Тестові записи додані в admin_actions.log і user_entries.log")

    def run_selftest_plot(chat_id: int, user, source: str):
        image_path = None
        try:
            image_path = create_test_plot(chat_id)
            with image_path.open("rb") as image_file:
                bot.send_photo(chat_id, image_file, caption="🧪 Тестовий PNG-графік (headless/Agg)")
            log_admin_action(user, "selftest_plot", f"source={source} file={image_path.name}", chat_id=chat_id)
        except Exception as exc:
            user_logger.exception("user_action=selftest_plot_failed chat_id=%s error=%s", chat_id, exc)
            bot.send_message(chat_id, "❌ Не вдалося згенерувати тестовий графік.")
        finally:
            if image_path and image_path.exists():
                image_path.unlink(missing_ok=True)

    def schedule_shutdown():
        def _stop():
            os._exit(0)

        threading.Timer(1.0, _stop).start()

    def schedule_restart():
        def _restart():
            os.execv(sys.executable, [sys.executable, "-m", "poweron_bot.main"])

        threading.Timer(1.0, _restart).start()

    def run_broadcast(text: str) -> dict:
        wizard._load_users_payload()
        sent = 0
        failed = 0
        failures = []
        started = time.time()
        delivery_latencies = []

        for idx, chat_id_str in enumerate(wizard._users_payload.keys(), start=1):
            try:
                chat_id = int(chat_id_str)
            except ValueError:
                failed += 1
                failures.append("invalid_chat_id")
                continue

            try:
                send_started = time.time()
                bot.send_message(chat_id, f"📣 Повідомлення від адміністратора:\n\n{text}")
                sent += 1
                delivery_latencies.append(int((time.time() - send_started) * 1000))
            except Exception as exc:
                failed += 1
                failures.append(type(exc).__name__)
                error_text = str(exc).lower()
                if "429" in error_text or "too many requests" in error_text:
                    time.sleep(1.5)
                elif "timed out" in error_text or "timeout" in error_text:
                    time.sleep(0.8)

                for retry in range(2):
                    backoff = 0.4 * (2 ** retry)
                    time.sleep(backoff)
                    try:
                        send_started = time.time()
                        bot.send_message(chat_id, f"📣 Повідомлення від адміністратора:\n\n{text}")
                        sent += 1
                        failed = max(0, failed - 1)
                        delivery_latencies.append(int((time.time() - send_started) * 1000))
                        break
                    except Exception as retry_exc:
                        failures.append(type(retry_exc).__name__)

            if idx % 25 == 0:
                time.sleep(0.3)

        duration_ms = int((time.time() - started) * 1000)
        return {
            "sent": sent,
            "failed": failed,
            "duration_ms": duration_ms,
            "failure_types": sorted(set(failures))[:8],
            "delivery_latencies_ms": delivery_latencies,
        }

    def build_health_text() -> str:
        api_ok = False
        api_error = None
        try:
            items = asyncio.run(wizard.client.search_settlements("а", limit=1))
            api_ok = isinstance(items, list)
        except Exception as exc:
            api_error = str(exc)

        cache_ok = os.path.isdir(wizard.client.cache_dir)
        snapshot = wizard.health_snapshot()
        wizard_metrics = snapshot.get("wizard", {})
        client_metrics = snapshot.get("client", {})
        return (
            "🩺 Health check:\n"
            f"• API: {'✅ OK' if api_ok else '❌ FAIL'}\n"
            f"• Cache dir: {'✅ OK' if cache_ok else '❌ FAIL'} ({wizard.client.cache_dir})\n"
            f"• Polling restart loop: ✅ enabled\n"
            f"• schedule: req={wizard_metrics.get('schedule_requests', 0)} ok={wizard_metrics.get('schedule_success', 0)} fail={wizard_metrics.get('schedule_failures', 0)}\n"
            f"• text fallback count: {wizard_metrics.get('text_fallbacks', 0)}\n"
            f"• auto update: runs={wizard_metrics.get('auto_update_runs', 0)} notify={wizard_metrics.get('auto_update_notifications', 0)} heap={snapshot.get('auto_heap_size', 0)}\n"
            f"• render: attempts={client_metrics.get('render_attempts', 0)} fail={client_metrics.get('render_failures', 0)} fullpage_fallback={client_metrics.get('fullpage_fallbacks', 0)}\n"
            f"• cache: hits={client_metrics.get('cache_hits', 0)} miss={client_metrics.get('cache_misses', 0)}"
            f"\n• auto queue size: {snapshot.get('auto_heap_size', 0)}"
            f"\n• auto retry pressure: {sum(int((item or {}).get('failures', 0) or 0) for item in wizard.auto_update.values())}"
            f"\n{format_latency_block('API latency', client_metrics.get('api_latencies_ms', []))}"
            f"\n{format_latency_block('Render latency', client_metrics.get('render_latencies_ms', []))}"
            f"\n{format_latency_block('Schedule latency', wizard_metrics.get('schedule_latencies_ms', []))}"
            + (f"\n• API error: {api_error}" if api_error else "")
        )

    def build_analytics_text() -> str:
        snapshot = wizard.health_snapshot()
        wizard_metrics = snapshot.get("wizard", {})
        client_metrics = snapshot.get("client", {})
        users_total = len(wizard._users_payload)
        dau = sum(1 for item in wizard._users_payload.values() if item.get("seen"))
        return (
            "📈 Analytics\n"
            f"• Users total: {users_total}\n"
            f"• Active seen users: {dau}\n"
            f"• schedule req/success/fail: {wizard_metrics.get('schedule_requests', 0)}/{wizard_metrics.get('schedule_success', 0)}/{wizard_metrics.get('schedule_failures', 0)}\n"
            f"• auto notifications: {wizard_metrics.get('auto_update_notifications', 0)}\n"
            f"• render fail: {client_metrics.get('render_failures', 0)}\n"
            f"• last render ms: {wizard_metrics.get('last_render_ms', 0)}\n"
            f"{format_latency_block('API latency', client_metrics.get('api_latencies_ms', []))}\n"
            f"{format_latency_block('Render latency', client_metrics.get('render_latencies_ms', []))}\n"
            f"{format_latency_block('Schedule latency', wizard_metrics.get('schedule_latencies_ms', []))}"
        )

    def send_users_export(chat_id: int, user, source: str):
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        export_path = TMP_DIR / "users_export.csv"
        with export_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["chat_id", "seen", "history_count", "pinned_count", "auto_enabled", "auto_interval", "silent"])
            for chat_id_str, payload in wizard._users_payload.items():
                auto = payload.get("auto_update") or {}
                writer.writerow([
                    chat_id_str,
                    int(bool(payload.get("seen"))),
                    len(payload.get("history") or []),
                    len(payload.get("pinned") or []),
                    int(bool(auto.get("enabled"))),
                    int(auto.get("interval", 60) or 60),
                    int(bool(auto.get("silent", True))),
                ])
        with export_path.open("rb") as csv_file:
            bot.send_document(chat_id, csv_file, visible_file_name="users_export.csv", caption="👥 Експорт користувачів")
        log_admin_action(user, "users_export", f"source={source}", chat_id=chat_id)

    def send_logs_tail(chat_id: int, user, source: str, lines: int = 100):
        snippets = []
        for log_file in (LOGS_DIR / "admin_actions.log", LOGS_DIR / "user_entries.log"):
            if not log_file.exists():
                snippets.append(f"{log_file.name}: файл відсутній")
                continue
            content = log_file.read_text(encoding="utf-8", errors="ignore").splitlines()[-lines:]
            snippets.append(f"{log_file.name}:\n" + "\n".join(content[-20:]))
        text_payload = "\n\n".join(snippets)[:3800]
        bot.send_message(chat_id, f"📄 Останні записи логів:\n\n{text_payload}")
        log_admin_action(user, "logs_tail", f"source={source} lines={lines}", chat_id=chat_id)

    def build_feature_flags_text() -> str:
        flags = wizard.feature_flags
        lines = ["🎛 Прапорці функцій:"]
        for key, value in sorted(flags.items()):
            lines.append(f"• {key}: {'Увімкнено' if value else 'Вимкнено'}")
        lines.append("\nМожна перемкнути через інлайн-меню або командою /feature_flags <name> <on|off>")
        return "\n".join(lines)

    def set_feature_flag(user, chat_id: int, name: str, value: bool):
        if name not in wizard.feature_flags:
            bot.send_message(chat_id, f"Невідомий feature flag: {name}")
            return
        wizard.feature_flags[name] = value
        log_admin_action(user, "feature_flag_set", f"{name}={value}", chat_id=chat_id)
        bot.send_message(
            chat_id,
            f"✅ {name}: {'Увімкнено' if value else 'Вимкнено'}",
            reply_markup=feature_flags_keyboard(wizard.feature_flags),
        )

    def build_rating_text() -> str:
        summary = wizard.get_rating_summary()
        distribution = summary.get("distribution") or {}
        return (
            "⭐ Загальна оцінка бота\n"
            f"• Кількість оцінок: {summary.get('count', 0)}\n"
            f"• Середня оцінка: {summary.get('average', 0):.2f}/5\n"
            f"• 5⭐: {distribution.get('5', 0)} | 4⭐: {distribution.get('4', 0)} | 3⭐: {distribution.get('3', 0)} | 2⭐: {distribution.get('2', 0)} | 1⭐: {distribution.get('1', 0)}"
        )

    def build_feedback_preview(limit: int = 10) -> str:
        entries = wizard.get_feedback_entries()[-limit:]
        if not entries:
            return "📝 Відгуків поки немає."
        lines = ["📝 Останні відгуки:"]
        for item in reversed(entries):
            created = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(item.get("created_at", 0) or 0)))
            user_caption = item.get("username") or item.get("first_name") or str(item.get("chat_id"))
            text_value = (item.get("text") or "").replace("\n", " ").strip()
            lines.append(f"• [{created}] @{user_caption}: {text_value[:180]}")
        return "\n".join(lines)

    def send_feedback_export(chat_id: int, user, source: str):
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        export_path = TMP_DIR / "feedback_export.csv"
        entries = wizard.get_feedback_entries()
        with export_path.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["created_at", "chat_id", "username", "first_name", "text"])
            for item in entries:
                writer.writerow([
                    int(item.get("created_at", 0) or 0),
                    int(item.get("chat_id", 0) or 0),
                    item.get("username", ""),
                    item.get("first_name", ""),
                    item.get("text", ""),
                ])
        with export_path.open("rb") as csv_file:
            bot.send_document(chat_id, csv_file, visible_file_name="feedback_export.csv", caption="📥 Експорт відгуків")
        log_admin_action(user, "feedback_export", f"source={source} entries={len(entries)}", chat_id=chat_id)

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

    @bot.message_handler(commands=["status"])
    def cmd_status(message):
        log_user_action(message, "status_command")
        if not is_allowed(message):
            bot.send_message(message.chat.id, "⛔️ Доступ заборонено")
            return
        bot.send_message(message.chat.id, build_status_text(message.chat.id))

    @bot.message_handler(commands=["admin"])
    def cmd_admin(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "admin_menu_open", chat_id=message.chat.id)
        bot.send_message(message.chat.id, "🛠 Адмін-меню:", reply_markup=admin_keyboard())

    @bot.message_handler(commands=["stats"])
    def cmd_stats(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "stats", chat_id=message.chat.id)
        bot.send_message(message.chat.id, build_stats_text())

    @bot.message_handler(commands=["analytics"])
    def cmd_analytics(message):
        if not is_admin(message.from_user.id):
            return
        wizard._load_users_payload()
        log_admin_action(message.from_user, "analytics", chat_id=message.chat.id)
        bot.send_message(message.chat.id, build_analytics_text())

    @bot.message_handler(commands=["health"])
    def cmd_health(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "health", chat_id=message.chat.id)
        bot.send_message(message.chat.id, build_health_text())

    @bot.message_handler(commands=["broadcast"])
    def cmd_broadcast(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "broadcast_start", chat_id=message.chat.id)
        admin_broadcast_pending.add(message.chat.id)
        bot.send_message(message.chat.id, "📣 Введіть текст для розсилки всім користувачам:")

    @bot.message_handler(commands=["selftest_logs"])
    def cmd_selftest_logs(message):
        if not is_admin(message.from_user.id):
            return
        run_selftest_logs(message.chat.id, message.from_user, source="command", message=message)

    @bot.message_handler(commands=["selftest_plot"])
    def cmd_selftest_plot(message):
        if not is_admin(message.from_user.id):
            return
        run_selftest_plot(message.chat.id, message.from_user, source="command")

    @bot.message_handler(commands=["download_logs"])
    def cmd_download_logs(message):
        if not is_admin(message.from_user.id):
            return
        send_logs_to_admin(message.chat.id, message.from_user, source="command")

    @bot.message_handler(commands=["users_export"])
    def cmd_users_export(message):
        if not is_admin(message.from_user.id):
            return
        wizard._load_users_payload()
        send_users_export(message.chat.id, message.from_user, source="command")

    @bot.message_handler(commands=["feedback_view"])
    def cmd_feedback_view(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "feedback_view", chat_id=message.chat.id)
        bot.send_message(message.chat.id, build_feedback_preview())

    @bot.message_handler(commands=["feedback_export"])
    def cmd_feedback_export(message):
        if not is_admin(message.from_user.id):
            return
        send_feedback_export(message.chat.id, message.from_user, source="command")

    @bot.message_handler(commands=["ratings"])
    def cmd_ratings(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "ratings", chat_id=message.chat.id)
        bot.send_message(message.chat.id, build_rating_text())

    @bot.message_handler(commands=["logs_tail"])
    def cmd_logs_tail(message):
        if not is_admin(message.from_user.id):
            return
        send_logs_tail(message.chat.id, message.from_user, source="command")

    @bot.message_handler(commands=["feature_flags"])
    def cmd_feature_flags(message):
        if not is_admin(message.from_user.id):
            return
        parts = (message.text or "").split()
        if len(parts) == 3:
            name = parts[1].strip()
            value = parts[2].strip().lower() in {"1", "on", "true", "yes"}
            set_feature_flag(message.from_user, message.chat.id, name, value)
            return
        bot.send_message(
            message.chat.id,
            build_feature_flags_text(),
            reply_markup=feature_flags_keyboard(wizard.feature_flags),
        )

    @bot.message_handler(commands=["shutdown"])
    def cmd_shutdown(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "shutdown", chat_id=message.chat.id)
        bot.send_message(message.chat.id, "🛑 Сервер буде зупинено через 1 секунду.")
        schedule_shutdown()

    @bot.message_handler(commands=["restart"])
    def cmd_restart(message):
        if not is_admin(message.from_user.id):
            return
        log_admin_action(message.from_user, "restart", chat_id=message.chat.id)
        bot.send_message(message.chat.id, "🔄 Перезапуск сервера через 1 секунду.")
        schedule_restart()

    @bot.message_handler(func=lambda m: True)
    def on_message(message):
        text = (message.text or "").strip()
        log_user_action(message, "message", f"text={text[:120]}")
        if not is_allowed(message):
            bot.send_message(message.chat.id, "⛔️ Доступ заборонено")
            return

        if is_admin(message.from_user.id) and message.chat.id in admin_broadcast_pending:
            text = (message.text or "").strip()
            if not text:
                bot.send_message(message.chat.id, "Повідомлення порожнє. Введіть текст знову:")
                return
            admin_broadcast_pending.discard(message.chat.id)
            admin_broadcast_draft[message.chat.id] = text
            log_admin_action(message.from_user, "broadcast_preview", f"len={len(text)}", chat_id=message.chat.id)
            bot.send_message(
                message.chat.id,
                f"📣 Попередній перегляд розсилки:\n\n{text}\n\nПідтвердити відправку?",
                reply_markup=broadcast_confirm_keyboard(),
            )
            return

        if wizard.handle_message(message):
            return
        if text.lower() in {"/start", "start", "старт", "🚀 старт"}:
            wizard.send_home(message.chat.id)

    @bot.callback_query_handler(func=lambda call: True)
    def on_callback(call):
        if allowed_ids and call.from_user.id not in allowed_ids:
            return

        if call.data == "admin:menu" and is_admin(call.from_user.id):
            bot.send_message(call.message.chat.id, "🛠 Адмін-меню:", reply_markup=admin_keyboard())
            return
        if call.data == "admin:stats" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "stats", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, build_stats_text())
            return
        if call.data == "admin:analytics" and is_admin(call.from_user.id):
            wizard._load_users_payload()
            log_admin_action(call.from_user, "analytics", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, build_analytics_text())
            return
        if call.data == "admin:health" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "health", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, build_health_text())
            return
        if call.data == "admin:broadcast" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "broadcast_start", chat_id=call.message.chat.id)
            admin_broadcast_pending.add(call.message.chat.id)
            bot.send_message(call.message.chat.id, "📣 Введіть текст для розсилки всім користувачам:")
            return
        if call.data == "admin:broadcast_confirm" and is_admin(call.from_user.id):
            text = admin_broadcast_draft.pop(call.message.chat.id, "")
            if not text:
                bot.send_message(call.message.chat.id, "Немає підготовленого тексту для розсилки.")
                return
            broadcast_result = run_broadcast(text)
            log_admin_action(
                call.from_user,
                "broadcast_confirm",
                f"sent={broadcast_result['sent']} failed={broadcast_result['failed']} duration_ms={broadcast_result['duration_ms']}",
                chat_id=call.message.chat.id,
            )
            bot.send_message(
                call.message.chat.id,
                "✅ Розсилку завершено.\n"
                f"Надіслано: {broadcast_result['sent']}\n"
                f"Помилок: {broadcast_result['failed']}\n"
                f"Тривалість: {broadcast_result['duration_ms']} мс\n"
                f"{format_latency_block('Delivery latency', broadcast_result.get('delivery_latencies_ms', []))}\n"
                f"Типи помилок: {', '.join(broadcast_result['failure_types']) if broadcast_result['failure_types'] else '—'}",
            )
            return
        if call.data == "admin:broadcast_cancel" and is_admin(call.from_user.id):
            admin_broadcast_draft.pop(call.message.chat.id, None)
            log_admin_action(call.from_user, "broadcast_cancel", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, "❌ Розсилку скасовано.")
            return
        if call.data == "admin:selftest_logs" and is_admin(call.from_user.id):
            run_selftest_logs(call.message.chat.id, call.from_user, source="callback")
            return
        if call.data == "admin:selftest_plot" and is_admin(call.from_user.id):
            run_selftest_plot(call.message.chat.id, call.from_user, source="callback")
            return
        if call.data == "admin:download_logs" and is_admin(call.from_user.id):
            send_logs_to_admin(call.message.chat.id, call.from_user, source="callback")
            return
        if call.data == "admin:users_export" and is_admin(call.from_user.id):
            wizard._load_users_payload()
            send_users_export(call.message.chat.id, call.from_user, source="callback")
            return
        if call.data == "admin:logs_tail" and is_admin(call.from_user.id):
            send_logs_tail(call.message.chat.id, call.from_user, source="callback")
            return
        if call.data == "admin:feedback_view" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "feedback_view", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, build_feedback_preview())
            return
        if call.data == "admin:feedback_export" and is_admin(call.from_user.id):
            send_feedback_export(call.message.chat.id, call.from_user, source="callback")
            return
        if call.data == "admin:ratings" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "ratings", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, build_rating_text())
            return
        if call.data == "admin:feature_flags" and is_admin(call.from_user.id):
            bot.send_message(
                call.message.chat.id,
                build_feature_flags_text(),
                reply_markup=feature_flags_keyboard(wizard.feature_flags),
            )
            return
        if call.data.startswith("admin:feature_toggle:") and is_admin(call.from_user.id):
            flag_name = call.data.split(":", 2)[2]
            if flag_name not in wizard.feature_flags:
                bot.answer_callback_query(call.id, "Невідомий прапорець")
                return
            wizard.feature_flags[flag_name] = not wizard.feature_flags[flag_name]
            log_admin_action(
                call.from_user,
                "feature_flag_toggle",
                f"{flag_name}={wizard.feature_flags[flag_name]} source=inline",
                chat_id=call.message.chat.id,
            )
            bot.edit_message_text(
                build_feature_flags_text(),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                reply_markup=feature_flags_keyboard(wizard.feature_flags),
            )
            bot.answer_callback_query(call.id, f"{flag_name}: {'ON' if wizard.feature_flags[flag_name] else 'OFF'}")
            return
        if call.data == "admin:shutdown" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "shutdown", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, "🛑 Сервер буде зупинено через 1 секунду.")
            schedule_shutdown()
            return
        if call.data == "admin:restart" and is_admin(call.from_user.id):
            log_admin_action(call.from_user, "restart", chat_id=call.message.chat.id)
            bot.send_message(call.message.chat.id, "🔄 Перезапуск сервера через 1 секунду.")
            schedule_restart()
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
