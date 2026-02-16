import os
import tempfile
import time
import unittest
from pathlib import Path
from types import MethodType

from poweron_bot.client import PowerOnClient, PowerOnClientError
from poweron_bot.logging_setup import get_admin_logger, get_user_logger
from poweron_bot.paths import BASE_DIR
from poweron_bot.wizard import PowerOnWizard


class DummyBot:
    def __init__(self):
        self.messages = []
        self.photos = []

    def send_message(self, chat_id, text, reply_markup=None):
        self.messages.append((chat_id, text))

    def send_photo(self, chat_id, image_file, caption=None):
        self.photos.append((chat_id, caption))


class ClientTests(unittest.TestCase):
    def test_cache_cleanup_removes_old_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = PowerOnClient(cache_dir=tmp, enable_periodic_cleanup=False)
            old_path = os.path.join(tmp, "old.png")
            with open(old_path, "wb") as f:
                f.write(b"x")

            # make very old
            old_mtime = 0
            os.utime(old_path, (old_mtime, old_mtime))
            client._last_cache_cleanup_ts = 0
            client._cleanup_cache_files()
            self.assertFalse(os.path.exists(old_path))
            self.assertGreaterEqual(client.metrics.get("cache_cleanup_runs", 0), 1)

    def test_cleanup_cache_now_forces_cleanup_even_with_recent_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = PowerOnClient(cache_dir=tmp, enable_periodic_cleanup=False)
            old_path = os.path.join(tmp, "old2.png")
            with open(old_path, "wb") as f:
                f.write(b"x")
            os.utime(old_path, (0, 0))

            client._last_cache_cleanup_ts = time.time()
            removed = client.cleanup_cache_now()

            self.assertEqual(removed, 1)
            self.assertFalse(os.path.exists(old_path))

    def test_browser_candidates_prefers_env_path(self):
        old = os.environ.get("POWERON_BROWSER_PATH")
        try:
            os.environ["POWERON_BROWSER_PATH"] = "/opt/custom/chrome"
            candidates = PowerOnClient._browser_executable_candidates()
            self.assertGreaterEqual(len(candidates), 1)
            self.assertEqual(candidates[0], "/opt/custom/chrome")
        finally:
            if old is None:
                os.environ.pop("POWERON_BROWSER_PATH", None)
            else:
                os.environ["POWERON_BROWSER_PATH"] = old

    def test_fetch_house_schedule_returns_target_house(self):
        client = PowerOnClient(cache_dir=tempfile.mkdtemp(), enable_periodic_cleanup=False)

        async def _fake_get_json(self, path, params=None):
            assert path == "/pw_houses"
            return {
                "hydra:member": [
                    {"id": 10, "chergGpv": "A"},
                    {"id": 11, "chergGpv": "B", "chergGav": "2"},
                ]
            }

        client._get_json = MethodType(_fake_get_json, client)
        import asyncio

        schedule = asyncio.run(client.fetch_house_schedule(1, 2, 11))
        self.assertEqual(schedule["gpv"], "B")
        self.assertEqual(schedule["gav"], "2")

    def test_render_schedule_force_refresh_bypasses_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            client = PowerOnClient(cache_dir=tmp, enable_periodic_cleanup=False)
            cache_key = "1:2:3"
            image_path = os.path.join(tmp, "cached.png")
            with open(image_path, "wb") as f:
                f.write(b"cached")

            from poweron_bot.client import CacheRecord

            client._cache[cache_key] = CacheRecord(path=image_path, expires_at=10**10)

            calls = {"n": 0}

            async def _fake_capture(self, settlement_name, street_name, house_name, out_path):
                calls["n"] += 1
                with open(out_path, "wb") as out:
                    out.write(b"fresh")

            client._capture_from_site = MethodType(_fake_capture, client)
            import asyncio

            result = asyncio.run(client.render_schedule_screenshot("Town", "Street", "1", cache_key, force_refresh=True))
            self.assertTrue(os.path.exists(result))
            self.assertEqual(calls["n"], 1)


class LoggingTests(unittest.TestCase):
    def test_logging_handlers_use_project_logs_dir(self):
        user_logger = get_user_logger()
        admin_logger = get_admin_logger()

        user_paths = {Path(getattr(handler, "baseFilename", "")) for handler in user_logger.handlers}
        admin_paths = {Path(getattr(handler, "baseFilename", "")) for handler in admin_logger.handlers}

        self.assertIn(BASE_DIR / "logs" / "user_entries.log", user_paths)
        self.assertIn(BASE_DIR / "logs" / "admin_actions.log", admin_paths)


class WizardFallbackTests(unittest.TestCase):
    def test_home_keyboard_has_single_home_button(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)

        kb = wizard._home_keyboard()
        home_count = sum(1 for row in kb.keyboard for btn in row if (btn.get("text") if isinstance(btn, dict) else "") == "🏠 Додому")
        self.assertEqual(home_count, 1)
        map_count = sum(1 for row in kb.keyboard for btn in row if (btn.get("text") if isinstance(btn, dict) else "") == "🗺 Мапа світла (Тернопіль)")
        self.assertEqual(map_count, 1)

    def test_home_keyboard_shows_admin_panel_only_for_admin(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot, admin_user_id=42)

        admin_kb = wizard._home_keyboard(chat_id=42)
        regular_kb = wizard._home_keyboard(chat_id=43)

        admin_has = any((btn.get("text") if isinstance(btn, dict) else "") == "🛠 Адмін панель" for row in admin_kb.keyboard for btn in row)
        regular_has = any((btn.get("text") if isinstance(btn, dict) else "") == "🛠 Адмін панель" for row in regular_kb.keyboard for btn in row)

        self.assertTrue(admin_has)
        self.assertFalse(regular_has)

        admin_map = any((btn.get("text") if isinstance(btn, dict) else "") == "🗺 Мапа світла (Тернопіль)" for row in admin_kb.keyboard for btn in row)
        regular_map = any((btn.get("text") if isinstance(btn, dict) else "") == "🗺 Мапа світла (Тернопіль)" for row in regular_kb.keyboard for btn in row)
        self.assertTrue(admin_map)
        self.assertTrue(regular_map)

    def test_send_schedule_falls_back_to_text(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)

        def _raise_render(*args, **kwargs):
            raise PowerOnClientError("boom")

        wizard._render_schedule = _raise_render
        wizard.state[1] = {
            "settlement": {"id": 1, "name": "Town", "raw_name": "Town"},
            "street": {"id": 2, "name": "Street"},
            "house": {"id": 3, "name": "1", "schedule": {"gpv": "1"}},
        }
        wizard._send_schedule(1, show_wait=False)

        self.assertGreaterEqual(len(bot.messages), 1)
        self.assertEqual(wizard.metrics["text_fallbacks"], 1)
        self.assertGreaterEqual(len(wizard.metrics.get("schedule_latencies_ms", [])), 1)

    def test_map_command_returns_ternopil_screenshot(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot, admin_user_id=77)

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp.write(b"png")
            fake_map_path = tmp.name

        async def _fake_map(*args, **kwargs):
            return fake_map_path

        wizard.client.render_ternopil_map_screenshot = _fake_map
        msg = type(
            "Msg",
            (),
            {
                "text": "/map_ternopil",
                "chat": type("Chat", (), {"id": 77})(),
                "from_user": type("User", (), {"id": 77, "username": "u", "first_name": "N"})(),
            },
        )()

        try:
            handled = wizard.handle_message(msg)
            self.assertTrue(handled)
            self.assertGreaterEqual(len(bot.photos), 1)
            self.assertTrue(any("svitlo.ternopil.webcam" in (caption or "") for _, caption in bot.photos))
        finally:
            os.unlink(fake_map_path)

    def test_map_command_falls_back_to_link_when_render_fails(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot, admin_user_id=77)

        async def _raise_map(*args, **kwargs):
            raise PowerOnClientError("boom")

        wizard.client.render_ternopil_map_screenshot = _raise_map
        msg = type(
            "Msg",
            (),
            {
                "text": "/map_ternopil",
                "chat": type("Chat", (), {"id": 77})(),
                "from_user": type("User", (), {"id": 77, "username": "u", "first_name": "N"})(),
            },
        )()

        handled = wizard.handle_message(msg)
        self.assertTrue(handled)
        self.assertTrue(any("svitlo.ternopil.webcam" in text for _, text in bot.messages))

    def test_map_command_for_non_admin_returns_ternopil_screenshot(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot, admin_user_id=77)

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            tmp.write(b"png")
            fake_map_path = tmp.name

        async def _fake_map(*args, **kwargs):
            return fake_map_path

        wizard.client.render_ternopil_map_screenshot = _fake_map
        msg = type(
            "Msg",
            (),
            {
                "text": "/map_ternopil",
                "chat": type("Chat", (), {"id": 78})(),
                "from_user": type("User", (), {"id": 78, "username": "u", "first_name": "N"})(),
            },
        )()

        try:
            handled = wizard.handle_message(msg)
            self.assertTrue(handled)
            self.assertGreaterEqual(len(bot.photos), 1)
        finally:
            os.unlink(fake_map_path)

    def test_build_entry_refreshes_schedule_from_api(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)

        async def _fake_fetch(settlement_id, street_id, house_id):
            self.assertEqual((settlement_id, street_id, house_id), (1, 2, 3))
            return {"gpv": "9", "gav": "8"}

        wizard.client.fetch_house_schedule = _fake_fetch
        entry = wizard._build_entry_from_context(
            1,
            {
                "cache_key": "1:2:3",
                "settlement_name": "Town",
                "settlement_display": "Town",
                "street_name": "Street",
                "house_name": "1",
                "schedule": {"gpv": "1"},
            },
        )

        self.assertEqual(entry["schedule"]["gpv"], "9")

    def test_history_limit_is_six(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)

        for i in range(8):
            wizard._upsert_history(1, {
                "cache_key": f"1:2:{i}",
                "settlement_display": "Town",
                "street_name": "Street",
                "house_name": str(i),
            })

        self.assertEqual(len(wizard.history[1]), 6)

    def test_auto_update_can_select_specific_addresses(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)
        wizard._ensure_user_loaded(1)
        wizard.history[1] = [
            {"cache_key": "1:2:3", "settlement_display": "A", "street_name": "S", "house_name": "1"},
            {"cache_key": "1:2:4", "settlement_display": "B", "street_name": "S", "house_name": "2"},
        ]
        wizard.auto_update[1] = wizard._default_auto_update_settings()
        call = type("Call", (), {"data": "poweron:auto_addr:1:2:4", "message": type("M", (), {"chat": type("C", (), {"id": 1})()})()})()

        handled = wizard.handle_callback(call)

        self.assertTrue(handled)
        self.assertIn("1:2:4", wizard.auto_update[1]["selected_keys"])

    def test_deliver_schedule_shows_only_gpv(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            tmp.write(b"x")
            tmp.flush()
            wizard._deliver_schedule(
                1,
                tmp.name,
                {"settlement_display": "Town", "street_name": "Street", "house_name": "1"},
                {"gpv": "7", "gav": "2"},
                auto=False,
            )

        self.assertGreaterEqual(len(bot.messages), 1)
        body = bot.messages[-1][1]
        self.assertIn("ГПВ", body)
        self.assertNotIn("ГАВ", body)


class WizardFeedbackTests(unittest.TestCase):
    def _message(self, chat_id: int, text: str, user_id: int = 1, username: str = "user"):
        return type(
            "Msg",
            (),
            {
                "text": text,
                "chat": type("Chat", (), {"id": chat_id})(),
                "from_user": type("User", (), {"id": user_id, "username": username, "first_name": "Name"})(),
            },
        )()


    def test_user_can_rate_only_once(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)

        self.assertTrue(wizard.handle_message(self._message(11, "⭐ Оцінка")))
        call_first = type("Call", (), {"data": "poweron:rate:4", "message": type("M", (), {"chat": type("C", (), {"id": 11})()})()})()
        self.assertTrue(wizard.handle_callback(call_first))

        call_second = type("Call", (), {"data": "poweron:rate:2", "message": type("M", (), {"chat": type("C", (), {"id": 11})()})()})()
        self.assertTrue(wizard.handle_callback(call_second))

        summary = wizard.get_rating_summary()
        self.assertEqual(summary["distribution"]["4"], 1)
        self.assertEqual(summary["distribution"]["2"], 0)

    def test_rating_and_feedback_flow(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)

        self.assertTrue(wizard.handle_message(self._message(10, "⭐ Оцінити бота")))
        call = type("Call", (), {"data": "poweron:rate:5", "message": type("M", (), {"chat": type("C", (), {"id": 10})()})()})()
        self.assertTrue(wizard.handle_callback(call))
        summary = wizard.get_rating_summary()
        self.assertGreaterEqual(summary["count"], 1)

        self.assertTrue(wizard.handle_message(self._message(10, "📝 Зворотній зв'язок")))
        self.assertTrue(wizard.handle_message(self._message(10, "Все супер")))
        entries = wizard.get_feedback_entries()
        self.assertGreaterEqual(len(entries), 1)
        self.assertEqual(entries[-1]["chat_id"], 10)

    def test_feedback_nudge_for_new_user_without_rating_and_feedback(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)
        chat_id = int(time.time() * 1000) % 10_000_000 + 300000

        with wizard._feedback_lock:
            wizard._feedback_payload["entries"] = [item for item in wizard._feedback_payload.get("entries", []) if int(item.get("chat_id", 0)) != chat_id]
            wizard._feedback_payload["ratings"].pop(str(chat_id), None)

        wizard.seen_users.add(chat_id)
        wizard._ensure_user_loaded(chat_id)
        wizard.engagement[chat_id] = {"last_feedback_nudge_ts": 0}
        wizard.send_home(chat_id)

        self.assertTrue(any("поставте оцінку" in text.lower() for _, text in bot.messages))

    def test_feedback_nudge_for_active_user_without_feedback(self):
        bot = DummyBot()
        wizard = PowerOnWizard(bot)
        chat_id = int(time.time() * 1000) % 10_000_000 + 400000

        with wizard._feedback_lock:
            wizard._feedback_payload["entries"] = [item for item in wizard._feedback_payload.get("entries", []) if int(item.get("chat_id", 0)) != chat_id]
            wizard._feedback_payload["ratings"].pop(str(chat_id), None)

        wizard.set_user_rating(chat_id, 5)
        wizard.seen_users.add(chat_id)
        wizard._ensure_user_loaded(chat_id)
        wizard.engagement[chat_id] = {"last_feedback_nudge_ts": 0}
        wizard.history[chat_id] = [
            {"cache_key": "1:2:1", "settlement_display": "A", "street_name": "S", "house_name": "1"},
            {"cache_key": "1:2:2", "settlement_display": "B", "street_name": "S", "house_name": "2"},
            {"cache_key": "1:2:3", "settlement_display": "C", "street_name": "S", "house_name": "3"},
        ]
        wizard.send_home(chat_id)

        self.assertTrue(any("короткий відгук" in text.lower() for _, text in bot.messages))


if __name__ == "__main__":
    unittest.main()
