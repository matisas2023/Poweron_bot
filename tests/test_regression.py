import os
import tempfile
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
            client = PowerOnClient(cache_dir=tmp)
            old_path = os.path.join(tmp, "old.png")
            with open(old_path, "wb") as f:
                f.write(b"x")

            # make very old
            old_mtime = 0
            os.utime(old_path, (old_mtime, old_mtime))
            client._last_cache_cleanup_ts = 0
            client._cleanup_cache_files()
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
        client = PowerOnClient(cache_dir=tempfile.mkdtemp())

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
            client = PowerOnClient(cache_dir=tmp)
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
        wizard.history[1] = [
            {"cache_key": "1:2:3", "settlement_display": "A", "street_name": "S", "house_name": "1"},
            {"cache_key": "1:2:4", "settlement_display": "B", "street_name": "S", "house_name": "2"},
        ]
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


if __name__ == "__main__":
    unittest.main()
