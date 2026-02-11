import os
import tempfile
import unittest

from poweron_bot.client import PowerOnClient, PowerOnClientError
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


if __name__ == "__main__":
    unittest.main()
