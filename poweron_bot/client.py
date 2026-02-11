import asyncio
import hashlib
import importlib.util
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_API_URL = "https://api-poweron.toe.com.ua/api"
BASE_SITE_URL = "https://poweron.toe.com.ua/"
CACHE_TTL_SECONDS = 600
API_RETRIES = 3
CAPTURE_RETRIES = 2
CACHE_CLEANUP_INTERVAL_SECONDS = 300
CACHE_MAX_FILES = 500
CACHE_MAX_FILE_AGE_SECONDS = 2 * 24 * 60 * 60
BROWSER_ENV_PATH = "POWERON_BROWSER_PATH"

SITE_PROFILE = {
    "search_button_names": ["Знайти", "Пошук", "Показати", "Отримати графік"],
    "search_button_fallback_text": "Знай",
    "queue_card_selector": ".queue-card",
    "schedule_markers": ["Черга", "Вибрано:", "подача електроенергії", "Графік"],
    "selected_marker": "Вибрано:",
    "legend_marker": "подача електроенергії",
}


class PowerOnClientError(Exception):
    pass


class PowerOnNetworkError(PowerOnClientError):
    pass


class PowerOnRenderError(PowerOnClientError):
    pass


@dataclass
class CacheRecord:
    path: str
    expires_at: float


class PowerOnClient:
    def __init__(self, cache_dir: Optional[str] = None):
        from poweron_bot.paths import TMP_DIR

        default_cache_dir = TMP_DIR / "poweron"
        self.cache_dir = str(Path(cache_dir).resolve()) if cache_dir else str(default_cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        self._cache: Dict[str, CacheRecord] = {}
        self._locks: Dict[str, Tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}
        self._last_cache_cleanup_ts = 0.0
        self.metrics = {
            "api_requests": 0,
            "api_failures": 0,
            "render_attempts": 0,
            "render_failures": 0,
            "fullpage_fallbacks": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "last_render_duration_ms": 0,
            "last_render_error": "",
        }

    def _get_lock_for_current_loop(self, cache_key: str) -> asyncio.Lock:
        current_loop = asyncio.get_running_loop()
        lock_record = self._locks.get(cache_key)
        if not lock_record:
            lock = asyncio.Lock()
            self._locks[cache_key] = (current_loop, lock)
            return lock

        lock_loop, lock = lock_record
        if lock_loop is not current_loop:
            lock = asyncio.Lock()
            self._locks[cache_key] = (current_loop, lock)
        return lock

    @staticmethod
    def _has_module(module_name: str) -> bool:
        return importlib.util.find_spec(module_name) is not None

    async def _get_json(self, path: str, params: Optional[dict] = None) -> dict:
        last_error = None
        for attempt in range(1, API_RETRIES + 1):
            self.metrics["api_requests"] += 1
            try:
                if self._has_module("httpx"):
                    import httpx

                    async with httpx.AsyncClient(base_url=BASE_API_URL, timeout=30.0) as client:
                        response = await client.get(path, params=params)
                        response.raise_for_status()
                        return response.json()

                if self._has_module("requests"):
                    import requests

                    def _request_sync():
                        response = requests.get(f"{BASE_API_URL}{path}", params=params, timeout=30.0)
                        response.raise_for_status()
                        return response.json()

                    return await asyncio.to_thread(_request_sync)

                raise PowerOnClientError("Відсутній HTTP-клієнт. Встановіть httpx або requests.")
            except (TimeoutError, OSError) as exc:
                last_error = exc
            except Exception as exc:
                last_error = exc

            self.metrics["api_failures"] += 1
            if attempt < API_RETRIES:
                await asyncio.sleep(0.5 * attempt)

        raise PowerOnNetworkError(f"Не вдалося отримати дані API: {last_error}")

    @staticmethod
    def _member_items(payload: dict) -> List[dict]:
        return payload.get("hydra:member", []) if isinstance(payload, dict) else []

    async def search_settlements(self, query: str, limit: int = 10) -> List[dict]:
        payload = await self._get_json("/pw_cities", params={"pagination": "false", "otg.id": ""})
        items = self._member_items(payload)
        norm_query = (query or "").strip().lower()
        result = []
        for item in items:
            name = item.get("name", "")
            otg_name = (item.get("otg") or {}).get("name", "")
            caption = f"{name} ({otg_name} ОТГ)" if otg_name else name
            if not norm_query or norm_query in caption.lower():
                result.append({"id": item.get("id"), "name": caption, "raw_name": name})
            if len(result) >= limit:
                break
        return result

    async def search_streets(self, settlement_id: int, query: str, limit: int = 10) -> List[dict]:
        payload = await self._get_json(
            "/pw_streets",
            params={"pagination": "false", "city.id": settlement_id},
        )
        items = self._member_items(payload)
        norm_query = (query or "").strip().lower()
        result = []
        for item in items:
            street_name = (item.get("name") or "").replace("вул. ", "").strip()
            if not norm_query or norm_query in street_name.lower():
                result.append({"id": item.get("id"), "name": street_name})
            if len(result) >= limit:
                break
        return result

    async def search_houses(self, settlement_id: int, street_id: int, query: str, limit: int = 10) -> List[dict]:
        payload = await self._get_json(
            "/pw_accounts",
            params={"pagination": "false", "city.id": settlement_id, "street.id": street_id},
        )
        items = self._member_items(payload)
        norm_query = (query or "").strip().lower()
        result = []
        for item in items:
            house = (item.get("buildingName") or "").strip()
            if not house:
                continue
            if not norm_query or norm_query in house.lower():
                result.append(
                    {
                        "id": item.get("id"),
                        "name": house,
                        "schedule": {
                            "gpv": item.get("chergGpv", "—"),
                            "gav": item.get("chergGav", "—"),
                            "achr": item.get("chergAchr", "—"),
                            "gvsp": item.get("chergGvsp", "—"),
                            "sgav": item.get("chergSgav", "—"),
                        },
                    }
                )
            if len(result) >= limit:
                break
        return result

    def _cleanup_cache_files(self) -> None:
        now = time.time()
        if now - self._last_cache_cleanup_ts < CACHE_CLEANUP_INTERVAL_SECONDS:
            return
        self._last_cache_cleanup_ts = now

        try:
            files = []
            for name in os.listdir(self.cache_dir):
                path = os.path.join(self.cache_dir, name)
                if not os.path.isfile(path) or not name.endswith(".png"):
                    continue
                stat = os.stat(path)
                files.append((path, stat.st_mtime))

            for path, mtime in files:
                if now - mtime > CACHE_MAX_FILE_AGE_SECONDS:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

            files = sorted(
                [(path, os.stat(path).st_mtime) for path, _ in files if os.path.exists(path)],
                key=lambda item: item[1],
                reverse=True,
            )
            for path, _ in files[CACHE_MAX_FILES:]:
                try:
                    os.remove(path)
                except OSError:
                    pass
        except OSError:
            return

    async def render_schedule_screenshot(self, settlement_name: str, street_name: str, house_name: str, cache_key: str) -> str:
        self._cleanup_cache_files()
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached and cached.expires_at > now and os.path.exists(cached.path):
            self.metrics["cache_hits"] += 1
            return cached.path

        self.metrics["cache_misses"] += 1
        lock = self._get_lock_for_current_loop(cache_key)
        async with lock:
            cached = self._cache.get(cache_key)
            if cached and cached.expires_at > now and os.path.exists(cached.path):
                self.metrics["cache_hits"] += 1
                return cached.path

            file_hash = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
            image_path = os.path.join(self.cache_dir, f"{file_hash}.png")

            last_error = None
            for attempt in range(1, CAPTURE_RETRIES + 1):
                self.metrics["render_attempts"] += 1
                started = time.time()
                try:
                    await self._capture_from_site(settlement_name, street_name, house_name, image_path)
                    self.metrics["last_render_duration_ms"] = int((time.time() - started) * 1000)
                    self._cache[cache_key] = CacheRecord(path=image_path, expires_at=time.time() + CACHE_TTL_SECONDS)
                    return image_path
                except (PowerOnRenderError, TimeoutError, OSError) as exc:
                    last_error = exc
                    self.metrics["last_render_error"] = str(exc)
                    self.metrics["render_failures"] += 1
                    if attempt < CAPTURE_RETRIES:
                        await asyncio.sleep(0.75 * attempt)

            raise PowerOnRenderError(
                "Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/"
            ) from last_error


    @staticmethod
    def _browser_executable_candidates() -> List[str]:
        env_path = (os.getenv(BROWSER_ENV_PATH, "") or "").strip()
        candidates = []
        if env_path:
            candidates.append(env_path)

        for binary in [
            "chromium",
            "chromium-browser",
            "google-chrome",
            "google-chrome-stable",
        ]:
            resolved = shutil.which(binary)
            if resolved:
                candidates.append(resolved)

        # unique preserve order
        uniq = []
        for item in candidates:
            if item not in uniq:
                uniq.append(item)
        return uniq

    @staticmethod
    def _ubuntu_browser_launch_args() -> List[str]:
        return [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
        ]

    async def _capture_from_site(self, settlement_name: str, street_name: str, house_name: str, image_path: str) -> None:
        from playwright.async_api import Error as PlaywrightError
        from playwright.async_api import TimeoutError as PlaywrightTimeoutError
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            launch_kwargs = {"headless": True, "args": self._ubuntu_browser_launch_args()}
            browser = None
            launch_errors = []

            try:
                browser = await playwright.chromium.launch(**launch_kwargs)
            except Exception as bundled_exc:
                launch_errors.append(f"bundled_chromium={bundled_exc}")

            if browser is None:
                for candidate in self._browser_executable_candidates():
                    try:
                        browser = await playwright.chromium.launch(executable_path=candidate, **launch_kwargs)
                        break
                    except Exception as exc:
                        launch_errors.append(f"{candidate}={exc}")

            if browser is None:
                errors_preview = "; ".join(launch_errors[:3])
                raise PowerOnRenderError(
                    "Не вдалося запустити браузер для скріншота (Ubuntu 22.04). "
                    "Встановіть Chromium: `sudo apt install chromium-browser` або задайте POWERON_BROWSER_PATH. "
                    f"Деталі: {errors_preview}"
                )

            page = await browser.new_page(viewport={"width": 1400, "height": 2200})
            try:
                await page.goto(BASE_SITE_URL, wait_until="domcontentloaded", timeout=60000)
                await self._select_option(page, 0, settlement_name)
                await self._select_option(page, 1, street_name)
                await self._select_option(page, 2, house_name)
                await self._click_search_button(page)
                await page.wait_for_timeout(1200)
                await self._wait_for_schedule_render(page)
                await self._screenshot_graph_fragment(page, image_path)
            except (PlaywrightTimeoutError, PlaywrightError) as exc:
                raise PowerOnRenderError("Помилка рендеру графіка на сайті.") from exc
            finally:
                await browser.close()

    @staticmethod
    async def _click_search_button(page) -> None:
        for name in SITE_PROFILE["search_button_names"]:
            button = page.get_by_role("button", name=name).first
            if await button.count():
                await button.click()
                return

        fallback_button = page.locator("button").filter(has_text=SITE_PROFILE["search_button_fallback_text"]).first
        if await fallback_button.count():
            await fallback_button.click()
            return

        raise PowerOnRenderError("Не знайдено кнопку пошуку графіка на сайті.")

    @staticmethod
    async def _wait_for_schedule_render(page) -> None:
        queue_card = page.locator(SITE_PROFILE["queue_card_selector"]).first
        try:
            await queue_card.wait_for(timeout=10000)
            return
        except Exception:
            pass

        for marker in SITE_PROFILE["schedule_markers"]:
            element = page.get_by_text(marker, exact=False).first
            try:
                await element.wait_for(timeout=4000)
                return
            except Exception:
                continue

        await page.wait_for_timeout(2000)

    def _mark_fullpage_fallback(self) -> None:
        self.metrics["fullpage_fallbacks"] += 1

    async def _screenshot_graph_fragment(self, page, image_path: str) -> None:
        viewport = page.viewport_size or {"width": 1400, "height": 2200}

        queue_cards = page.locator(SITE_PROFILE["queue_card_selector"])
        cards_count = await queue_cards.count()
        if cards_count:
            first_card_box = await self._safe_bounding_box(queue_cards.first)
            last_card_box = await self._safe_bounding_box(queue_cards.nth(cards_count - 1))
            if first_card_box and last_card_box:
                selected_box = await self._safe_bounding_box(page.get_by_text(SITE_PROFILE["selected_marker"], exact=False).first)
                legend_box = await self._safe_bounding_box(page.get_by_text(SITE_PROFILE["legend_marker"], exact=False).first)

                clip_top = first_card_box["y"] - 120
                if selected_box:
                    clip_top = min(clip_top, selected_box["y"] - 20)

                clip_bottom = last_card_box["y"] + last_card_box["height"] + 40
                if legend_box:
                    clip_bottom = max(clip_bottom, legend_box["y"] + legend_box["height"] + 40)

                await page.screenshot(
                    path=image_path,
                    clip={
                        "x": 40,
                        "y": max(0, clip_top),
                        "width": max(300, viewport["width"] - 80),
                        "height": max(220, clip_bottom - max(0, clip_top)),
                    },
                )
                return

        self._mark_fullpage_fallback()
        await page.screenshot(path=image_path, full_page=True)

    @staticmethod
    async def _safe_bounding_box(locator):
        try:
            if await locator.count() == 0:
                return None
            return await locator.bounding_box()
        except Exception:
            return None

    @staticmethod
    async def _select_option(page, input_index: int, desired_text: str) -> None:
        input_box = page.locator('input[id^="react-select-"]').nth(input_index)
        if await input_box.count() == 0:
            input_box = page.locator('div[class*="control"] input').nth(input_index)
        if await input_box.count() == 0:
            input_box = page.get_by_role("combobox").nth(input_index)

        await input_box.click(force=True)
        await input_box.fill("")
        await input_box.type(desired_text, delay=25)

        options = page.locator('[class*="menu"] [class*="option"], [id*="-option-"]')
        if await options.count() == 0:
            options = page.get_by_role("option")

        try:
            await options.first.wait_for(timeout=12000)
        except Exception:
            # for house input site can accept raw value with Enter
            await input_box.press("Enter")
            await page.wait_for_timeout(300)
            return

        count = await options.count()
        normalized_desired = (desired_text or "").lower().replace("вул. ", "").strip()
        for idx in range(min(count, 30)):
            option_text = (await options.nth(idx).inner_text()).strip()
            if option_text.lower().replace("вул. ", "").strip() == normalized_desired:
                await options.nth(idx).click()
                return
        await options.first.click()
