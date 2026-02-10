import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import httpx

BASE_API_URL = "https://api-poweron.toe.com.ua/api"
BASE_SITE_URL = "https://poweron.toe.com.ua/"
CACHE_TTL_SECONDS = 600


class PowerOnClientError(Exception):
    pass


@dataclass
class CacheRecord:
    path: str
    expires_at: float


class PowerOnClient:
    def __init__(self, cache_dir: str = "tmp/poweron"):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self._cache: Dict[str, CacheRecord] = {}
        self._locks: Dict[str, Tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}

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

    async def _get_json(self, path: str, params: Optional[dict] = None) -> dict:
        async with httpx.AsyncClient(base_url=BASE_API_URL, timeout=30.0) as client:
            response = await client.get(path, params=params)
            response.raise_for_status()
            return response.json()

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

    async def render_schedule_screenshot(self, settlement_name: str, street_name: str, house_name: str, cache_key: str) -> str:
        now = time.time()
        cached = self._cache.get(cache_key)
        if cached and cached.expires_at > now and os.path.exists(cached.path):
            return cached.path

        lock = self._get_lock_for_current_loop(cache_key)
        async with lock:
            cached = self._cache.get(cache_key)
            if cached and cached.expires_at > now and os.path.exists(cached.path):
                return cached.path

            file_hash = hashlib.sha1(cache_key.encode("utf-8")).hexdigest()
            image_path = os.path.join(self.cache_dir, f"{file_hash}.png")
            await self._capture_from_site(settlement_name, street_name, house_name, image_path)
            self._cache[cache_key] = CacheRecord(path=image_path, expires_at=time.time() + CACHE_TTL_SECONDS)
            return image_path

    async def _capture_from_site(self, settlement_name: str, street_name: str, house_name: str, image_path: str) -> None:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1400, "height": 2200})
            try:
                await page.goto(BASE_SITE_URL, wait_until="networkidle", timeout=60000)
                await self._select_option(page, 0, settlement_name)
                await self._select_option(page, 1, street_name)
                await self._select_option(page, 2, house_name)
                await page.get_by_role("button", name="Знайти").click()
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(1500)
                await self._screenshot_graph_fragment(page, image_path)
            except Exception as exc:
                raise PowerOnClientError("Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/") from exc
            finally:
                await browser.close()

    @staticmethod
    async def _screenshot_graph_fragment(page, image_path: str) -> None:
        top_anchor = page.get_by_text("Вибрано:").first
        bottom_anchor = page.get_by_text("Якщо у вас відсутня електроенергія", exact=False).first
        if await top_anchor.count() == 0:
            raise PowerOnClientError("Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/")
        top_box = await top_anchor.bounding_box()
        if not top_box:
            raise PowerOnClientError("Не вдалося отримати графік. Спробуйте ще раз або відкрийте вручну: https://poweron.toe.com.ua/")

        bottom_box = await bottom_anchor.bounding_box() if await bottom_anchor.count() else None
        viewport = page.viewport_size or {"width": 1400, "height": 2200}
        clip_y = max(0, top_box["y"] - 20)
        clip_bottom = (bottom_box["y"] - 20) if bottom_box else (clip_y + 420)
        await page.screenshot(
            path=image_path,
            clip={"x": 40, "y": clip_y, "width": max(300, viewport["width"] - 80), "height": max(180, clip_bottom - clip_y)},
        )

    @staticmethod
    async def _select_option(page, input_index: int, desired_text: str) -> None:
        input_box = page.locator('input[id^="react-select-"]').nth(input_index)
        await input_box.click(force=True)
        await input_box.fill("")
        await input_box.type(desired_text, delay=25)
        options = page.locator('div[class*="menu"] div[class*="option"]')
        await options.first.wait_for(timeout=15000)
        count = await options.count()
        normalized_desired = (desired_text or "").lower().replace("вул. ", "").strip()
        for idx in range(min(count, 20)):
            option_text = (await options.nth(idx).inner_text()).strip()
            if option_text.lower().replace("вул. ", "").strip() == normalized_desired:
                await options.nth(idx).click()
                return
        await options.first.click()
