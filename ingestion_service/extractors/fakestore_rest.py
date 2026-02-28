import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List

import aiohttp
from aiohttp.client_exceptions import ClientError

from ..config import APP
from ..logger import log
from .base_extractor import BaseExtractor


class FakeStoreRestExtractor(BaseExtractor):

    MAX_RETRIES = 3
    RETRY_DELAY = 5

    def __init__(self):
        super().__init__("FakeStore REST")
        self.api_url = APP.fakestore_api_url
        self.poll_interval = APP.fakestore_poll_interval

    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        await self.start()

        async with aiohttp.ClientSession() as session:
            while self.is_running:
                try:
                    products = await self._fetch_products(session)

                    for product in products:
                        if not self.is_running:
                            break
                        parsed = self._parse_product(product)
                        if parsed:
                            yield parsed

                    log.info(f"Fetched {len(products)} products from FakeStore")

                except ClientError as e:
                    log.error(f"HTTP error while fetching products: {e}")

                except Exception as e:
                    log.exception(f"Unexpected error in FakeStore extractor: {e}")

                await asyncio.sleep(self.poll_interval)

        await self.stop()

    async def _fetch_products(self, session: aiohttp.ClientSession) -> List[Dict]:
        for attempt in range(self.MAX_RETRIES):
            try:
                async with session.get(self.api_url, timeout=30) as response:
                    response.raise_for_status()
                    return await response.json()

            except ClientError as e:
                if attempt == self.MAX_RETRIES - 1:
                    raise

                delay = self.RETRY_DELAY * (2**attempt)
                log.warning(
                    f"Retry {attempt + 1}/{self.MAX_RETRIES} after {delay}s: {e}"
                )
                await asyncio.sleep(delay)

        return []

    def _parse_product(self, data: Dict) -> Dict[str, Any]:
        try:
            return {
                "product_id": int(data.get("id")),
                "title": str(data.get("title", "")),
                "price": float(data.get("price", 0)),
                "category": str(data.get("category", "")),
                "updated_at": datetime.now(timezone.utc),
            }
        except (KeyError, TypeError, ValueError) as e:
            log.error(f"Failed to parse product: {e}")
            return None
