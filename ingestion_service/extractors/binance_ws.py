import asyncio
import json
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from ..config import APP
from ..logger import log
from .base_extractor import BaseExtractor


class BinanceWebSocketExtractor(BaseExtractor):
    RECONNECT_DELAY = 5
    MAX_RECONNECT_ATTEMPTS = 10

    def __init__(self):
        super().__init__("Binance WebSocket")
        self.ws_streams = APP.binance_streams_with_symbols
        self._reconnect_counts: Dict[str, int] = {}

    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        await self.start()
        
        queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

        async def worker(ws_url: str, symbol: str):
            while self.is_running:
                try:
                    async with websockets.connect(
                        ws_url, ping_interval=30, ping_timeout=10
                    ) as websocket:
                        log.info(f"Connected to {symbol} stream: {ws_url}")
                        self._reconnect_counts[symbol] = 0

                        async for message in websocket:
                            if not self.is_running:
                                break
                            try:
                                data = json.loads(message)
                                parsed = self._parse_trade_message(data, symbol)
                                if parsed:
                                    await queue.put(parsed)
                            except json.JSONDecodeError as e:
                                log.error(f"Failed to parse message ({symbol}): {e}")
                                continue

                except ConnectionClosed as e:
                    log.warning(f"{symbol} connection closed: {e}")
                    await self._handle_reconnect(symbol)
                except WebSocketException as e:
                    log.error(f"{symbol} WebSocket error: {e}")
                    await self._handle_reconnect(symbol)
                except Exception as e:
                    log.exception(f"Unexpected error in {symbol} worker: {e}")
                    await self._handle_reconnect(symbol)

        tasks = [
            asyncio.create_task(worker(url, sym))
            for url, sym in self.ws_streams
        ]

        try:
            while self.is_running:
                try:
                    trade = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield trade
                    queue.task_done()
                except asyncio.TimeoutError:
                    continue
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.stop()

    def _parse_trade_message(self, data: Dict, symbol: str) -> Dict[str, Any]:
        try:
            if data.get("e") != "trade":
                return None
            return {
                "symbol": symbol,
                "trade_id": str(data.get("t")),
                "price": float(data.get("p")),
                "quantity": float(data.get("q")),
                "trade_time": datetime.fromtimestamp(
                    data.get("T") / 1000, tz=timezone.utc
                ),
                "is_buyer_maker": bool(data.get("m")),
            }
        except (KeyError, TypeError, ValueError) as e:
            log.error(f"Failed to parse trade message ({symbol}): {e}")
            return None

    async def _handle_reconnect(self, symbol: str):
        if self._reconnect_counts.get(symbol, 0) >= self.MAX_RECONNECT_ATTEMPTS:
            log.error(f"Max reconnect attempts for {symbol}. Stopping stream.")
            return

        delay = self.RECONNECT_DELAY * (2 ** self._reconnect_counts.get(symbol, 0))
        self._reconnect_counts[symbol] = self._reconnect_counts.get(symbol, 0) + 1

        log.warning(
            f"{symbol}: Reconnecting in {delay}s "
            f"(attempt {self._reconnect_counts[symbol]}/{self.MAX_RECONNECT_ATTEMPTS})"
        )
        await asyncio.sleep(delay)