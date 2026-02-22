import asyncio
import json
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from .base_extractor import BaseExtractor
from ..config import APP
from ..logger import log

class BinanceWebSocketExtractor(BaseExtractor):
    RECONNECT_DELAY = 5
    MAX_RECONNECT_ATTEMPTS = 10

    def __init__(self):
        super().__init__("Binance WebSocket")
        self.ws_url = APP.binance_ws_url
        self._reconnect_count = 0

    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        await self.start()

        while self.is_running:
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=30,
                    ping_timeout=10
                ) as websocket:
                    log.info(f"Connected to Binance WebSocket: {self.ws_url}")
                    self._reconnect_count = 0
                    
                    async for message in websocket:
                        if not self.is_running:
                            break
                        
                        try:
                            data = json.loads(message)
                            parsed = self._parse_trade_messages(data)
                            if parsed:
                                yield parsed
                        except json.JSONDecodeError as e:
                            log.error(f"Failed to parse message: {e}")
                            continue
            
            except ConnectionClosed as e:
                log.warning(f"WebSocket connection closed: {e}")
                await self._handle_reconnect()
            
            except WebSocketException as e:
                log.error(f"WebSocket error: {e}")
                await self._handle_reconnect()
            
            except Exception as e:
                log.exception(f"Unexpected error in Binance extractor: {e}")
                await self._handle_reconnect()
        
        await self.stop()

    def _parse_trade_messages(self, data: Dict) -> Dict[str, Any]:
        try:
            return {
                'trade_id': str(data.get('t')),
                'price': float(data.get('p')),
                'quantity': float(data.get('q')),
                'trade_time': datetime.fromtimestamp(
                    data.get('T') / 1000,
                    tz = timezone.utc
                ),
                'is_buyer_maker': bool(data.get('m'))
            }
        except (KeyError, TypeError, ValueError) as e:
            log.error(f"Failed to parse trade message: {e}")
            return None
        
    async def _handle_reconnect(self):
        if self._reconnect_count >= self.MAX_RECONNECT_ATTEMPTS:
            log.error("Max reconnect attempts reached. Stopping extractor.")
            self.is_running = False
            return
        
        delay = self.RECONNECT_DELAY * (2 ** self._reconnect_count)
        self._reconnect_count += 1
        
        log.warning(f"Reconnecting in {delay} seconds (attempt {self._reconnect_count}/{self.MAX_RECONNECT_ATTEMPTS})")
        await asyncio.sleep(delay)