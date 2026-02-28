from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from ..database.models import RawTrade
from ..config import APP
from ..logger import log


class CryptoDataLoader:
    def __init__(self, session_factory):
        self.session_factory = session_factory
        self.batch_size = APP.batch_size
        self._buffer: List[Dict[str, Any]] = []

    async def load(self, trade_data: Dict[str, Any]):
        self._buffer.append(trade_data)

        if len(self._buffer) >= self.batch_size:
            await self._flush()

    async def _flush(self):
        if not self._buffer:
            return

        try:
            async with self.session_factory() as session:
                stmt = pg_insert(RawTrade).values(self._buffer)
                stmt = stmt.on_conflict_do_nothing(index_elements=["trade_id"])

                await session.execute(stmt)
                await session.commit()

            log.info(f"Loaded {len(self._buffer)} trades to crypto DB")

        except Exception as e:
            log.exception(f"Failed to flush trades to DB: {e}")

        finally:
            self._buffer.clear()

    async def flush_remaining(self):
        await self._flush()
        log.info("Remaining trades flushed to DB")
