import asyncio
import signal

from .database import close_db_pools, get_crypto_pool, get_store_pool, init_db_pools
from .extractors import BinanceWebSocketExtractor, FakeStoreRestExtractor
from .loaders import CryptoDataLoader, StoreDataLoader
from .logger import log


class IngestionService:

    def __init__(self):
        self.is_running = True
        self.crypto_extractor = None
        self.store_extractor = None
        self.crypto_loader = None
        self.store_loader = None

    def _setup_signal_handlers(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

    def _signal_handler(self, signum, frame):
        log.info(f"Received signal {signum}. Shutting down gracefully...")
        self.is_running = False

    async def _run_crypto_pipeline(self):
        self.crypto_extractor = BinanceWebSocketExtractor()
        self.crypto_loader = CryptoDataLoader(get_crypto_pool())

        try:
            async for trade in self.crypto_extractor.extract():
                if not self.is_running:
                    break
                await self.crypto_loader.load(trade)
        except Exception as e:
            log.exception(f"Crypto pipeline error: {e}")
        finally:
            await self.crypto_loader.flush_remaining()
            await self.crypto_extractor.stop()

    async def _run_store_pipeline(self):
        self.store_extractor = FakeStoreRestExtractor()
        self.store_loader = StoreDataLoader(get_store_pool())

        try:
            async for product in self.store_extractor.extract():
                if not self.is_running:
                    break
                await self.store_loader.load(product)
        except Exception as e:
            log.exception(f"Store pipeline error: {e}")
        finally:
            await self.store_extractor.stop()

    async def run(self):
        log.info("Starting Ingestion Service...")

        await init_db_pools()

        await asyncio.gather(
            self._run_crypto_pipeline(),
            self._run_store_pipeline(),
            return_exceptions=True,
        )

        await close_db_pools()
        log.info("Ingestion Service stopped")


async def main():
    service = IngestionService()
    service._setup_signal_handlers()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
