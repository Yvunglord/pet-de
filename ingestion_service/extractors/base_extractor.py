from abc import ABC, abstractmethod
from typing import AsyncGenerator, Any, Dict
from ..logger import log

class BaseExtractor(ABC):
    def __init__(self, name: str):
        self.name = name
        self.is_running = False

    @abstractmethod
    async def extract(self) -> AsyncGenerator[Dict[str, Any], None]:
        pass
        yield
    
    async def start(self):
        self.is_running = True
        log.info(f"{self.name} extractor started")
    
    async def stop(self):
        self.is_running = False
        log.info(f"{self.name} extractor stopped")
    
    async def __aenter__(self):
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()