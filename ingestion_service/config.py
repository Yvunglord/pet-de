from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from urllib.parse import urlparse


class DatabaseSettingsBase(BaseSettings):
    host: str
    port: int
    dbname: str
    user: str
    password: str

    model_config = SettingsConfigDict()

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )


class CryptoDatabaseSettings(DatabaseSettingsBase):
    model_config = SettingsConfigDict(env_prefix="CRYPTO_")


class StoreDatabaseSettings(DatabaseSettingsBase):
    model_config = SettingsConfigDict(env_prefix="STORE_")


class AppSettings(BaseSettings):
    log_level: str = "INFO"
    batch_size: int = 100
    
    binance_ws_streams: list[str] = [
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
        "wss://stream.binance.com:9443/ws/ethusdt@trade",
    ]
    
    fakestore_api_url: str = "https://fakestoreapi.com/products"
    fakestore_poll_interval: int = 300

    model_config = SettingsConfigDict()

    @field_validator("binance_ws_streams")
    @classmethod
    def validate_ws_streams(cls, v: list[str]) -> list[str]:
        """Валидация: все URL должны быть валидными Binance WebSocket стримами"""
        if not v:
            raise ValueError("binance_ws_streams cannot be empty")
        
        for url in v:
            parsed = urlparse(url)
            if parsed.scheme not in ("wss", "ws"):
                raise ValueError(f"Invalid WebSocket scheme in URL: {url}")
            if "binance.com" not in parsed.netloc:
                raise ValueError(f"Non-Binance URL detected: {url}")
            if "@trade" not in url and "@aggTrade" not in url:
                raise ValueError(f"Unsupported stream type in URL: {url}")
        
        return v

    @staticmethod
    def extract_symbol_from_stream(ws_url: str) -> str:
        """
        Извлекает символ из Binance WebSocket URL.
        Пример: wss://.../ws/ethusdt@trade → "ETHUSDT"
        """
        path = urlparse(ws_url).path.rstrip("/")
        stream = path.split("/")[-1]
        symbol = stream.split("@")[0].upper()
        return symbol

    @property
    def binance_streams_with_symbols(self) -> list[tuple[str, str]]:
        """
        Возвращает список кортежей (ws_url, symbol) для удобного использования в инджестере.
        Пример: [("wss://.../btcusdt@trade", "BTCUSDT"), ...]
        """
        return [(url, self.extract_symbol_from_stream(url)) for url in self.binance_ws_streams]


CRYPTO_DB = CryptoDatabaseSettings()
STORE_DB = StoreDatabaseSettings()
APP = AppSettings()