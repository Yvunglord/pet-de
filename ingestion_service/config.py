from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettingsBase(BaseSettings):
    host: str
    port: int
    dbname: str
    user: str
    password: str

    model_config = SettingsConfigDict()

    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"


class CryptoDatabaseSettings(DatabaseSettingsBase):
    model_config = SettingsConfigDict(env_prefix="CRYPTO_")


class StoreDatabaseSettings(DatabaseSettingsBase):
    model_config = SettingsConfigDict(env_prefix="STORE_")


class AppSettings(BaseSettings):
    log_level: str = "INFO"
    batch_size: int = 100
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    fakestore_api_url: str = "https://fakestoreapi.com/products"
    fakestore_poll_interval: int = 300

    model_config = SettingsConfigDict()


CRYPTO_DB = CryptoDatabaseSettings()
STORE_DB = StoreDatabaseSettings()
APP = AppSettings()
