from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)

from ..config import CRYPTO_DB, STORE_DB
from ..logger import log

_crypto_engine = None
_store_engine = None
_crypto_session_factory = None
_store_session_factory = None


async def init_db_pools():
    global _crypto_engine, _store_engine, _crypto_session_factory, _store_session_factory

    _crypto_engine = create_async_engine(
        CRYPTO_DB.database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        echo=False,
    )

    _store_engine = create_async_engine(
        STORE_DB.database_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        echo=False,
    )

    _crypto_session_factory = async_sessionmaker(
        bind=_crypto_engine, class_=AsyncSession, expire_on_commit=False
    )

    _store_session_factory = async_sessionmaker(
        bind=_store_engine, class_=AsyncSession, expire_on_commit=False
    )

    log.info("Database connection pools initialized successfully")


async def close_db_pools():

    if _crypto_engine:
        await _crypto_engine.dispose()
        log.info("Crypto DB pool closed")

    if _store_engine:
        await _store_engine.dispose()
        log.info("Store DB pool closed")


def get_crypto_pool() -> async_sessionmaker:
    if _crypto_session_factory is None:
        raise RuntimeError(
            "Database pools not initialized. Call init_db_pools() first."
        )
    return _crypto_session_factory


def get_store_pool() -> async_sessionmaker:
    if _store_session_factory is None:
        raise RuntimeError(
            "Database pools not initialized. Call init_db_pools() first."
        )
    return _store_session_factory
