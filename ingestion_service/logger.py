import sys
from loguru import logger
from .config import APP

def setup_logger():
    logger.remove()

    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        level=APP.log_level,
        colorize=True
    )

    logger.add(
        "logs/ingestion_{time:YYYY-MM-DD}.log",
        rotation="100 MB",
        retention="7 days",
        level=APP.log_level,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}"
    )

    return logger

log = setup_logger()