import logging
from logging.handlers import RotatingFileHandler

from poweron_bot.paths import LOGS_DIR


def _build_rotating_handler(log_name: str) -> RotatingFileHandler:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        LOGS_DIR / log_name,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    return handler


def get_user_logger() -> logging.Logger:
    logger = logging.getLogger("poweron_user_entries")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        logger.addHandler(_build_rotating_handler("user_entries.log"))
    return logger


def get_admin_logger() -> logging.Logger:
    logger = logging.getLogger("poweron_admin_actions")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        logger.addHandler(_build_rotating_handler("admin_actions.log"))
    return logger
