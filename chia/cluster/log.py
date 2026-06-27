import logging
import sys
import time
from contextlib import contextmanager

LOGGER_NAME = "chia"

_FORMAT_DEFAULT = "[chia] %(levelname)s %(message)s"
_FORMAT_VERBOSE = "[chia] %(asctime)s %(levelname)s [%(name)s] %(message)s"


def setup_logging(verbose: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = _FORMAT_VERBOSE if verbose else _FORMAT_DEFAULT

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt))

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    if name:
        return logging.getLogger(f"{LOGGER_NAME}.{name}")
    return logging.getLogger(LOGGER_NAME)


@contextmanager
def log_phase(logger: logging.Logger, description: str):
    logger.info(description)
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        logger.debug(f"{description} completed in {elapsed:.1f}s")
