"""
Logging configuration module.
Provides standardized logging setup using loguru.
"""

import sys
from loguru import logger


def setup_logging(
    level: str = "INFO",
    format: str = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    log_file: str = None,
) -> None:
    """
    Configure loguru logger.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        format: Log message format
        log_file: Optional file path to write logs
    """
    logger.remove()

    logger.add(
        sys.stderr,
        format=format,
        level=level,
        colorize=True,
    )

    if log_file:
        logger.add(
            log_file,
            format=format,
            level=level,
            rotation="10 MB",
            retention="7 days",
        )


def get_logger(name: str = None):
    """
    Get a logger instance.

    Args:
        name: Logger name (optional)

    Returns:
        Logger instance
    """
    return logger
