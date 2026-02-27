"""
================================================================================
NIAGARA BAS LOGGING v2.0
================================================================================
Centralized logging configuration for the Niagara BAS suite.

USAGE:
    from logging_config import get_logger
    logger = get_logger("auth")

    logger.info("Login successful")
    logger.error("Connection failed: %s", error)
    logger.debug("URL: %s", url)
================================================================================
"""

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Optional


_initialized = False


def setup_logging(
    name: str = "niagara",
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3
) -> logging.Logger:
    """
    Configure the root niagara logger.

    Call once at application startup. Subsequent calls are no-ops.

    Args:
        name: Root logger name
        level: Logging level (default INFO)
        log_file: Optional path for rotating file log
        max_bytes: Max log file size before rotation (default 5 MB)
        backup_count: Number of rotated log files to keep

    Returns:
        Configured root logger
    """
    global _initialized

    logger = logging.getLogger(name)

    if _initialized:
        return logger

    logger.setLevel(level)

    # Console handler (skip if stdout is None, e.g. --windowed exe)
    if sys.stdout is not None:
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level)
        fmt = logging.Formatter(
            '[%(asctime)s] %(levelname)-7s %(name)s: %(message)s',
            datefmt='%H:%M:%S'
        )
        console.setFormatter(fmt)
        logger.addHandler(console)
    else:
        # Windowed mode with no console - add NullHandler to prevent
        # propagation to root logger (which also has no valid stream)
        logger.addHandler(logging.NullHandler())

    # File handler (optional)
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(logging.DEBUG)
        file_fmt = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(name)s:%(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_fmt)
        logger.addHandler(file_handler)

    _initialized = True
    return logger


def get_logger(module_name: str) -> logging.Logger:
    """
    Get a child logger for a specific module.

    Args:
        module_name: Short module identifier (e.g., "auth", "engine")

    Returns:
        Logger instance named 'niagara.{module_name}'
    """
    return logging.getLogger(f"niagara.{module_name}")
