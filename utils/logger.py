from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import colorlog

from config import logging_config


def get_logger(
    name: str,
    level: Optional[str] = None,
    log_to_file: bool = True,
) -> logging.Logger:
    """
    Create (or retrieve) a named logger with console + optional file handler.

    Args:
        name:        Logger name (typically the module __name__).
        level:       Override the default log level from config.
        log_to_file: Whether to also write logs to a rotating file.

    Returns:
        A configured :class:`logging.Logger` instance.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    resolved_level = getattr(
        logging, (level or logging_config.level).upper(), logging.INFO
    )
    logger.setLevel(resolved_level)

    # ── Console handler (colour) ──────────────────────────────────────────────
    console_handler = colorlog.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(resolved_level)
    console_handler.setFormatter(
        colorlog.ColoredFormatter(
            fmt=(
                "%(log_color)s%(asctime)s | %(levelname)-8s | "
                "%(name)s | %(message)s%(reset)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "bold_red",
            },
        )
    )
    logger.addHandler(console_handler)

    # ── File handler ──────────────────────────────────────────────────────────
    if log_to_file:
        from logging.handlers import RotatingFileHandler

        log_dir: Path = logging_config.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)

        safe_name = name.replace(".", "_").replace("/", "_")
        log_file = log_dir / f"{safe_name}.log"

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(resolved_level)
        file_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)

    # Prevent log messages from bubbling to the root logger
    logger.propagate = False
    return logger
