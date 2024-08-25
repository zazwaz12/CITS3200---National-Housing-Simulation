"""
Functions for logging
"""

import inspect
import logging
import warnings
from functools import wraps
from typing import Any, Callable

from loguru import logger


class __InterceptHandler(logging.Handler):
    """
    Intercept standard logging messages and redirect them to Loguru logger
    """

    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def config_logger():
    """
    Configure loguru logger settings and set it as as the default logger
    """
    logger.remove()
    logger.add(
        "./logs/out_{time}.log",
        format="{time:YYYY-MM-DD at HH:mm:ss} {level} {message}",
        backtrace=True,
        diagnose=True,
        level="DEBUG",
        retention="7 days",
    )

    logging.basicConfig(handlers=[__InterceptHandler()], level=0, force=True)
    warnings.showwarning = lambda msg, *args, **kwargs: logger.warning(msg)


def log_entry_exit(*, entry=True, exit=True, level="DEBUG"):
    """
    Logs entry and exit of a function that this decorator wraps
    """

    def wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
        name = func.__name__

        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            result = None
            logger_ = logger.opt(depth=1)
            if entry:
                logger.log(
                    level,
                    f"Entering '{name}' (args={args}, kwargs={kwargs})",
                )
                result = func(*args, **kwargs)
            if exit:
                logger.log(level, f"Exiting '{name}' (result={result})")
            return result

        return wrapped

    return wrapper
