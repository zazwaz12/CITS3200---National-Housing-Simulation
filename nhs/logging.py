"""
Functions for logging
"""

import inspect
import logging
import warnings
from functools import wraps
from typing import Any, Callable, Literal, Optional, TextIO, Type, TypeVar, Union

from loguru import logger

T = TypeVar("T")


class __InterceptHandler(logging.Handler):
    """
    Intercept standard logging messages and redirect them to Loguru logger
    """

    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
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


def config_logger(logger_config: dict[str, Any]) -> None:
    """
    Configure loguru logger settings and set it as the default logger
    """

    logger.remove()
    logger.add(
        logger_config["log_file"],
        format=logger_config["format"],
        backtrace=logger_config["backtrace"],
        diagnose=logger_config["diagnose"],
        level=logger_config["level"],
    )

    logging.basicConfig(handlers=[__InterceptHandler()], level=0, force=True)

    def custom_showwarning(
        message: Union[Warning, str],
        category: Type[Warning],
        filename: str,
        lineno: int,
        file: Optional[TextIO] = None,
        line: Optional[str] = None,
    ) -> None:
        logger.warning(f"{category.__name__}: {str(message)}")

    warnings.showwarning = custom_showwarning


def log_entry_exit(
    *,
    entry: bool = True,
    exit: bool = True,
    level: Literal[
        "TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"
    ] = "DEBUG",
):
    """
    Logs entry and exit of a function that this decorator wraps
    """

    def wrapper(func: Callable[..., T]) -> Callable[..., T]:
        name = func.__name__

        @wraps(func)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            if entry:
                logger.log(level, f"Entering '{name}' (args={args}, kwargs={kwargs})")
            result = func(*args, **kwargs)
            if exit:
                logger.log(level, f"Exiting '{name}' (result={result})")
            return result

        return wrapped

    return wrapper
