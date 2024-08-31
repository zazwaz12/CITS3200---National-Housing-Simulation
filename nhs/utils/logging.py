from functools import wraps
import inspect
import logging
from loguru import logger
from typing import Any, Callable, TypeVar, Literal, Optional, TextIO, Type, Union, Dict
import warnings
import yaml

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

def read_config(config_path: str = 'config.yml') -> Dict[str, Any]:
    """
    Read YAML configuration file and return as a dictionary.
    
    Args:
        config_path (str): Path to the configuration file. Defaults to 'config.yml'.
    
    Returns:
        Dict[str, Any]: Configuration settings as a dictionary.
    
    Raises:
        FileNotFoundError: If the configuration file is not found.
        yaml.YAMLError: If there's an error parsing the YAML file.
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file: {e}")
        raise

def config_logger(configurations: Dict[str, Any]) -> None:
    """
    Configure loguru logger settings and set it as the default logger
    """

    logger.remove()
    logger.add(
        configurations['logging']['log_file'],
        format=configurations['logging']['format'],
        backtrace=configurations['logging']['backtrace'],
        diagnose=configurations['logging']['diagnose'],
        level=configurations['logging']['level'],
        retention=configurations['logging']['retention'],
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
