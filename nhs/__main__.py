"""
Ran when nhs is used as a standalone module
"""

from loguru import logger
from .config import logger_config
from .logging import config_logger

logger.enable("nhs")
config_logger(logger_config())
