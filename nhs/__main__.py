"""
Ran when nhs is used as a standalone module
"""

from nhs.utils.logging import config_logger
from loguru import logger

logger.enable("nhs")
config_logger()

