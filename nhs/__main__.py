"""
Ran when nhs is used as a standalone module
"""

from loguru import logger

from nhs.utils.logging import config_logger

logger.enable("nhs")
config_logger()  # type: ignore
