import logging
from typing import Any, Dict

import yaml

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_user_config(config_path: str = "configurations.yml") -> Dict[str, Any]:
    """
    Read and parse a YAML configuration file.

    Parameters
    ----------
    config_path : str, optional
        The file path to the YAML configuration file. Default is 'configurations.yml'.

    Returns
    -------
    Dict[str, Any]
        A dictionary containing the configuration settings parsed from the YAML file.

    Raises
    ------
    FileNotFoundError:
        If the specified configuration file does not exist.
    yaml.YAMLError:
        If there is an issue with parsing the YAML content of the file.
    """
    try:
        with open(config_path, "r") as file:
            config: Dict[str, Any] = yaml.safe_load(file)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing configuration file: {e}")
        raise
