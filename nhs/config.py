from loguru import logger
from typing import Any

import yaml


@logger.catch(reraise=True)
def parse_config(config_path: str = "configurations.yml") -> dict[str, Any]:
    """
    Parse a YAML configuration file at `config_path` and return the settings as a dictionary.
    """
    with open(config_path, "r") as file:
        config: dict[str, Any] = yaml.safe_load(file)
    logger.info(f"Configuration loaded from {config_path}")
    return config


def data_config(config_path: str = "configurations.yml") -> dict[str, Any]:
    """
    Parse a YAML configuration file at `config_path` and return the data settings as a dictionary.
    """
    return parse_config(config_path)["data"]


def simulation_config(config_path: str = "configurations.yml") -> dict[str, Any]:
    """
    Parse a YAML configuration file at `config_path` and return the simulation settings as a dictionary.
    """
    return parse_config(config_path)["simulation"]


def logger_config(config_path: str = "configurations.yml") -> dict[str, Any]:
    """
    Parse a YAML configuration file at `config_path` and return the logger settings as a dictionary.
    """
    return parse_config(config_path)["logger"]
