"""
Module for configuration parsing and data handling.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
import os.path
import yaml
import logging


class ConfigParser(ABC):
    """
    Abstract base class for configuration parsers.
    """

    def __init__(self, path_to_config_folder: str, config_file_name: str) -> None:
        """
        Initializes a ConfigParser instance.

        Args:
            path_to_config_folder (str): The path to the folder containing the configuration file.
            config_file_name (str): The name of the configuration file.
        """
        self.config_path: str = os.path.join(path_to_config_folder, config_file_name)

    @abstractmethod
    def get_data(self) -> Dict[str, Any]:
        """
        Abstract method to be implemented by subclasses for retrieving configuration data.

        Returns:
            dict: Configuration data.
        """


class YamlConfigParser(ConfigParser):
    """
    Configuration parser for YAML files.

    Handles file not found and YAML parsing errors gracefully.
    """

    def get_data(self) -> Dict[str, Any]:
        """
        Reads data from a YAML configuration file.

        Returns:
            dict: Configuration data. Returns an empty dictionary in case of errors.
        """
        try:
            with open(self.config_path, "r") as config_file:
                config_data: Dict[str, Any] = yaml.safe_load(config_file)
                return config_data
        except FileNotFoundError:
            logging.error(f"Error: Config file not found at {self.config_path}")
            return {}
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML in config file: {e}")
            return {}


class ConfigData:
    """
    Container for configuration data.
    """

    def __init__(self, config_data: Dict[str, Any], env_var: str) -> None:
        """
        Initializes a ConfigData instance.

        Args:
            config_data (dict): The configuration data.
            env_var (str): The environment variable specifying the configuration.
        """
        self.config_data: Dict[str, Any] = config_data
        self.env_var: str = env_var

    @property
    def db_config(self) -> Dict[str, Any]:
        """
        Retrieves the database configuration from the overall configuration.

        Returns:
            dict: Database configuration.
        """
        return self.config_data.get("DB_CONNECTION", {}).get(self.env_var, {})

    @property
    def common(self) -> Dict[str, Any]:
        """
        Retrieves the common configuration from the overall configuration.

        Returns:
            dict: Common configuration.
        """
        return self.config_data.get("COMMON", {})
