"""
This script collects data from GDACS (Global Disaster Alert and Coordination System) and writes it to a database.

It includes functions for initializing a Selenium webdriver, getting configuration data, retrieving alert and disaster event feature collections,
writing data to a database, and main functions for getting and writing alerts and disaster event data.
"""

from typing import Dict, Tuple, Union, Any
import time
import logging
from selenium import webdriver
from configs.config_parser import YamlConfigParser, ConfigData
from data_crawler import GDACSGeoJsonDtaCrawler, GDACSXmlDtaCrawler
from db.db import GeoJsonDBDataWriter

PATH_TO_CONFIG_FOLDER: str = "src/configs/"
CONFIG_FILE_NAME: str = "config.yaml"
ENV: str = "localhost"

# Configure logging
logging.basicConfig(level=logging.INFO)


def initialize_webdriver(
    browser_name: str,
) -> Union[webdriver.Chrome, webdriver.Firefox, webdriver.Edge, webdriver.Ie]:
    """
    Initializes a Selenium webdriver based on the specified browser name.

    Args:
        browser_name (str): The name of the browser ('Google Chrome', 'Mozilla Firefox', 'Microsoft Edge', or 'Internet Explorer').

    Returns:
        webdriver: An instance of the Selenium webdriver for the specified browser.

    Raises:
        ValueError: If the specified browser is not supported.
        Exception: If an error occurs during webdriver initialization.
    """
    try:
        if browser_name == "Google Chrome":
            return webdriver.Chrome()
        elif browser_name == "Mozilla Firefox":
            return webdriver.Firefox()
        elif browser_name == "Microsoft Edge":
            return webdriver.Edge()
        elif browser_name == "Internet Explorer":
            return webdriver.Ie()
        else:
            raise ValueError(f"Unsupported browser: {browser_name}")
    except Exception as e:
        logging.error(f"Error initializing webdriver: {e}")


def get_config_data() -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Reads configuration data from a YAML file and returns common data and database configuration.

    Returns:
        tuple: A tuple containing common data and database configuration.
    """
    config_parser = YamlConfigParser(PATH_TO_CONFIG_FOLDER, CONFIG_FILE_NAME)
    config_data = config_parser.get_data()
    config_data_instance = ConfigData(config_data, ENV)
    return config_data_instance.common, config_data_instance.db_config


def get_alert_feature_collection(
    webdriver_instance: webdriver,
    geojson_url: str,
    browser_download_directory_path: str,
) -> Dict[str, Any]:
    """
    Retrieves alert feature collection data using GDACSGeoJsonDtaCrawler.

    Args:
        webdriver_instance (webdriver): An instance of the Selenium webdriver.
        geojson_url (str): The URL to the GeoJSON data.
        browser_download_directory_path (str): The path to the browser download directory.

    Returns:
        dict: The alert feature collection data.

    Raises:
        Exception: If an error occurs during the data retrieval process.
    """
    try:
        geojson_obj = GDACSGeoJsonDtaCrawler(
            geojson_url, webdriver_instance, browser_download_directory_path
        )
        return geojson_obj.get_data()
    except Exception as e:
        logging.error(f"Error getting alert feature collection: {e}")


def get_disaster_event_feature_collection(xml_url: str) -> Dict[str, Any]:
    """
    Retrieves disaster event feature collection data using GDACSXmlDtaCrawler.

    Args:
        xml_url (str): The URL to the XML data.

    Returns:
        dict: The disaster event feature collection data.

    Raises:
        Exception: If an error occurs during the data retrieval process.
    """
    try:
        xml_data_obj = GDACSXmlDtaCrawler(xml_url)
        return xml_data_obj.get_data()
    except Exception as e:
        logging.error(f"Error getting disaster event feature collection: {e}")


def write_to_database(
    db_config: Dict[str, Any],
    feature_collection: Dict[str, Any],
    table_name: str,
    src: str,
) -> None:
    """
    Writes feature collection data to a database using GeoJsonDBDataWriter.

    Args:
        db_config (dict): The database configuration.
        feature_collection (dict): The feature collection data.
        table_name (str): The name of the database table.
        src (str): The data source coordinate system identifier.

    Raises:
        Exception: If an error occurs during the database write process.
    """
    try:
        db_obj = GeoJsonDBDataWriter(db_config, feature_collection, table_name, src)
        db_obj.write()
    except Exception as e:
        logging.error(f"Error writing to the database: {e}")


def get_and_write_alerts_data(
    common_data: Dict[str, Any], db_config: Dict[str, Any]
) -> None:
    """
    Gets alert feature collection data and writes it to the database.

    Args:
        common_data (dict): Common configuration data.
        db_config (dict): Database configuration data.
    """
    logging.info(50 * "*" + "\n")
    browser_name = common_data["webdriver"]
    driver = initialize_webdriver(browser_name)
    logging.info("Getting alert feature_collection...")

    alert_feature_collection = get_alert_feature_collection(
        driver,
        common_data["geojson_url"],
        common_data["browser_download_directory_path"],
    )

    logging.info("Writing alert feature_collection to DB...")
    write_to_database(
        db_config,
        alert_feature_collection,
        common_data["disaster_alerts_table_name"],
        common_data["src"],
    )


def get_and_write_disaster_event_data(
    common_data: Dict[str, Any], db_config: Dict[str, Any]
) -> None:
    """
    Gets disaster event feature collection data and writes it to the database.

    Args:
        common_data (dict): Common configuration data.
        db_config (dict): Database configuration data.
    """
    logging.info(50 * "*" + "\n")
    logging.info("Getting disaster event feature collection...")
    disaster_event_feature_collection = get_disaster_event_feature_collection(
        common_data["xml_url"]
    )

    logging.info("Writing disaster event feature collection to DB...")
    write_to_database(
        db_config,
        disaster_event_feature_collection,
        common_data["disaster_events_table_name"],
        common_data["src"],
    )


if __name__ == "__main__":
    while True:
        common_data_, db_configuration = get_config_data()

        get_and_write_alerts_data(common_data_, db_configuration)
        get_and_write_disaster_event_data(common_data_, db_configuration)
        time.sleep(86400)
