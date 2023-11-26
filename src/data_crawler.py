""" Module for data crawling operations from GDACS (Global Disaster Alert and Coordination System) website.

Defines an abstract base class `DataCrawler` for data crawling and two concrete classes:
- `GDACSGeoJsonDtaCrawler`: Web scraper for downloading GeoJSON data from GDACS website.
- `GDACSXmlDtaCrawler`: XML data crawler for fetching data from GDACS website. """


import os
import glob
from typing import Dict, Any, Union
from abc import ABC, abstractmethod
import time
import json
import xml.etree.ElementTree as ET
import logging
import requests
import geojson
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



class DataCrawler(ABC):
    """Abstract base class for data crawling."""

    @abstractmethod
    def get_data(self) -> Union[None, Dict[str, Any]]:
        """Abstract method to retrieve data."""


class GDACSGeoJsonDtaCrawler(DataCrawler):
    """
    Web scraper for downloading GeoJSON data from GDACS website.

    Attributes:
    - VERTICAL_SCROLL_SHIFT (int): The vertical shift for scrolling to the download button.
    - DOWNLOAD_BUTTON_ID (str): The ID of the download button element.

    Methods:
    - __init__(url: str, driver: WebDriver, downloads_directory_path: str, xpath: str = "//a[@href='javascript:onclick=downloadResult();']") -> None:
      Initializes the GDACSGeoJsonDtaCrawler instance.
    - scroll_page_to_download_button() -> None:
      Scrolls the page to the download button.
    - download_geojson() -> bool:
      Initiates the download of GeoJSON data from the GDACS website.
    - get_data() -> Union[None, Dict[str, Any]]:
      Retrieves GeoJSON data from the latest downloaded file.
    """

    VERTICAL_SCROLL_SHIFT: int = -50
    DOWNLOAD_BUTTON_ID: str = "contentSearch"

    def __init__(
        self,
        url: str,
        driver,
        downloads_directory_path: str,
        xpath: str = "//a[@href='javascript:onclick=downloadResult();']",
    ) -> None:
        """
        Initializes the GDACSGeoJsonDtaCrawler instance.

        Parameters:
        - url (str): The URL to the GDACS website.
        - driver (WebDriver): The Selenium WebDriver.
        - downloads_directory_path (str): Path to the directory where GeoJSON files will be downloaded.
        - xpath (str, optional): XPath expression to locate the download button. Defaults to GDACS website's XPath.
        """
        self.url: str = url
        self.driver = driver
        self.downloads_directory_path: str = downloads_directory_path
        self.xpath: str = xpath

    def scroll_page_to_download_button(self) -> None:
        """Scrolls the page to the download button."""
        content_search_element = self.driver.find_element("id", self.DOWNLOAD_BUTTON_ID)
        element_position = content_search_element.location["y"]
        scroll_position = element_position + self.VERTICAL_SCROLL_SHIFT
        self.driver.execute_script(f"window.scrollTo(0, {scroll_position});")

    def download_geojson(self) -> bool:
        """
        Initiates the download of GeoJSON data from the GDACS website.

        Returns:
        - bool: True if the download is successful, False otherwise.
        """
        try:
            self.driver.get(self.url)
            # Maximize the browser window
            self.driver.maximize_window()

            # Wait for the page to load
            time.sleep(1)
            self.scroll_page_to_download_button()
            time.sleep(5)

            # Find the button
            button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, self.xpath))
            )
            button.click()
            # Wait for the GeoJson file to load
            time.sleep(10)
            return True

        finally:
            # Close the WebDriver
            self.driver.quit()

    def get_data(self) -> Union[None, Dict[str, Any]]:
        """
        Retrieves GeoJSON data from the latest downloaded file.

        Returns:
        - dict: Parsed GeoJSON content.
        """
        geojson_data_flag = self.download_geojson()
        if geojson_data_flag:
            latest_file = max(
                glob.glob(os.path.join(self.downloads_directory_path, "*.geojson")),
                key=os.path.getctime,
            )
            with open(latest_file, "r") as file:
                feature_collection = json.load(file)
            return feature_collection


class GDACSXmlDtaCrawler(DataCrawler):
    """
    XML data crawler for fetching data from GDACS website.

    Methods:
    - __init__(url: str) -> None:
      Initializes the GDACSXmlDtaCrawler instance.
    - fetch_xml_data() -> bytes:
      Fetches XML data from the specified URL.
    - parse_xml_tree() -> Element:
      Parses the XML data and returns the root of the XML tree.
    - extract_event_information(event: Element) -> dict:
      Extracts event information from an XML <item> element.
    - create_geojson_feature(event_dict: dict, feature_id: int) -> Union[geojson.Feature, None]:
      Creates a GeoJSON feature from the extracted event information.
    - get_data() -> geojson.FeatureCollection:
      Retrieves GeoJSON data from the parsed XML tree.
    """

    def __init__(self, url: str) -> None:
        """
        Initializes the GDACSXmlDtaCrawler instance.

        Parameters:
        - url (str): The URL to fetch XML data from.
        """
        self.url: str = url

    def fetch_xml_data(self) -> bytes:
        """Fetches XML data from the specified URL."""
        response = requests.get(self.url)
        return response.content

    def parse_xml_tree(self):
        """Parses the XML data and returns the root of the XML tree."""
        xml_content = self.fetch_xml_data()
        return ET.fromstring(xml_content)

    def extract_event_information(self, event) -> Dict[str, Any]:
        """Extracts event information from an XML <item> element."""
        event_dict = {}

        event_dict["Title"] = event.find("title").text
        event_dict["Description"] = event.find("description").text
        event_dict["Link"] = event.find("link").text
        event_dict["Publication Date"] = event.find("pubDate").text

        severity_elem = event.find(
            ".//gdacs:severity", namespaces={"gdacs": "http://www.gdacs.org"}
        )
        if severity_elem is not None:
            event_dict["Severity unit"] = (severity_elem.attrib["unit"],)
            event_dict["Severity value"] = (severity_elem.attrib["value"],)
            event_dict["Severity text"] = severity_elem.text

        population_elem = event.find(
            ".//gdacs:population", namespaces={"gdacs": "http://www.gdacs.org"}
        )
        if population_elem is not None:
            event_dict["Population unit"] = (population_elem.attrib["unit"],)
            event_dict["Population value"] = (population_elem.attrib["value"],)
            event_dict["Population text"] = population_elem.text

        event_dict["Date Added"] = event.find(
            ".//gdacs:dateadded", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Date Modified"] = event.find(
            ".//gdacs:datemodified", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Is Current"] = event.find(
            ".//gdacs:iscurrent", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["From Date"] = event.find(
            ".//gdacs:fromdate", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["To Date"] = event.find(
            ".//gdacs:todate", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Duration in Week"] = event.find(
            ".//gdacs:durationinweek", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Year"] = event.find(
            ".//gdacs:year", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Subject"] = event.find(
            ".//dc:subject", namespaces={"dc": "http://purl.org/dc/elements/1.1/"}
        ).text
        event_dict["Is PermaLink"] = event.find("./guid").attrib["isPermaLink"]
        event_dict["Bbox"] = event.find(
            ".//gdacs:bbox", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["GeoRSS Point"] = event.find(
            ".//georss:point", namespaces={"georss": "http://www.georss.org/georss"}
        ).text
        event_dict["Event Type"] = event.find(
            ".//gdacs:eventtype", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Alert Level"] = event.find(
            ".//gdacs:alertlevel", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Alert Score"] = event.find(
            ".//gdacs:alertscore", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Episode Alert Level"] = event.find(
            ".//gdacs:episodealertlevel", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Episode Alert Score"] = event.find(
            ".//gdacs:episodealertscore", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Event ID"] = event.find(
            ".//gdacs:eventid", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Episode ID"] = event.find(
            ".//gdacs:episodeid", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Calculation Type"] = event.find(
            ".//gdacs:calculationtype", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Severity"] = event.find(
            ".//gdacs:severity", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Population"] = event.find(
            ".//gdacs:population", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Vulnerability"] = event.find(
            ".//gdacs:vulnerability", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text
        event_dict["Country"] = event.find(
            ".//gdacs:country", namespaces={"gdacs": "http://www.gdacs.org"}
        ).text

        return event_dict

    def create_geojson_feature(
        self, event_dict: Dict[str, Any], feature_id: int
    ) -> Union[geojson.Feature, None]:
        """Creates a GeoJSON feature from the extracted event information."""
        if event_dict.get("GeoRSS Point"):
            latitude, longitude = map(float, event_dict["GeoRSS Point"].split())
            # Create a GeoJSON feature
            feature = geojson.Feature(
                id=feature_id,
                properties=event_dict,
                geometry=geojson.Point(coordinates=(longitude, latitude)),
            )
            return feature
        logging.warning(
            "GeoRSS Point not found for the following item:  %d",
            event_dict["Title"],
        )
        return None

    def get_data(self) -> geojson.FeatureCollection:
        """
        Retrieves GeoJSON data from the parsed XML tree.

        Returns:
        - geojson.FeatureCollection: Parsed GeoJSON content.
        """
        tree = self.parse_xml_tree()
        # FeatureCollection to store GeoJSON features
        feature_collection = geojson.FeatureCollection([])

        # Counter for generating auto-incrementing IDs
        feature_id_counter = 1

        # Iterate through the XML tree and extract information from each <item> element
        for event in tree.findall(".//item"):
            event_dict = self.extract_event_information(event)

            # Create a GeoJSON feature with auto-incrementing integer ID
            feature = self.create_geojson_feature(event_dict, feature_id_counter)
            if feature:
                # Increment the counter
                feature_id_counter += 1

                # Add the GeoJSON feature to the FeatureCollection
                feature_collection["features"].append(feature)

        return feature_collection
